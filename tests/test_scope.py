"""Scope enforcement: whatever the credential allows, only the requested references are listed and downloaded.

Run from the plugin root: ``python -m unittest discover -s tests``.

The plugin modules are imported lazily in ``setUpModule``: the Cat imports every
``.py`` file of the plugin, and at import time this module needs only the stdlib.
"""
import enum
import importlib
import sys
import tempfile
import types
import unittest
from pathlib import Path
from urllib.parse import unquote

base = gd = s3 = az = pu = pipeline = config = httpx = None

PACKAGE = "cloud_sources_under_test"


def _stub_cat() -> None:
    """Minimal ``cat`` package, enough to import the ingestion pipeline without a running Cat."""
    if "cat" in sys.modules:
        return
    noop = lambda *args, **kwargs: None  # noqa: E731
    cat = types.ModuleType("cat")
    cat.log = types.SimpleNamespace(info=noop, warning=noop, error=noop, debug=noop)
    models = types.ModuleType("cat.services.memory.models")
    models.VectorMemoryType = enum.Enum("VectorMemoryType", {"DECLARATIVE": "declarative", "EPISODIC": "episodic"})
    sys.modules.update({
        "cat": cat,
        "cat.services": types.ModuleType("cat.services"),
        "cat.services.memory": types.ModuleType("cat.services.memory"),
        "cat.services.memory.models": models,
    })


def setUpModule():
    global base, gd, s3, az, pu, pipeline, config, httpx
    if PACKAGE not in sys.modules:
        module = types.ModuleType(PACKAGE)
        module.__path__ = [str(Path(__file__).resolve().parents[1])]
        sys.modules[PACKAGE] = module
    _stub_cat()
    base = importlib.import_module(f"{PACKAGE}.connectors.base")
    gd = importlib.import_module(f"{PACKAGE}.connectors.google_drive")
    s3 = importlib.import_module(f"{PACKAGE}.connectors.s3")
    az = importlib.import_module(f"{PACKAGE}.connectors.azure_blob")
    pu = importlib.import_module(f"{PACKAGE}.connectors.presigned_url")
    pipeline = importlib.import_module(f"{PACKAGE}.ingestion.pipeline")
    config = importlib.import_module(f"{PACKAGE}.ingestion.config")
    httpx = importlib.import_module("httpx")


def _options():
    return base.ConnectorOptions(
        max_file_bytes=1024,
        max_retries=0,
        accepted_mime_types=frozenset({"text/plain"}),
        allowed_host_suffixes=frozenset({"amazonaws.com", "blob.core.windows.net"}),
    )


async def _open(connector, handler):
    await connector.open()
    await connector._client.aclose()
    connector._client = httpx.AsyncClient(transport=httpx.MockTransport(handler), follow_redirects=True)
    return connector


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------
S3_CREDENTIAL = {"access_key_id": "AKIA", "secret_access_key": "secret", "session_token": "tok", "region": "eu-west-1"}


class S3ScopeTest(unittest.IsolatedAsyncioTestCase):
    def connector(self, *references):
        credential = s3.S3Connector.parse_credential(S3_CREDENTIAL)
        return s3.S3Connector(credential, _options(), references=references)

    def item(self, bucket, key):
        return base.SourceItem(
            provider="s3", item_id=f"aws/{bucket}/{key}", name=key.rsplit("/", 1)[-1], mime_type="text/plain",
            version="1", download_hints={"bucket": bucket, "key": key},
        )

    def test_dot_segments_in_reference_are_refused(self):
        credential = s3.S3Connector.parse_credential(S3_CREDENTIAL)
        for reference in ("s3://bucket/shared/../admin/x.txt", "s3://bucket/./x.txt", "s3://bucket/a/.."):
            with self.subTest(reference=reference), self.assertRaises(base.ConnectorError):
                s3.S3Connector.validate_request(credential, [reference], _options())

    async def test_in_scope(self):
        connector = self.connector("s3://bucket/shared/", "s3://bucket/report.txt", "s3://whole/")
        cases = {
            ("bucket", "shared/a.txt"): True,
            ("bucket", "shared/deep/b.txt"): True,
            ("bucket", "report.txt"): True,
            ("bucket", "report.txt/inner.txt"): True,  # "report.txt" may also be a prefix
            ("whole", "any/key.txt"): True,
            ("bucket", "shared-other/a.txt"): False,
            ("bucket", "admin/secret.txt"): False,
            ("bucket", "shared/../admin/secret.txt"): False,  # httpx would request /admin/secret.txt
            ("bucket", "shared/./a.txt"): False,
            ("other", "shared/a.txt"): False,
        }
        for (bucket, key), expected in cases.items():
            with self.subTest(bucket=bucket, key=key):
                self.assertIs(await connector.in_scope(self.item(bucket, key)), expected)

    async def test_no_references_means_nothing_in_scope(self):
        self.assertFalse(await self.connector().in_scope(self.item("bucket", "a.txt")))


# ---------------------------------------------------------------------------
# Azure Blob
# ---------------------------------------------------------------------------
ACCOUNT_URL = "https://acct.blob.core.windows.net"


class AzureScopeTest(unittest.IsolatedAsyncioTestCase):
    def credential(self):
        return az.AzureBlobConnector.parse_credential({"account_url": ACCOUNT_URL, "sas_token": "sv=1&sig=x"})

    def connector(self, *references):
        return az.AzureBlobConnector(self.credential(), _options(), references=references)

    def item(self, container, name):
        return base.SourceItem(
            provider="azure_blob", item_id=f"acct/{container}/{name}", name=name.rsplit("/", 1)[-1],
            mime_type="text/plain", version="1", download_hints={"container": container, "name": name},
        )

    def test_dot_segments_in_reference_are_refused(self):
        for reference in ("docs/shared/../admin/x.txt", f"{ACCOUNT_URL}/docs/shared/%2E%2E/admin/x.txt", "docs/./x"):
            with self.subTest(reference=reference), self.assertRaises(base.ConnectorError):
                az.AzureBlobConnector.validate_request(self.credential(), [reference], _options())

    async def test_in_scope(self):
        connector = self.connector("docs/shared/", f"{ACCOUNT_URL}/docs/report.txt", "whole/")
        cases = {
            ("docs", "shared/a.txt"): True,
            ("docs", "report.txt"): True,
            ("whole", "x/y.txt"): True,
            ("docs", "shared-other/a.txt"): False,
            ("docs", "shared/../admin/secret.txt"): False,
            ("other", "shared/a.txt"): False,
        }
        for (container, name), expected in cases.items():
            with self.subTest(container=container, name=name):
                self.assertIs(await connector.in_scope(self.item(container, name)), expected)


# ---------------------------------------------------------------------------
# Pre-signed URLs
# ---------------------------------------------------------------------------
class PresignedScopeTest(unittest.IsolatedAsyncioTestCase):
    def test_dot_segments_in_url_are_refused(self):
        credential = pu.PresignedUrlConnector.parse_credential({})
        for url in (
            "https://acct.blob.core.windows.net/cont/shared/../admin/x.txt?sv=1&sig=x",
            "https://acct.blob.core.windows.net/cont/shared/%2E%2E/admin/x.txt?sv=1&sig=x",
            "https://b.s3.eu-west-1.amazonaws.com/./a.txt?X-Amz-Signature=abc",
        ):
            with self.subTest(url=url), self.assertRaises(base.ConnectorError):
                pu.PresignedUrlConnector.validate_request(credential, [url], _options())

    async def test_only_the_referenced_urls(self):
        url = "https://b.s3.eu-west-1.amazonaws.com/a.txt?X-Amz-Signature=abc"
        connector = pu.PresignedUrlConnector(pu.PresignedUrlConnector.parse_credential({}), _options(), references=[url])
        items = [item async for item in connector.iter_items(url)]
        self.assertTrue(await connector.in_scope(items[0]))
        other = base.SourceItem(
            provider="presigned_url", item_id="x", name="b.txt", mime_type="text/plain", version="1",
            download_hints={"url": "https://b.s3.eu-west-1.amazonaws.com/b.txt?X-Amz-Signature=abc"},
        )
        self.assertFalse(await connector.in_scope(other))


# ---------------------------------------------------------------------------
# Google Drive
# ---------------------------------------------------------------------------
FOLDER = "application/vnd.google-apps.folder"
SHORTCUT = "application/vnd.google-apps.shortcut"


def _file(id_, parent, name=None, mime="text/plain", target=None):
    meta = {"id": id_, "name": name or id_, "mimeType": mime, "parents": [parent] if parent else []}
    if target:
        meta["shortcutDetails"] = {"targetId": target}
    return meta


class FakeDrive:
    """In-memory Drive. Records which folders were listed and which files were read."""

    def __init__(self, files):
        self.files = {f["id"]: f for f in files}
        self.listed = []
        self.read = []

    def __call__(self, request):
        path = request.url.path
        if path.endswith("/files"):
            folder = unquote(request.url.params["q"]).split("'")[1]
            self.listed.append(folder)
            children = [f for f in self.files.values() if folder in f["parents"]]
            return httpx.Response(200, json={"files": children})
        file_id = path.rsplit("/", 1)[-1]
        self.read.append(file_id)
        if file_id not in self.files:
            return httpx.Response(404, json={"error": {"errors": [{"reason": "notFound"}]}})
        return httpx.Response(200, json=self.files[file_id])


# ids must look like Drive ids (10+ chars)
MY_ROOT, FOLDER_A, SUB_A, FOLDER_B, OUTSIDE, OUTSIDE_SUB = (
    "myroot____", "folder_A__", "sub_A_____", "folder_B__", "outside___", "outside_s_",
)
DRIVE = [
    _file(MY_ROOT, None, mime=FOLDER),
    _file(FOLDER_A, MY_ROOT, mime=FOLDER),
    _file("file_A1___", FOLDER_A),
    _file(SUB_A, FOLDER_A, mime=FOLDER),
    _file("file_Asub_", SUB_A),
    _file(FOLDER_B, MY_ROOT, mime=FOLDER),
    _file("file_B1___", FOLDER_B),
    _file(OUTSIDE, MY_ROOT, mime=FOLDER),
    _file("file_out__", OUTSIDE),
    _file(OUTSIDE_SUB, OUTSIDE, mime=FOLDER),
    _file("file_out2_", OUTSIDE_SUB),
    # shortcuts inside FOLDER_A
    _file("sc_to_out_", FOLDER_A, mime=SHORTCUT, target="file_out__"),
    _file("sc_to_outf", FOLDER_A, mime=SHORTCUT, target=OUTSIDE_SUB),
    _file("sc_to_B1__", FOLDER_A, mime=SHORTCUT, target="file_B1___"),
    _file("sc_to_sub_", FOLDER_A, mime=SHORTCUT, target="file_Asub_"),
    # a shortcut used directly as a reference
    _file("sc_ref____", MY_ROOT, mime=SHORTCUT, target=OUTSIDE),
]


class DriveScopeTest(unittest.IsolatedAsyncioTestCase):
    async def collect(self, references, walk=None):
        fake = FakeDrive(DRIVE)
        credential = gd.GoogleDriveConnector.parse_credential({"access_token": "tok"})
        connector = await _open(gd.GoogleDriveConnector(credential, _options(), references=references), fake)
        self.addAsyncCleanup(connector.close)
        in_scope, out_of_scope = [], []
        for reference in walk or references:
            async for item in connector.iter_items(reference):
                (in_scope if await connector.in_scope(item) else out_of_scope).append(item.item_id)
        return fake, sorted(in_scope), sorted(out_of_scope)

    async def test_shortcut_to_a_file_outside_is_out_of_scope(self):
        fake, in_scope, out_of_scope = await self.collect([FOLDER_A])
        self.assertIn("file_out__", out_of_scope)
        self.assertNotIn("file_out__", in_scope)

    async def test_shortcut_to_a_folder_outside_is_never_listed(self):
        fake, in_scope, out_of_scope = await self.collect([FOLDER_A])
        self.assertNotIn(OUTSIDE_SUB, fake.listed)
        self.assertNotIn("file_out2_", in_scope + out_of_scope)

    async def test_shortcuts_inside_the_references_are_followed(self):
        fake, in_scope, _ = await self.collect([FOLDER_A, FOLDER_B], walk=[FOLDER_A])
        self.assertIn("file_B1___", in_scope)  # target inside another referenced folder
        self.assertIn("file_Asub_", in_scope)  # target deeper in the same reference

    async def test_regular_tree(self):
        _, in_scope, out_of_scope = await self.collect([FOLDER_A])
        self.assertEqual(in_scope, ["file_A1___", "file_Asub_"])
        self.assertEqual(out_of_scope, ["file_B1___", "file_out__"])

    async def test_shortcut_given_as_reference_is_followed(self):
        fake, in_scope, out_of_scope = await self.collect(["sc_ref____"])
        self.assertEqual(in_scope, ["file_out2_", "file_out__"])
        self.assertEqual(out_of_scope, [])

    async def test_regular_items_need_no_extra_requests(self):
        fake, _, _ = await self.collect([SUB_A])
        self.assertEqual(fake.read, [SUB_A])


# ---------------------------------------------------------------------------
# Pipeline: the enforcement point
# ---------------------------------------------------------------------------
class PipelineScopeTest(unittest.IsolatedAsyncioTestCase):
    async def test_out_of_scope_items_are_never_downloaded(self):
        downloads = []

        class FakeConnector(base.SourceConnector):
            provider = "fake"
            display_name = "Fake"
            credential_model = base.SourceCredential

            async def iter_items(self, reference, recursive=True):
                for item_id in ("inside.txt", "outside.txt"):
                    yield base.SourceItem(provider="fake", item_id=item_id, name=item_id, mime_type="text/plain", version="1")

            async def in_scope(self, item):
                return item.item_id == "inside.txt" and list(self._references) == ["ref"]

            async def download(self, item, destination):
                downloads.append(item.item_id)
                destination.write(b"data")
                return base.DownloadResult(bytes_written=4, mime_type="text/plain")

        class NoPoints:
            async def has_points(self, metadata_filter):
                return False

        job = pipeline.IngestionJob(
            job_id="job", provider="fake", references=["ref"], recursive=True, visibility="owner",
            user_id="user", agent_id="agent",
        )
        report = pipeline.JobReport(job_id="job", provider="fake")
        runner = pipeline.ConnectorIngestionPipeline(None, None, config.ConnectorsSettings(), {"text/plain"})
        with tempfile.TemporaryDirectory() as workdir:
            downloaded = await runner._collect(FakeConnector, base.SourceCredential(), job, NoPoints(), workdir, report)
        self.assertEqual(downloads, ["inside.txt"])
        self.assertEqual([d.item.item_id for d in downloaded], ["inside.txt"])
        self.assertEqual(report.out_of_scope, 1)
        self.assertIn("out_of_scope=1", report.summary())


if __name__ == "__main__":
    unittest.main()
