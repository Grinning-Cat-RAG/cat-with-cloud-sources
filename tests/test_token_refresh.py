"""Automatic refresh of access tokens (Google Drive, Azure Blob with an Entra ID bearer token).

Run from the plugin root: ``python -m unittest discover -s tests``.

The plugin modules are imported lazily in ``setUpModule``: the Cat imports every
``.py`` file of the plugin, and at import time this module needs only the stdlib.
"""
import asyncio
import importlib
import json
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch
from urllib.parse import parse_qs

base = gd = az = config = httpx = SecretStr = None

FILE_ID = "A" * 12
TOKEN_HOST = "oauth2.googleapis.com"


def setUpModule():
    global base, gd, az, config, httpx, SecretStr
    package = "cloud_sources_under_test"
    if package not in sys.modules:
        module = types.ModuleType(package)
        module.__path__ = [str(Path(__file__).resolve().parents[1])]
        sys.modules[package] = module
    base = importlib.import_module(f"{package}.connectors.base")
    gd = importlib.import_module(f"{package}.connectors.google_drive")
    az = importlib.import_module(f"{package}.connectors.azure_blob")
    config = importlib.import_module(f"{package}.ingestion.config")
    httpx = importlib.import_module("httpx")
    SecretStr = importlib.import_module("pydantic").SecretStr


def _in(seconds: int) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


class FakeGoogle:
    """Drive API + OAuth token endpoint. Drive accepts only the tokens in ``valid``."""

    def __init__(self, valid=("new-token",), token_responses=None):
        self.valid = set(valid)
        self.token_responses = list(token_responses or [(200, {"access_token": "new-token", "expires_in": 3599})])
        self.token_requests = []
        self.drive_tokens = []

    def __call__(self, request):
        if request.url.host == TOKEN_HOST:
            self.token_requests.append({k: v[0] for k, v in parse_qs(request.content.decode()).items()})
            status, body = self.token_responses.pop(0) if len(self.token_responses) > 1 else self.token_responses[0]
            # json.dumps, not json=: httpx refuses NaN, which a real server may still send
            return httpx.Response(status, content=json.dumps(body).encode(), headers={"Content-Type": "application/json"})
        token = request.headers.get("Authorization", "").removeprefix("Bearer ")
        self.drive_tokens.append(token)
        if token not in self.valid:
            return httpx.Response(401, json={"error": {"errors": [{"reason": "authError"}]}})
        return httpx.Response(200, json={"id": FILE_ID, "name": "f.txt", "mimeType": "text/plain"})


class TokenRefreshTest(unittest.IsolatedAsyncioTestCase):
    def options(self, oauth=True):
        clients = {"google_drive": base.OAuthClient("client-id", SecretStr("client-secret"))} if oauth else {}
        return base.ConnectorOptions(max_file_bytes=1024, max_retries=2, oauth_clients=clients)

    def credential(self, **raw):
        return gd.GoogleDriveConnector.parse_credential(raw)

    async def connector(self, credential, fake, oauth=True):
        connector = gd.GoogleDriveConnector(credential, self.options(oauth))
        await connector.open()
        await connector._client.aclose()
        connector._client = httpx.AsyncClient(transport=httpx.MockTransport(fake), follow_redirects=True)
        self.addAsyncCleanup(connector.close)
        return connector

    # -- request validation ----------------------------------------------------
    def test_expired_access_token_accepted_with_refresh_token(self):
        credential = self.credential(access_token="old", refresh_token="rt", expires_at=_in(-60))
        self.assertTrue(credential.refreshable)

    def test_expired_access_token_rejected_without_refresh_token(self):
        with self.assertRaises(base.CredentialError):
            self.credential(access_token="old", expires_at=_in(-60))

    def test_refresh_token_requires_oauth_client(self):
        credential = self.credential(access_token="old", refresh_token="rt")
        with self.assertRaises(base.ConnectorError) as ctx:
            gd.GoogleDriveConnector.validate_refresh(credential, self.options(oauth=False))
        self.assertIn("OAuth client", str(ctx.exception))
        gd.GoogleDriveConnector.validate_refresh(credential, self.options(oauth=True))
        # without a refresh token no OAuth client is needed
        gd.GoogleDriveConnector.validate_refresh(self.credential(access_token="old"), self.options(oauth=False))

    def test_refresh_token_never_in_repr(self):
        credential = self.credential(access_token="old", refresh_token="very-secret-rt")
        self.assertNotIn("very-secret-rt", repr(credential))

    # -- proactive refresh -----------------------------------------------------
    async def test_refreshes_before_request_when_expiring(self):
        fake = FakeGoogle()
        connector = await self.connector(self.credential(access_token="old", refresh_token="rt", expires_at=_in(10)), fake)
        await connector._get_file(FILE_ID)
        await connector._get_file(FILE_ID)
        self.assertEqual(fake.drive_tokens, ["new-token", "new-token"])
        self.assertEqual(len(fake.token_requests), 1, "expires_at must be updated from expires_in")
        self.assertEqual(
            fake.token_requests[0],
            {"grant_type": "refresh_token", "refresh_token": "rt", "client_id": "client-id", "client_secret": "client-secret"},
        )

    async def test_expired_without_refresh_token_stops(self):
        fake = FakeGoogle(valid=("old",))
        credential = self.credential(access_token="old", expires_at=_in(3600))
        connector = await self.connector(credential, fake)
        connector._credential = credential.model_copy(update={"expires_at": datetime.now(timezone.utc)})
        with self.assertRaises(base.CredentialError):
            await connector._get_file(FILE_ID)
        self.assertEqual(fake.token_requests, [])
        self.assertEqual(fake.drive_tokens, [])

    # -- refresh on 401 --------------------------------------------------------
    async def test_refreshes_once_on_401(self):
        fake = FakeGoogle()
        connector = await self.connector(self.credential(access_token="old", refresh_token="rt"), fake)
        meta = await connector._get_file(FILE_ID)
        self.assertEqual(meta["id"], FILE_ID)
        self.assertEqual(fake.drive_tokens, ["old", "new-token"])
        self.assertEqual(len(fake.token_requests), 1)

    async def test_401_after_refresh_is_fatal(self):
        fake = FakeGoogle(valid=())
        connector = await self.connector(self.credential(access_token="old", refresh_token="rt"), fake)
        with self.assertRaises(base.CredentialError):
            await connector._get_file(FILE_ID)
        self.assertEqual(len(fake.token_requests), 1)
        self.assertEqual(fake.drive_tokens, ["old", "new-token"])

    async def test_401_without_refresh_token_is_fatal(self):
        fake = FakeGoogle()
        connector = await self.connector(self.credential(access_token="old"), fake)
        with self.assertRaises(base.CredentialError):
            await connector._get_file(FILE_ID)
        self.assertEqual(fake.token_requests, [])

    async def test_403_does_not_refresh(self):
        def handler(request):
            if request.url.host == TOKEN_HOST:
                self.fail("403 means missing scope or permission: refreshing cannot help")
            return httpx.Response(403, json={"error": {"errors": [{"reason": "forbidden"}]}})

        connector = await self.connector(self.credential(access_token="old", refresh_token="rt"), handler)
        with self.assertRaises(base.CredentialError):
            await connector._get_file(FILE_ID)

    async def test_streaming_download_refreshes_on_401(self):
        def handler(request):
            if request.url.host == TOKEN_HOST:
                return httpx.Response(200, json={"access_token": "new-token", "expires_in": 3599})
            if request.headers["Authorization"] != "Bearer new-token":
                return httpx.Response(401)
            return httpx.Response(200, content=b"hello")

        connector = await self.connector(self.credential(access_token="old", refresh_token="rt"), handler)
        item = base.SourceItem(provider="google_drive", item_id=FILE_ID, name="f.txt", mime_type="text/plain", version="1")
        destination = __import__("io").BytesIO()
        result = await connector.download(item, destination)
        self.assertEqual(result.bytes_written, 5)
        self.assertEqual(destination.getvalue(), b"hello")

    # -- token endpoint --------------------------------------------------------
    async def test_invalid_grant_is_fatal_and_leaks_nothing(self):
        fake = FakeGoogle(token_responses=[(400, {"error": "invalid_grant", "error_description": "Token has been expired or revoked."})])
        connector = await self.connector(self.credential(access_token="old", refresh_token="very-secret-rt"), fake)
        with self.assertRaises(base.CredentialError) as ctx:
            await connector._get_file(FILE_ID)
        self.assertIn("invalid_grant", str(ctx.exception))
        self.assertNotIn("very-secret-rt", str(ctx.exception))
        self.assertNotIn("client-secret", str(ctx.exception))

    async def test_token_endpoint_transient_errors_are_retried(self):
        fake = FakeGoogle(token_responses=[(503, {}), (200, {"access_token": "new-token", "expires_in": 3599})])
        connector = await self.connector(self.credential(access_token="old", refresh_token="rt", expires_at=_in(0)), fake)
        with patch.object(base, "asyncio", types.SimpleNamespace(sleep=AsyncMock(), Lock=asyncio.Lock)):
            await connector._get_file(FILE_ID)
        self.assertEqual(len(fake.token_requests), 2)
        self.assertEqual(fake.drive_tokens, ["new-token"])

    async def test_token_response_without_access_token_is_fatal(self):
        fake = FakeGoogle(token_responses=[(200, {"token_type": "Bearer"})])
        connector = await self.connector(self.credential(access_token="old", refresh_token="rt", expires_at=_in(0)), fake)
        with self.assertRaises(base.CredentialError):
            await connector._get_file(FILE_ID)

    async def test_token_response_not_json_is_fatal(self):
        def handler(request):
            if request.url.host == TOKEN_HOST:
                return httpx.Response(200, content=b"<html>proxy error</html>")
            self.fail("Drive must not be called without a valid token")

        connector = await self.connector(self.credential(access_token="old", refresh_token="rt", expires_at=_in(0)), handler)
        with self.assertRaises(base.CredentialError):
            await connector._get_file(FILE_ID)

    async def test_invalid_expires_in_is_ignored(self):
        for expires_in in (10 ** 20, -5, float("nan"), "3599", True):
            with self.subTest(expires_in=expires_in):
                fake = FakeGoogle(token_responses=[(200, {"access_token": "new-token", "expires_in": expires_in})])
                connector = await self.connector(
                    self.credential(access_token="old", refresh_token="rt", expires_at=_in(0)), fake
                )
                await connector._get_file(FILE_ID)
                self.assertIsNone(connector._credential.expires_at)
                self.assertEqual(fake.drive_tokens, ["new-token"])

    async def test_rotated_refresh_token_is_used_next_time(self):
        fake = FakeGoogle(
            valid=("second",),
            token_responses=[
                (200, {"access_token": "first", "expires_in": 1, "refresh_token": "rt-2"}),
                (200, {"access_token": "second", "expires_in": 3599}),
            ],
        )
        connector = await self.connector(self.credential(access_token="old", refresh_token="rt-1", expires_at=_in(0)), fake)
        await connector._get_file(FILE_ID)
        self.assertEqual([r["refresh_token"] for r in fake.token_requests], ["rt-1", "rt-2"])

    async def test_concurrent_requests_refresh_once(self):
        fake = FakeGoogle()
        connector = await self.connector(self.credential(access_token="old", refresh_token="rt", expires_at=_in(0)), fake)
        await asyncio.gather(*(connector._get_file(FILE_ID) for _ in range(5)))
        self.assertEqual(len(fake.token_requests), 1)


ACCOUNT_URL = "https://acct.blob.core.windows.net"
ENTRA_HOST = "login.microsoftonline.com"
ENTRA_HOSTS = {"login.microsoftonline.com", "login.microsoftonline.us", "login.chinacloudapi.cn"}
TENANT = "contoso.onmicrosoft.com"


class FakeAzure:
    """Blob API + Entra ID token endpoint. Blob storage accepts only the bearer tokens in ``valid``."""

    def __init__(self, valid=("new-token",), token_responses=None, rejection=(401, "InvalidAuthenticationInfo")):
        self.valid = set(valid)
        self.token_responses = list(token_responses or [(200, {"access_token": "new-token", "expires_in": 3599})])
        self.rejection = rejection
        self.token_requests = []
        self.token_paths = []
        self.token_hosts = []
        self.blob_tokens = []

    def __call__(self, request):
        if request.url.host in ENTRA_HOSTS:
            self.token_hosts.append(request.url.host)
            self.token_paths.append(request.url.path)
            self.token_requests.append({k: v[0] for k, v in parse_qs(request.content.decode()).items()})
            status, body = self.token_responses.pop(0) if len(self.token_responses) > 1 else self.token_responses[0]
            return httpx.Response(status, json=body)
        self.blob_tokens.append(request.headers.get("Authorization", "").removeprefix("Bearer "))
        if self.blob_tokens[-1] not in self.valid:
            status, code = self.rejection
            return httpx.Response(status, headers={"x-ms-error-code": code})
        return httpx.Response(200, content=b"<EnumerationResults/>")


class AzureTokenRefreshTest(unittest.IsolatedAsyncioTestCase):
    def options(self, oauth=True):
        clients = (
            {"azure_blob": base.OAuthClient("client-id", SecretStr("client-secret"), tenant_id=TENANT)} if oauth else {}
        )
        return base.ConnectorOptions(
            max_file_bytes=1024, max_retries=2, oauth_clients=clients,
            allowed_host_suffixes=frozenset(
                {"blob.core.windows.net", "blob.core.usgovcloudapi.net", "blob.core.chinacloudapi.cn", "example.com"}
            ),
        )

    def credential(self, account_url=ACCOUNT_URL, **raw):
        return az.AzureBlobConnector.parse_credential({"account_url": account_url, **raw})

    async def connector(self, credential, fake, oauth=True):
        connector = az.AzureBlobConnector(credential, self.options(oauth))
        await connector.open()
        await connector._client.aclose()
        connector._client = httpx.AsyncClient(transport=httpx.MockTransport(fake))
        self.addAsyncCleanup(connector.close)
        return connector

    async def request(self, connector):
        return await connector._get(connector._container_url("docs"), {"restype": "container"}, "testing")

    # -- request validation ----------------------------------------------------
    def test_refresh_token_only_with_bearer_token(self):
        with self.assertRaises(base.CredentialError) as ctx:
            self.credential(sas_token="sv=1&sig=x", refresh_token="rt")
        self.assertIn("refresh_token", str(ctx.exception))
        self.assertNotIn("sig=x", str(ctx.exception))

    def test_expired_bearer_accepted_with_refresh_token(self):
        credential = self.credential(bearer_token="old", refresh_token="rt", expires_at=_in(-60))
        self.assertTrue(credential.refreshable)
        self.assertFalse(self.credential(bearer_token="old").refreshable)
        self.assertFalse(self.credential(sas_token="sv=1&sig=x").refreshable)

    def test_refresh_token_requires_oauth_client(self):
        credential = self.credential(bearer_token="old", refresh_token="rt")
        with self.assertRaises(base.ConnectorError):
            az.AzureBlobConnector.validate_refresh(credential, self.options(oauth=False))
        az.AzureBlobConnector.validate_refresh(credential, self.options(oauth=True))

    # -- refresh ---------------------------------------------------------------
    async def test_refreshes_before_request_when_expiring(self):
        fake = FakeAzure()
        connector = await self.connector(self.credential(bearer_token="old", refresh_token="rt", expires_at=_in(10)), fake)
        await self.request(connector)
        await self.request(connector)
        self.assertEqual(fake.blob_tokens, ["new-token", "new-token"])
        self.assertEqual(fake.token_paths, [f"/{TENANT}/oauth2/v2.0/token"])
        self.assertEqual(fake.token_hosts, [ENTRA_HOST])
        self.assertEqual(
            fake.token_requests[0],
            {
                "grant_type": "refresh_token", "refresh_token": "rt", "client_id": "client-id",
                "client_secret": "client-secret", "scope": "https://storage.azure.com/.default offline_access",
            },
        )

    async def test_sovereign_clouds_use_their_entra_authority(self):
        clouds = {
            "https://acct.blob.core.usgovcloudapi.net": "login.microsoftonline.us",
            "https://acct.blob.core.chinacloudapi.cn": "login.chinacloudapi.cn",
            "https://acct.privatelink.blob.core.windows.net": "login.microsoftonline.com",
        }
        for account_url, authority in clouds.items():
            with self.subTest(account_url=account_url):
                credential = self.credential(account_url, bearer_token="old", refresh_token="rt", expires_at=_in(0))
                az.AzureBlobConnector.validate_request(credential, ["docs/"], self.options())
                fake = FakeAzure()
                connector = await self.connector(credential, fake)
                await self.request(connector)
                self.assertEqual(fake.token_hosts, [authority])
                self.assertEqual(fake.blob_tokens, ["new-token"])

    def test_refresh_refused_for_unknown_cloud(self):
        # e.g. a custom domain: the Entra ID authority cannot be derived from it
        refreshable = self.credential("https://files.example.com", bearer_token="old", refresh_token="rt")
        with self.assertRaises(base.ConnectorError) as ctx:
            az.AzureBlobConnector.validate_request(refreshable, ["docs/"], self.options())
        self.assertIn("cloud", str(ctx.exception))
        not_refreshable = self.credential("https://files.example.com", bearer_token="old")
        az.AzureBlobConnector.validate_request(not_refreshable, ["docs/"], self.options())

    async def test_unknown_cloud_never_sends_the_client_secret(self):
        def handler(request):
            if request.url.host != "files.example.com":
                self.fail(f"unexpected request to {request.url.host}")
            return httpx.Response(401, headers={"x-ms-error-code": "InvalidAuthenticationInfo"})

        credential = self.credential("https://files.example.com", bearer_token="old", refresh_token="rt")
        connector = await self.connector(credential, handler)
        with self.assertRaises(base.CredentialError):
            await self.request(connector)

    async def test_refreshes_once_on_401(self):
        fake = FakeAzure()
        connector = await self.connector(self.credential(bearer_token="old", refresh_token="rt"), fake)
        await self.request(connector)
        self.assertEqual(fake.blob_tokens, ["old", "new-token"])
        self.assertEqual(len(fake.token_requests), 1)

    async def test_401_after_refresh_is_fatal(self):
        fake = FakeAzure(valid=())
        connector = await self.connector(self.credential(bearer_token="old", refresh_token="rt"), fake)
        with self.assertRaises(base.CredentialError):
            await self.request(connector)
        self.assertEqual(len(fake.token_requests), 1)

    async def test_403_does_not_refresh(self):
        fake = FakeAzure(rejection=(403, "AuthorizationPermissionMismatch"))
        connector = await self.connector(self.credential(bearer_token="old", refresh_token="rt"), fake)
        with self.assertRaises(base.CredentialError):
            await self.request(connector)
        self.assertEqual(fake.token_requests, [])

    async def test_sas_rejection_does_not_refresh(self):
        fake = FakeAzure(valid=(), rejection=(403, "AuthenticationFailed"))
        connector = await self.connector(self.credential(sas_token="sv=1&sig=x"), fake)
        with self.assertRaises(base.CredentialError):
            await self.request(connector)
        self.assertEqual(fake.token_requests, [])

    async def test_rotated_refresh_token_is_used_next_time(self):
        fake = FakeAzure(
            valid=("second",),
            token_responses=[
                (200, {"access_token": "first", "expires_in": 1, "refresh_token": "rt-2"}),
                (200, {"access_token": "second", "expires_in": 3599, "refresh_token": "rt-3"}),
            ],
        )
        connector = await self.connector(self.credential(bearer_token="old", refresh_token="rt-1", expires_at=_in(0)), fake)
        await self.request(connector)
        self.assertEqual([r["refresh_token"] for r in fake.token_requests], ["rt-1", "rt-2"])

    async def test_invalid_grant_is_fatal_and_leaks_nothing(self):
        fake = FakeAzure(token_responses=[(400, {"error": "invalid_grant", "error_description": "AADSTS70008: expired"})])
        connector = await self.connector(self.credential(bearer_token="old", refresh_token="very-secret-rt"), fake)
        with self.assertRaises(base.CredentialError) as ctx:
            await self.request(connector)
        self.assertIn("invalid_grant", str(ctx.exception))
        self.assertNotIn("very-secret-rt", str(ctx.exception))
        self.assertNotIn("AADSTS", str(ctx.exception))


class SettingsTest(unittest.TestCase):
    def test_oauth_client_configured(self):
        settings = config.ConnectorsSettings.from_raw(
            {"google_oauth_client_id": " cid ", "google_oauth_client_secret": " secret "}
        )
        client = settings.oauth_clients["google_drive"]
        self.assertEqual(client.client_id, "cid")
        self.assertEqual(client.client_secret.get_secret_value(), "secret")

    def test_oauth_client_incomplete_is_ignored(self):
        self.assertEqual(config.ConnectorsSettings.from_raw({"google_oauth_client_id": "cid"}).oauth_clients, {})
        self.assertEqual(config.ConnectorsSettings.from_raw({}).oauth_clients, {})

    def test_azure_client_configured(self):
        settings = config.ConnectorsSettings.from_raw(
            {"azure_tenant_id": TENANT, "azure_client_id": "cid", "azure_client_secret": "secret"}
        )
        client = settings.oauth_clients["azure_blob"]
        self.assertEqual((client.tenant_id, client.client_id), (TENANT, "cid"))
        self.assertEqual(client.client_secret.get_secret_value(), "secret")
        self.assertNotIn("google_drive", settings.oauth_clients)

    def test_azure_client_incomplete_is_ignored(self):
        settings = config.ConnectorsSettings.from_raw({"azure_client_id": "cid", "azure_client_secret": "secret"})
        self.assertEqual(settings.oauth_clients, {})

    def test_azure_tenant_must_not_alter_the_token_url(self):
        for tenant in ("evil.example/x", "a?b=c", "../common", "host#frag"):
            with self.subTest(tenant=tenant), self.assertRaises(ValueError):
                config.ConnectorsSettings.from_raw({"azure_tenant_id": tenant})
        for tenant in ("", TENANT, "organizations", "8eaef023-2b34-4da1-9baa-8bc8c9d6a490"):
            config.ConnectorsSettings.from_raw({"azure_tenant_id": tenant})

    def test_from_raw(self):
        self.assertEqual(config.ConnectorsSettings.from_raw(None), config.ConnectorsSettings())
        self.assertEqual(config.ConnectorsSettings.from_raw({"max_files_per_job": 7}).max_files_per_job, 7)

    def test_default_hosts_include_azure_sovereign_clouds(self):
        hosts = config.ConnectorsSettings().allowed_host_suffixes
        for suffix in ("amazonaws.com", "blob.core.windows.net", "blob.core.usgovcloudapi.net", "blob.core.chinacloudapi.cn"):
            self.assertIn(suffix, hosts)
        # every cloud with an Entra ID authority is reachable with the default settings
        self.assertLessEqual(set(az.ENTRA_AUTHORITIES), hosts)

    def test_client_secret_keys_are_masked_by_the_core(self):
        # the core masks setting values whose key contains "_secret", "_key" or "_password"
        for key in ("google_oauth_client_secret", "azure_client_secret"):
            self.assertIn("_secret", key)
            self.assertIn(key, config.ConnectorsSettings.model_fields)


if __name__ == "__main__":
    unittest.main()
