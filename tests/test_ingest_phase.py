"""Ingestion through the RabbitHole and swap of the item versions (phase 2, and the unchanged check of phase 1).

Run from the root of the Cat core: ``python -m unittest discover -s cat/plugins/cat-with-cloud-sources/tests``.

The Cat imports every ``.py`` file of the plugin, tests included: at import time this module
needs only the stdlib, the plugin and hypothesis are loaded in ``setUpModule``.
"""
import asyncio
import contextvars
import io
import itertools
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

pipeline = base = md = None

PLUGIN_PATH = str(Path(__file__).resolve().parents[1])
PROVIDER = "s3"
OWNER = "user-1"

#: the job a fake call belongs to, for the step-by-step interleaving of concurrent jobs
_current_job = contextvars.ContextVar("current_job", default=None)


def _plugin_modules():
    from cat.looking_glass.mad_hatter.plugin import Plugin

    plugin = Plugin(PLUGIN_PATH)
    plugin._load_decorated_functions()
    package = plugin.overrides["load_settings"].function.__module__.rsplit(".", 1)[0]
    return lambda name: sys.modules[f"{package}.{name}"]


def setUpModule():
    global pipeline, base, md
    module = _plugin_modules()
    pipeline = module("ingestion.pipeline")
    base = module("connectors.base")
    md = module("ingestion.metadata")


class VectorDBDown(Exception):
    pass


class Gates:
    """Step-by-step execution of concurrent jobs: every vector DB / RabbitHole call waits for a step of its job."""

    def __init__(self):
        self.steps = {}  # job id -> asyncio.Semaphore
        self.fault = {}  # job id -> the next call of the job fails

    def add(self, job_id):
        self.steps[job_id] = asyncio.Semaphore(0)
        self.fault[job_id] = False

    async def wait(self) -> bool:
        """Wait for the step of the current job; True when that step must fail."""
        job_id = _current_job.get()
        if job_id is None or job_id not in self.steps:
            return False
        await self.steps[job_id].acquire()
        fault, self.fault[job_id] = self.fault[job_id], False
        return fault


class FakeVectorMemory:
    """The vector memory calls of ``PointIndex``, with injectable faults."""

    def __init__(self, gates=None):
        self.points = {}  # id -> metadata
        self.fail_reads = False
        self.fail_deletes = False
        self.gates = gates or Gates()

    async def get_all_tenant_points(self, collection, limit, offset=None, metadata=None, with_vectors=False):
        if await self.gates.wait() or self.fail_reads:
            raise VectorDBDown("vector db unreachable")
        matching = sorted(
            pid for pid, meta in self.points.items()
            if all(meta.get(k) == v for k, v in (metadata or {}).items())
        )
        start = offset or 0
        page = matching[start:start + limit]
        following = start + limit if start + limit < len(matching) else None
        return [
            SimpleNamespace(id=pid, payload={"page_content": "x", "metadata": dict(self.points[pid])})
            for pid in page
        ], following

    async def delete_tenant_points_by_ids(self, collection, ids):
        if await self.gates.wait() or self.fail_deletes:
            raise VectorDBDown("vector db unreachable")
        for pid in ids:
            self.points.pop(pid, None)


class FakeRabbitHole:
    """``RabbitHole.ingest_file`` as the core runs it: errors are swallowed, the points land in one upsert."""

    def __init__(self, memory: FakeVectorMemory):
        self._memory = memory
        self._ids = itertools.count()
        self._clock = itertools.count(1)
        self.calls = []
        self.produce_chunks = True
        self.produce = {}  # job id -> the ingestion of that job produces chunks
        self.skew = {}  # job id -> clock offset of the pod running the job
        self.chunks = 3

    def advance(self, seconds):
        """Time passing with no job running."""
        self._clock = itertools.count(next(self._clock) + seconds)

    async def ingest_file(self, cat, file, metadata, filename=None, store_file=True, content_type=None):
        self.calls.append(dict(
            cat=cat, content=file.read(), metadata=metadata, filename=filename,
            store_file=store_file, content_type=content_type,
        ))
        job_id = _current_job.get()
        # the core stamps the chunks before embedding them, with the clock of its pod
        when = float(next(self._clock)) + self.skew.get(job_id, 0)
        if await self._memory.gates.wait() or not self.produce.get(job_id, self.produce_chunks):
            return None  # the core logs the error and returns
        # the chunks, plus the catalogue card a plugin may add with the metadata of the first chunk
        for _ in range(self.chunks + 1):
            self._memory.points[f"p{next(self._ids)}"] = {**metadata, "source": filename, "when": when}
        return None


class FakeConnector:
    """A connector listing one file per reference (``item_id@version``)."""

    def __init__(self, credential, options, references=()):
        self.references = references

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def iter_items(self, reference, recursive=False):
        item_id, version, *downloaded = reference.split("@")
        self.downloaded_version = downloaded[0] if downloaded else None
        yield base.SourceItem(
            provider=PROVIDER, item_id=item_id, name="doc.txt", mime_type="text/plain", version=version, size=5,
        )

    async def in_scope(self, item):
        return True

    async def download(self, item, fh):
        fh.write(b"hello")
        return base.DownloadResult(
            bytes_written=5, mime_type="text/plain", extension=".txt", version=self.downloaded_version,
        )


def _settings(store_files=False):
    return SimpleNamespace(
        store_files=store_files, max_items_scanned=100, max_files_per_job=100, max_file_bytes=1000,
        max_total_bytes=10000, allowed_host_suffixes=(), allow_http_urls=False, oauth_clients={},
    )


def _make_pipeline(memory, rabbit_hole, store_files=False):
    ccat = SimpleNamespace(vector_memory_handler=memory, lizard=SimpleNamespace(rabbit_hole=rabbit_hole))
    return pipeline.ConnectorIngestionPipeline(ccat, ccat, _settings(store_files), {"text/plain"}), ccat


def _job(job_id="job-1", references=("bucket/doc.txt",)):
    return pipeline.IngestionJob(
        job_id=job_id, provider=PROVIDER, references=list(references), recursive=False,
        visibility=md.VISIBILITY_OWNER, user_id=OWNER, agent_id="agent", user_metadata={"team": "a"},
    )


def _downloaded(workdir, item_id, version, content=b"hello", tag=""):
    path = os.path.join(workdir, f"{item_id}-{version}{tag}.bin")
    Path(path).write_bytes(content)
    item = base.SourceItem(
        provider=PROVIDER, item_id=item_id, name="doc.txt", mime_type="text/plain", version=version,
        path=f"bucket/{item_id}.txt",
    )
    result = base.DownloadResult(bytes_written=len(content), mime_type="text/plain", extension=".txt")
    return pipeline._Downloaded(item=item, path=path, result=result, source_name=f"{item_id}.txt")


def _groups(memory, item_id):
    """The (version, job) groups of an item in memory: one group is one complete ingestion."""
    return {
        (meta[md.KEY_VERSION], meta[md.KEY_JOB]) for meta in memory.points.values()
        if meta.get(md.KEY_ITEM_ID) == item_id
    }


def _versions(memory, item_id):
    return {version for version, _ in _groups(memory, item_id)}


def _collect_into():
    """Phase 1 stand-in: downloads one file into the job workdir created by ``run``."""

    async def collect(connector_cls, credential, job, index, workdir, report):
        return [_downloaded(workdir, "item-a", "v1")]

    return collect


class IngestPhaseTest(unittest.TestCase):
    def setUp(self):
        self.workdir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.workdir, True)
        self.memory = FakeVectorMemory()
        self.rabbit_hole = FakeRabbitHole(self.memory)
        self.pipe, self.ccat = _make_pipeline(self.memory, self.rabbit_hole, store_files=True)
        self.job = _job()

    def _run(self, *entries, job=None):
        job = job or self.job
        report = pipeline.JobReport(job_id=job.job_id, provider=PROVIDER)
        index = pipeline.PointIndex(self.ccat, None)
        asyncio.run(self.pipe._ingest(list(entries), job, index, report))
        return report

    def _scan(self, item_id, version, job_id="job-scan"):
        """A whole job (phase 1 + 2) on one item listed at ``version``."""
        return self._scan_ref(f"{item_id}@{version}", job_id)

    def _scan_ref(self, reference, job_id):
        job = _job(job_id, references=[reference])
        return asyncio.run(self.pipe.run(FakeConnector, credential=None, job=job))

    def test_ingests_through_the_rabbit_hole_without_warnings(self):
        entry = _downloaded(self.workdir, "item-a", "v1", b"content")
        with patch.object(pipeline.log, "warning") as warning:
            report = self._run(entry)

        warning.assert_not_called()
        self.assertEqual((report.ingested, report.failed), (1, 0))
        call = self.rabbit_hole.calls[0]
        self.assertIs(call["cat"], self.ccat)
        self.assertEqual(call["content"], b"content")
        self.assertEqual(call["filename"], "item-a.txt")
        self.assertTrue(call["store_file"])
        self.assertEqual(call["content_type"], "text/plain")
        self.assertEqual(call["metadata"][md.KEY_VERSION], "v1")
        self.assertEqual(call["metadata"][md.KEY_OWNER], OWNER)
        self.assertFalse(os.path.exists(entry.path), "the downloaded file must be removed")

    def test_new_version_replaces_the_old_one(self):
        self._run(_downloaded(self.workdir, "item-a", "v1"))
        report = self._run(_downloaded(self.workdir, "item-a", "v2"), job=_job("job-2"))

        self.assertEqual((report.ingested, report.failed), (1, 0))
        self.assertEqual(_groups(self.memory, "item-a"), {("v2", "job-2")})

    def test_no_chunks_keeps_the_old_version(self):
        self._run(_downloaded(self.workdir, "item-a", "v1"))
        self.rabbit_hole.produce_chunks = False
        entry = _downloaded(self.workdir, "item-a", "v2")
        report = self._run(entry, job=_job("job-2"))

        self.assertEqual((report.ingested, report.failed), (0, 1))
        self.assertIn("ingestion produced no chunks", report.errors[0])
        self.assertEqual(_versions(self.memory, "item-a"), {"v1"})
        self.assertFalse(os.path.exists(entry.path))

    def test_vector_db_error_fails_the_item_and_continues(self):
        self.memory.fail_reads = True
        first = _downloaded(self.workdir, "item-a", "v1")
        report = self._run(first, _downloaded(self.workdir, "item-b", "v1"))
        self.assertEqual((report.ingested, report.failed), (0, 2))
        self.assertIn("VectorDBDown", report.errors[0])
        self.assertFalse(os.path.exists(first.path))

    def test_missing_rabbit_hole_aborts_the_job_and_cleans_up(self):
        # the lizard drops the RabbitHole on shutdown, while phase 1 may still be running
        self.ccat.lizard.rabbit_hole = None
        self.pipe._collect = _collect_into()
        report = asyncio.run(self.pipe.run(object, credential=None, job=self.job))

        self.assertEqual(report.aborted_reason, "Unexpected error: AttributeError")
        self.assertEqual(report.ingested, 0)
        self.assertEqual(self.memory.points, {})
        self.assertFalse(any(Path(tempfile.gettempdir()).glob(f"cat-connector-{self.job.job_id}-*")))

    def test_old_version_left_by_a_failed_delete_is_removed_by_the_next_job(self):
        self._scan("item-a", "v1", job_id="job-1")
        self.memory.fail_deletes = True
        report = self._scan("item-a", "v2", job_id="job-2")
        self.memory.fail_deletes = False
        self.assertEqual(report.failed, 1)
        self.assertEqual(_versions(self.memory, "item-a"), {"v1", "v2"})

        report = self._scan("item-a", "v2", job_id="job-3")

        self.assertEqual((report.unchanged, report.failed), (1, 0))
        self.assertEqual(_groups(self.memory, "item-a"), {("v2", "job-2")})

    def test_a_version_in_memory_but_not_the_latest_copy_is_ingested_again(self):
        # v2 is in memory, but a late job wrote v1 after it (and could not remove v2)
        self._scan("item-a", "v2", job_id="job-1")
        self.memory.fail_deletes = True
        self._scan("item-a", "v1", job_id="job-2")
        self.memory.fail_deletes = False
        self.assertEqual(_versions(self.memory, "item-a"), {"v1", "v2"})

        report = self._scan("item-a", "v2", job_id="job-3")

        self.assertEqual((report.ingested, report.unchanged), (1, 0))
        self.assertEqual(_groups(self.memory, "item-a"), {("v2", "job-3")})

    def test_no_point_in_memory_after_a_concurrent_replacement_is_a_failure(self):
        # the copy of this job was replaced by a later copy of another version before the check
        gates = self.memory.gates

        async def scenario():
            gates.add("job-a")
            entry = _downloaded(self.workdir, "item-a", "v1")
            task = _start(self.pipe, self.ccat, entry, _job("job-a"))
            gates.steps["job-a"].release()  # job-a writes v1
            await _settle()
            self.memory.points = {
                pid: {**meta, md.KEY_VERSION: "v2", md.KEY_JOB: "job-z", "when": 99.0}
                for pid, meta in self.memory.points.items()
            }
            for _ in range(3):
                gates.steps["job-a"].release()
            await _settle()
            return await task

        report = asyncio.run(scenario())

        self.assertEqual((report.ingested, report.unchanged, report.failed), (0, 0, 1))
        self.assertEqual(_versions(self.memory, "item-a"), {"v2"})

    def test_unchanged_cleanup_error_does_not_fail_the_scan(self):
        self._scan("item-a", "v1", job_id="job-1")
        self.memory.fail_deletes = True
        self._scan("item-a", "v2", job_id="job-2")

        report = self._scan("item-a", "v2", job_id="job-3")

        self.assertEqual((report.unchanged, report.failed), (1, 0))
        self.assertIsNone(report.aborted_reason)
        self.assertEqual(_versions(self.memory, "item-a"), {"v1", "v2"})

    def test_version_known_after_the_download(self):
        # pre-signed URLs: the listing has no version, the ETag arrives with the download
        report = self._scan_ref("item-a@@etag-1", "job-1")
        self.assertEqual((report.ingested, report.unchanged), (1, 0))
        self.assertEqual(_versions(self.memory, "item-a"), {"etag-1"})

        report = self._scan_ref("item-a@@etag-1", "job-2")
        self.assertEqual((report.ingested, report.unchanged, report.downloaded), (0, 1, 0))
        self.assertEqual(_groups(self.memory, "item-a"), {("etag-1", "job-1")})

        with patch.object(pipeline.PointIndex, "point_metadata", _fail_after(1)):
            report = self._scan_ref("item-a@@etag-1", "job-3")
        self.assertEqual((report.failed, report.downloaded), (1, 0))
        self.assertIsNone(report.aborted_reason)

    def test_point_metadata_reads_every_page(self):
        index = pipeline.PointIndex(self.ccat, None)
        index.PAGE_SIZE = 2
        self._run(_downloaded(self.workdir, "item-a", "v1"))

        points = asyncio.run(index.point_metadata({md.KEY_ITEM_ID: "item-a"}))

        self.assertEqual(len(points), self.rabbit_hole.chunks + 1)
        self.assertTrue(all(meta[md.KEY_VERSION] == "v1" for meta in points.values()))

    def test_late_job_on_the_old_version_never_empties_the_item(self):
        # job-b (v1) is in memory; job-c (v2) and a late job-a (v1, listed before the change) run together
        self._run(_downloaded(self.workdir, "item-a", "v1"), job=_job("job-b"))
        jobs = {
            "job-c": _downloaded(self.workdir, "item-a", "v2", tag="c"),
            "job-a": _downloaded(self.workdir, "item-a", "v1", tag="a"),
        }
        # c writes, a writes, c reads, a reads, a deletes, c deletes
        order = ["job-c", "job-a", "job-c", "job-a", "job-a", "job-c"]

        reports = asyncio.run(_interleave(self.pipe, self.ccat, self.memory.gates, jobs, order))

        self.assertEqual(_groups(self.memory, "item-a"), {("v1", "job-a")})
        self.assertEqual([(r.ingested, r.failed) for r in reports], [(1, 0), (1, 0)])

    def test_late_unchanged_scan_never_empties_the_item(self):
        # job-w (v1) is in memory; job-c writes v2, a late job-x writes v1, a scan listed at v1 runs meanwhile
        self._run(_downloaded(self.workdir, "item-a", "v1"), job=_job("job-w"))
        jobs = {
            "job-c": _downloaded(self.workdir, "item-a", "v2", tag="c"),
            "job-x": _downloaded(self.workdir, "item-a", "v1", tag="x"),
            "job-s": "item-a@v1",
        }
        order = ["job-c", "job-x", "job-s", "job-c", "job-s", "job-c", "job-x", "job-x"]

        reports = asyncio.run(_interleave(self.pipe, self.ccat, self.memory.gates, jobs, order))

        self.assertEqual(_groups(self.memory, "item-a"), {("v1", "job-x")})
        self.assertEqual([r.failed for r in reports], [0, 0, 0])
        self.assertEqual(reports[2].unchanged, 1)

    def test_concurrent_jobs_on_the_same_version_leave_one_copy(self):
        gates = self.memory.gates

        async def scenario():
            tasks = []
            for job_id in ("job-a", "job-b"):
                gates.add(job_id)
                entry = _downloaded(self.workdir, "item-a", "v1", tag=job_id)
                tasks.append(_start(self.pipe, self.ccat, entry, _job(job_id)))
            await asyncio.sleep(0)
            # both read the previous points, both write, then both verify and swap
            for job_id in ("job-a", "job-b", "job-a", "job-b"):
                gates.steps[job_id].release()
                await _settle()
            for _ in range(4):
                for job_id in ("job-a", "job-b"):
                    gates.steps[job_id].release()
                await _settle()
            return [await task for task in tasks]

        reports = asyncio.run(scenario())

        self.assertEqual([(r.ingested, r.failed) for r in reports], [(1, 0), (1, 0)])
        self.assertEqual(len(_groups(self.memory, "item-a")), 1)


def _fail_after(calls):
    """A ``point_metadata`` that answers ``calls`` times (with no points), then fails like a vector DB outage."""
    seen = itertools.count()

    async def point_metadata(self, metadata_filter):
        if next(seen) < calls:
            return {}
        raise VectorDBDown("vector db unreachable")

    return point_metadata


async def _interleave(pipe, ccat, gates, jobs, order):
    """Run ``jobs`` (an entry to ingest, or a reference to scan) one call at a time in ``order``, then drain them."""
    tasks = []
    for job_id, work in jobs.items():
        gates.add(job_id)
        if isinstance(work, str):
            tasks.append(_start_scan(pipe, work, job_id))
        else:
            tasks.append(_start(pipe, ccat, work, _job(job_id)))
    await _settle()
    for job_id in order:
        gates.steps[job_id].release()
        await _settle()
    while not all(task.done() for task in tasks):
        for job_id in jobs:
            gates.steps[job_id].release()
        await _settle()
    return [task.result() for task in tasks]


def _start_scan(pipe, reference, job_id):
    async def body():
        _current_job.set(job_id)
        return await pipe.run(FakeConnector, credential=None, job=_job(job_id, references=[reference]))

    return asyncio.ensure_future(body())


def _start(pipe, ccat, entry, job):
    async def body():
        _current_job.set(job.job_id)
        report = pipeline.JobReport(job_id=job.job_id, provider=PROVIDER)
        await pipe._ingest([entry], job, pipeline.PointIndex(ccat, None), report)
        return report

    return asyncio.ensure_future(body())


async def _settle():
    for _ in range(20):
        await asyncio.sleep(0)


def _stateful_case():
    from hypothesis import settings as h_settings
    from hypothesis import strategies as st
    from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

    # faults and empty ingestions are rare: frequent ones would stop most jobs before they interleave
    rare = st.integers(0, 7).map(lambda n: n == 0)

    class ConcurrentJobsMachine(RuleBasedStateMachine):
        """Concurrent jobs on one item, interleaved call by call, on pods with skewed clocks, with vector DB
        faults, empty ingestions and listings that lag behind the source.

        Rules checked at every step:
        - once a copy of the item has been confirmed in memory, the item is never left without content;
        - every finished job counts its file exactly once (ingested, unchanged or failed), and is never aborted;
        - with no job running, a job listing the current version of the source leaves exactly one copy of the
          item, at that version.
        """

        def __init__(self):
            super().__init__()
            self.loop = asyncio.new_event_loop()
            self.workdir = tempfile.mkdtemp()
            self.gates = Gates()
            self.memory = FakeVectorMemory(self.gates)
            self.rabbit_hole = FakeRabbitHole(self.memory)
            self.pipe, self.ccat = _make_pipeline(self.memory, self.rabbit_hole)
            self.jobs = itertools.count()
            self.running = {}  # job id -> task
            self.confirmed = False
            self.versions = ["v0"]  # every version the source had, the last one is the current one

        def teardown(self):
            for task in self.running.values():
                task.cancel()
            self.loop.run_until_complete(asyncio.sleep(0))
            self.loop.close()
            shutil.rmtree(self.workdir, ignore_errors=True)

        def _launch(self, work, chunks=True, skew=0):
            job_id = f"job-{next(self.jobs):03d}"
            self.gates.add(job_id)
            self.rabbit_hole.produce[job_id] = chunks
            self.rabbit_hole.skew[job_id] = skew

            async def body():
                _current_job.set(job_id)
                if isinstance(work, str):
                    return await self.pipe.run(FakeConnector, credential=None, job=_job(job_id, references=[work]))
                report = pipeline.JobReport(job_id=job_id, provider=PROVIDER)
                entry = _downloaded(self.workdir, "item", work[0], tag=job_id)
                await self.pipe._ingest([entry], _job(job_id), pipeline.PointIndex(self.ccat, None), report)
                return report

            self.running[job_id] = self.loop.create_task(body())
            self._settle()
            return job_id

        def _settle(self):
            self.loop.run_until_complete(_settle())
            for job_id, task in list(self.running.items()):
                if task.done():
                    del self.running[job_id]
                    report = task.result()
                    assert report.aborted_reason is None, report.aborted_reason
                    assert report.ingested + report.failed + report.unchanged == 1, report.summary()

        @rule()
        def source_changes(self):
            self.versions.append(f"v{len(self.versions)}")

        @rule(data=st.data(), empty=rare, skew=st.sampled_from([-3, 0, 3]))
        def start_ingestion(self, data, empty, skew):
            """A job that downloaded the item (phase 2 only), at a version it listed, possibly an old one."""
            version = data.draw(st.sampled_from(self.versions))
            self._launch((version,), not empty, skew)

        @rule(data=st.data(), empty=rare, skew=st.sampled_from([-3, 0, 3]))
        def start_scan(self, data, empty, skew):
            """A whole job (phase 1 + 2) listing the item at a version, possibly an old one."""
            version = data.draw(st.sampled_from(self.versions))
            self._launch(f"item@{version}", not empty, skew)

        @precondition(lambda self: self.running)
        @rule(data=st.data(), fault=rare)
        def step(self, data, fault):
            job_id = data.draw(st.sampled_from(sorted(self.running)))
            self.gates.fault[job_id] = fault
            self.gates.steps[job_id].release()
            self._settle()
            if any(meta.get(md.KEY_ITEM_ID) == "item" for meta in self.memory.points.values()):
                self.confirmed = True

        @precondition(lambda self: not self.running)
        @rule()
        def quiet_scan_leaves_one_current_copy(self):
            current = self.versions[-1]
            # pod clocks drift by seconds, far less than the time between two jobs on the same item
            self.rabbit_hole.advance(10)
            job_id = self._launch(f"item@{current}")
            while job_id in self.running:
                self.gates.steps[job_id].release()
                self._settle()
            self.confirmed = True
            groups = _groups(self.memory, "item")
            assert len(groups) == 1 and next(iter(groups))[0] == current, (current, groups)

        @invariant()
        def never_empty_once_confirmed(self):
            if self.confirmed:
                assert _groups(self.memory, "item"), "the item lost all its content"

    ConcurrentJobsMachine.TestCase.settings = h_settings(max_examples=300, stateful_step_count=40, deadline=None)
    return ConcurrentJobsMachine.TestCase


class IngestStatefulTest(unittest.TestCase):
    def test_concurrent_jobs_invariants(self):
        case = _stateful_case()
        result = unittest.TextTestRunner(stream=io.StringIO(), verbosity=0).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(case)
        )
        self.assertTrue(result.wasSuccessful(), result.failures + result.errors)


if __name__ == "__main__":
    unittest.main()
