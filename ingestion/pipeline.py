"""Two-phase ingestion of connector items.

Phase 1 (needs the credential): enumerate, drop items outside the requested
references, skip unchanged items, download into a private temporary directory.
When it ends the connector is closed and the credential reference is dropped.

Phase 2 (no credential): ingest every downloaded file through the RabbitHole of
the Cat, then remove the chunks of the previous version of the item (with its
whole source in the Cat, when the item was renamed). Old chunks are deleted only
after the new version is confirmed in memory, so a failed ingestion never loses
the existing content.

Splitting the phases keeps the credential lifetime short: embedding can take
far longer than a user access token lives.
"""
from __future__ import annotations

import asyncio
import dataclasses
import itertools
import os
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple, Type

from cat import log
from cat.rabbit_hole import DELETE_WAIT_SECONDS
from cat.rabbit_hole.chunks import owner_folder
from cat.services.memory.models import VectorMemoryType

from ..connectors.base import (
    ConnectorError,
    ConnectorOptions,
    CredentialError,
    DownloadResult,
    ItemNotFoundError,
    ItemTooLargeError,
    SourceConnector,
    SourceCredential,
    SourceItem,
    UnsupportedItemError,
)
from . import metadata as md
from .config import ConnectorsSettings


def build_connector_options(settings: ConnectorsSettings, accepted_mime_types) -> ConnectorOptions:
    return ConnectorOptions(
        max_file_bytes=settings.max_file_bytes,
        accepted_mime_types=frozenset(accepted_mime_types),
        allowed_host_suffixes=settings.allowed_host_suffixes,
        allow_http=settings.allow_http_urls,
        oauth_clients=settings.oauth_clients,
    )


@dataclass
class IngestionJob:
    job_id: str
    provider: str
    references: List[str]
    recursive: bool
    visibility: str
    user_id: str
    agent_id: str
    chat_id: str | None = None
    user_metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def owner(self) -> str:
        return md.owner_for(self.visibility, self.user_id)


@dataclass
class JobReport:
    job_id: str
    provider: str
    discovered: int = 0
    unchanged: int = 0
    unsupported: int = 0
    over_limit: int = 0
    downloaded: int = 0
    ingested: int = 0
    failed: int = 0
    out_of_scope: int = 0
    truncated: bool = False
    aborted_reason: str | None = None
    errors: List[str] = field(default_factory=list)

    def add_error(self, message: str) -> None:
        if len(self.errors) < 50:
            self.errors.append(message)

    def summary(self) -> str:
        return (
            f"job={self.job_id} provider={self.provider} discovered={self.discovered} "
            f"unchanged={self.unchanged} unsupported={self.unsupported} over_limit={self.over_limit} "
            f"downloaded={self.downloaded} ingested={self.ingested} failed={self.failed} "
            f"out_of_scope={self.out_of_scope}"
            + (" truncated=true" if self.truncated else "")
            + (f" aborted='{self.aborted_reason}'" if self.aborted_reason else "")
        )


@dataclass
class _Downloaded:
    item: SourceItem
    path: str
    result: DownloadResult
    source_name: str

    @property
    def version(self) -> str:
        return self.result.version or self.item.version


class PointIndex:
    """Queries on the connector chunks of one agent (or one chat) in the vector memory."""

    PAGE_SIZE = 256

    def __init__(self, ccat, chat_id: str | None):
        self._handler = ccat.vector_memory_handler
        self.chat_id = chat_id
        self._collection = str(VectorMemoryType.EPISODIC if chat_id else VectorMemoryType.DECLARATIVE)

    async def point_metadata(self, metadata_filter: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """The metadata of every matching point, by point id."""
        points: Dict[str, Dict[str, Any]] = {}
        offset = None
        while True:
            records, offset = await self._handler.get_all_tenant_points(
                self._collection, limit=self.PAGE_SIZE, offset=offset, metadata=metadata_filter, with_vectors=False,
            )
            for record in records:
                points[str(record.id)] = dict((record.payload or {}).get("metadata") or {})
            if not offset or not records:
                return points

    async def delete_ids(self, ids: Set[str]) -> None:
        if ids:
            await self._handler.delete_tenant_points_by_ids(self._collection, list(ids))


class ConnectorIngestionPipeline:
    def __init__(self, ccat, target, settings: ConnectorsSettings, accepted_mime_types: Set[str]):
        """
        Args:
            ccat: the CheshireCat of the agent (vector memory owner).
            target: the object passed to the RabbitHole: the StrayCat for chat-scoped
                ingestion, otherwise the CheshireCat.
        """
        self._ccat = ccat
        self._target = target
        self._settings = settings
        self._accepted = frozenset(accepted_mime_types)
        self._file_counter = itertools.count()

    async def run(
        self, connector_cls: Type[SourceConnector], credential: SourceCredential, job: IngestionJob
    ) -> JobReport:
        report = JobReport(job_id=job.job_id, provider=job.provider)
        index = PointIndex(self._ccat, job.chat_id)
        workdir = tempfile.mkdtemp(prefix=f"cat-connector-{job.job_id}-")
        os.chmod(workdir, 0o700)
        started = time.monotonic()
        try:
            downloaded = await self._collect(connector_cls, credential, job, index, workdir, report)
            del credential  # phase 2 never needs it
            await self._ingest(downloaded, job, index, report)
        except Exception as e:  # noqa: BLE001 - background job: log, never raise
            report.aborted_reason = f"Unexpected error: {type(e).__name__}"
            log.error(f"[connectors] job {job.job_id} failed: {type(e).__name__}: {e}")
        finally:
            shutil.rmtree(workdir, ignore_errors=True)

        log.info(f"[connectors] {report.summary()} elapsed={time.monotonic() - started:.1f}s")
        for error in report.errors:
            log.warning(f"[connectors] job {job.job_id}: {error}")
        return report

    # -- phase 1 ---------------------------------------------------------------
    async def _collect(
        self,
        connector_cls: Type[SourceConnector],
        credential: SourceCredential,
        job: IngestionJob,
        index: PointIndex,
        workdir: str,
        report: JobReport,
    ) -> List[_Downloaded]:
        settings = self._settings
        options = build_connector_options(settings, self._accepted)
        downloaded: List[_Downloaded] = []
        seen: Set[str] = set()
        total_bytes = 0

        async with connector_cls(credential, options, references=job.references) as connector:
            for reference in job.references:
                if report.truncated:
                    break
                try:
                    async for item in connector.iter_items(reference, recursive=job.recursive):
                        if item.item_id in seen:
                            continue
                        seen.add(item.item_id)
                        if (
                            report.discovered >= settings.max_items_scanned
                            or len(downloaded) >= settings.max_files_per_job
                        ):
                            report.truncated = True
                            break
                        report.discovered += 1

                        # the credential may reach far more than the references: never go beyond them
                        if not await connector.in_scope(item):
                            report.out_of_scope += 1
                            report.add_error(f"{item.path or item.item_id}: outside the requested references, skipped")
                            continue
                        if item.mime_type_known and item.mime_type not in self._accepted:
                            report.unsupported += 1
                            continue
                        if item.size is not None and (
                            item.size > settings.max_file_bytes or total_bytes + item.size > settings.max_total_bytes
                        ):
                            report.over_limit += 1
                            continue

                        identity = md.identity_filter(job.provider, item.item_id, job.owner, job.chat_id)
                        unchanged = await self._unchanged(index, identity, item, item.version, report)
                        if unchanged is not False:
                            continue

                        entry = await self._download(connector, item, job, workdir, report)
                        if entry is None:
                            continue
                        if entry.version != item.version:
                            # version known only after the download (e.g. pre-signed URLs)
                            unchanged = await self._unchanged(index, identity, item, entry.version, report)
                            if unchanged is not False:
                                _silent_remove(entry.path)
                                continue
                        if total_bytes + entry.result.bytes_written > settings.max_total_bytes:
                            os.remove(entry.path)
                            report.over_limit += 1
                            continue
                        total_bytes += entry.result.bytes_written
                        downloaded.append(entry)
                        report.downloaded += 1
                except CredentialError as e:
                    # fatal: stop enumerating, but still ingest what is already downloaded
                    report.aborted_reason = str(e)
                    break
                except ItemNotFoundError as e:
                    report.failed += 1
                    report.add_error(f"reference #{job.references.index(reference) + 1}: {e}")
                except ConnectorError as e:
                    report.failed += 1
                    report.add_error(f"reference #{job.references.index(reference) + 1}: {e}")

        return downloaded

    async def _unchanged(
        self, index: PointIndex, identity: Dict[str, Any], item: SourceItem, version: str, report: JobReport
    ) -> bool | None:
        """Whether the latest copy of the item in memory is at ``version``; None when the vector memory cannot tell.

        An unchanged item is also the moment to remove the copies a past job could not (see ``_latest_copy``).
        That cleanup is best effort: on error the item is still unchanged and the next job retries.
        An item whose latest copy is at another version is ingested again, even if ``version`` is in memory too.
        """
        try:
            points = await index.point_metadata(identity)
        except Exception as e:  # noqa: BLE001 - one item must not stop the job
            report.failed += 1
            report.add_error(f"{item.path or item.item_id}: {type(e).__name__}: {e}")
            return None
        latest = _latest_copy(points)
        if latest is None or points[next(iter(latest))].get(md.KEY_VERSION) != version:
            return False

        report.unchanged += 1
        try:
            await self._drop_copies(index, points, latest)
        except Exception as e:  # noqa: BLE001
            log.warning(f"[connectors] {item.path or item.item_id}: cleanup of the stale copies failed: {e}")
        return True

    async def _drop_copies(self, index: PointIndex, points: Dict[str, Dict[str, Any]], latest: Set[str]) -> None:
        """Remove every copy of an item but the latest one.

        A copy under the source name of the latest one is just points (the core replaces them at every ingestion of
        that source). A copy under another source name (the item was renamed, e.g. a Google document) is a source of
        its own in the core, with its ingestion state and its kept chunks, which a change of the embedder or of the
        chunker would bring back: it is deleted through the core, with its stored file.
        """
        latest_source = points[next(iter(latest))].get("source")
        stale = set(points) - latest
        for source in sorted({points[pid].get("source") for pid in stale} - {latest_source, None}):
            try:
                await self._delete_source(index, source, {pid for pid in stale if points[pid].get("source") == source})
            except _SourceChanged:
                pass  # a concurrent job wrote a newer copy under that name meanwhile: it is not stale anymore
        await index.delete_ids({pid for pid in stale if points[pid].get("source") in (latest_source, None)})

    async def _delete_source(self, index: PointIndex, source: str, stale: Set[str]) -> None:
        """Delete a source as the file manager routes of the core do: holding its lock (``SourceBusy`` while it is
        ingested: the next job retries), its stored file first and the plugins notified, then what its ingestion
        produced and its state. The deletion goes by source, not by point: holding the lock, no ingestion of the source
        can interleave, and the source must still hold only the ``stale`` points seen before (``_SourceChanged``
        otherwise: a concurrent job wrote a newer copy under that name, which must stay)."""
        ccat = self._ccat
        chat_id = index.chat_id
        folder = owner_folder(ccat.agent_key, chat_id)
        scope = chat_id or "agent"

        async def remove_file():
            # the source of this scope only: the same source name may live in another chat (the core filters the same)
            scoped = {"source": source} | ({"chat_id": chat_id} if chat_id else {})
            if set(await index.point_metadata(scoped)) - stale:
                raise _SourceChanged(source)
            await asyncio.to_thread(ccat.file_manager.remove_file, os.path.join(folder, source))
            await ccat.plugin_manager.execute_hook("before_file_manager_file_delete", source, scope, caller=ccat)

        await ccat.lizard.rabbit_hole.delete_source(ccat, source, chat_id, wait=DELETE_WAIT_SECONDS, before=remove_file)
        await ccat.plugin_manager.execute_hook("after_file_manager_file_deleted", source, scope, caller=ccat)

    async def _download(
        self, connector: SourceConnector, item: SourceItem, job: IngestionJob, workdir: str, report: JobReport
    ) -> _Downloaded | None:
        path = os.path.join(workdir, f"{next(self._file_counter)}.bin")
        try:
            with Path(path).open("wb") as fh:
                result = await connector.download(item, fh)
        except (UnsupportedItemError, ItemTooLargeError, ItemNotFoundError) as e:
            if isinstance(e, ItemTooLargeError):
                report.over_limit += 1
            elif isinstance(e, UnsupportedItemError):
                report.unsupported += 1
            else:
                report.failed += 1
            report.add_error(f"{item.path or item.item_id}: {e}")
            _silent_remove(path)
            return None
        except CredentialError:
            _silent_remove(path)
            raise
        except ConnectorError as e:
            report.failed += 1
            report.add_error(f"{item.path or item.item_id}: {e}")
            _silent_remove(path)
            return None

        if result.mime_type not in self._accepted or result.bytes_written == 0:
            report.unsupported += 1
            _silent_remove(path)
            return None

        name = md.source_name(item.name, result.extension, job.provider, item.item_id, job.owner)
        # download hints may hold secrets (e.g. pre-signed URLs): phase 2 must not keep them
        item = dataclasses.replace(item, download_hints={})
        return _Downloaded(item=item, path=path, result=result, source_name=name)

    # -- phase 2 ---------------------------------------------------------------
    async def _ingest(self, downloaded: List[_Downloaded], job: IngestionJob, index: PointIndex, report: JobReport):
        # the lizard drops the RabbitHole on shutdown: fail the whole phase before touching any item
        ingest_file = self._ccat.lizard.rabbit_hole.ingest_file

        for entry in downloaded:
            item = entry.item
            identity = md.identity_filter(job.provider, item.item_id, job.owner, job.chat_id)
            try:
                metadata = md.build_metadata(
                    provider=job.provider,
                    item_id=item.item_id,
                    version=entry.version,
                    owner=job.owner,
                    visibility=job.visibility,
                    job_id=job.job_id,
                    path=item.path,
                    web_url=item.web_url,
                    item_extra=item.extra_metadata,
                    user_metadata=job.user_metadata,
                )
                content = BytesIO(Path(entry.path).read_bytes())

                await ingest_file(
                    cat=self._target,
                    file=content,
                    filename=entry.source_name,
                    metadata=metadata,
                    store_file=self._settings.store_files,
                    content_type=entry.result.mime_type,
                )

                # the RabbitHole swallows errors: verify the new version landed
                points = await index.point_metadata(identity)
                landed = any(_job_of(meta) == job.job_id for meta in points.values())
                in_memory = any(meta.get(md.KEY_VERSION) == entry.version for meta in points.values())

                # one copy per item: the latest one, which may come from a concurrent job
                latest = _latest_copy(points)
                if latest is not None:
                    await self._drop_copies(index, points, latest)

                if landed:
                    report.ingested += 1
                elif in_memory:
                    report.unchanged += 1  # a concurrent job ingested the same version (and may have replaced ours)
                else:
                    report.failed += 1
                    report.add_error(f"{item.path or item.item_id}: ingestion produced no chunks")
            except Exception as e:  # noqa: BLE001 - one bad file must not stop the job
                report.failed += 1
                report.add_error(f"{item.path or item.item_id}: {type(e).__name__}: {e}")
            finally:
                _silent_remove(entry.path)


class _SourceChanged(Exception):
    """A source about to be deleted got a newer copy meanwhile."""


def _job_of(meta: Dict[str, Any]) -> str:
    return str(meta.get(md.KEY_JOB) or "")


def _latest_copy(points: Dict[str, Dict[str, Any]]) -> Set[str] | None:
    """The point ids of the latest copy of one item, None when there is no point.

    A copy is what one job wrote for the item: the RabbitHole stores it in a single upsert, so every reader sees
    it whole. Copies are ordered by the newest ``when`` of their points, then by job id. Every job keeps the
    latest copy it sees and deletes the others. No reader can see a copy later than the latest one in memory,
    so that copy is never deleted and the item never stays empty, whatever the interleaving of the jobs.
    The price: a late job can make an older version the latest copy; the next job finds the item changed
    and ingests the current version again.
    """
    copies: Dict[str, Tuple[float, Set[str]]] = {}
    for pid, meta in points.items():
        job_id = _job_of(meta)
        newest, ids = copies.get(job_id, (float("-inf"), set()))
        ids.add(pid)
        copies[job_id] = (max(newest, _when(meta)), ids)
    if not copies:
        return None
    return max(copies.items(), key=lambda copy: (copy[1][0], copy[0]))[1][1]


def _when(meta: Dict[str, Any]) -> float:
    # the RabbitHole stamps every chunk with the time.time() of its ingestion
    return float(meta.get("when") or 0)


def _silent_remove(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass
