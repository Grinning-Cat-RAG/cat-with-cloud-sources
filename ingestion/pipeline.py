"""Two-phase ingestion of connector items.

Phase 1 (needs the credential): enumerate, skip unchanged items, download into a
private temporary directory. When it ends the connector is closed and the
credential reference is dropped.

Phase 2 (no credential): ingest every downloaded file through the configured
ingestion engine, then remove the chunks of the previous version of the item.
Old chunks are deleted only after the new version is confirmed in memory, so a
failed ingestion never loses the existing content.

Splitting the phases keeps the credential lifetime short: embedding can take
far longer than a user access token lives.
"""
from __future__ import annotations

import dataclasses
import itertools
import os
import shutil
import tempfile
import time
from dataclasses import dataclass, field
from io import BytesIO
from typing import Any, Dict, List, Set, Type

from cat import log
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
            f"downloaded={self.downloaded} ingested={self.ingested} failed={self.failed}"
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
        self._chat_id = chat_id
        self._collection = str(VectorMemoryType.EPISODIC if chat_id else VectorMemoryType.DECLARATIVE)

    async def point_ids(self, metadata_filter: Dict[str, Any]) -> Set[str]:
        ids: Set[str] = set()
        offset = None
        while True:
            records, offset = await self._handler.get_all_tenant_points(
                self._collection, limit=self.PAGE_SIZE, offset=offset, metadata=metadata_filter, with_vectors=False,
            )
            ids.update(str(r.id) for r in records)
            if not offset or not records:
                return ids

    async def has_points(self, metadata_filter: Dict[str, Any]) -> bool:
        records, _ = await self._handler.get_all_tenant_points(
            self._collection, limit=1, metadata=metadata_filter, with_vectors=False,
        )
        return bool(records)

    async def delete_ids(self, ids: Set[str]) -> None:
        if ids:
            await self._handler.delete_tenant_points_by_ids(self._collection, list(ids))


class ConnectorIngestionPipeline:
    def __init__(self, ccat, target, settings: ConnectorsSettings, accepted_mime_types: Set[str]):
        """
        Args:
            ccat: the CheshireCat of the agent (vector memory owner).
            target: the object passed to the ingestion engine: the StrayCat for chat-scoped
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

        async with connector_cls(credential, options) as connector:
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

                        if item.mime_type_known and item.mime_type not in self._accepted:
                            report.unsupported += 1
                            continue
                        if item.size is not None and (
                            item.size > settings.max_file_bytes or total_bytes + item.size > settings.max_total_bytes
                        ):
                            report.over_limit += 1
                            continue

                        identity = md.identity_filter(job.provider, item.item_id, job.owner, job.chat_id)
                        if await index.has_points({**identity, md.KEY_VERSION: item.version}):
                            report.unchanged += 1
                            continue

                        entry = await self._download(connector, item, job, workdir, report)
                        if entry is None:
                            continue
                        if entry.version != item.version and await index.has_points(
                            {**identity, md.KEY_VERSION: entry.version}
                        ):
                            # version known only after the download (e.g. pre-signed URLs)
                            _silent_remove(entry.path)
                            report.unchanged += 1
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

    async def _download(
        self, connector: SourceConnector, item: SourceItem, job: IngestionJob, workdir: str, report: JobReport
    ) -> _Downloaded | None:
        path = os.path.join(workdir, f"{next(self._file_counter)}.bin")
        try:
            with open(path, "wb") as fh:
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
        engine = await self._resolve_engine()

        for entry in downloaded:
            item = entry.item
            identity = md.identity_filter(job.provider, item.item_id, job.owner, job.chat_id)
            try:
                previous_ids = await index.point_ids(identity)
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
                with open(entry.path, "rb") as fh:
                    content = BytesIO(fh.read())

                await engine(
                    cat=self._target,
                    file=content,
                    filename=entry.source_name,
                    metadata=metadata,
                    store_file=self._settings.store_files,
                    content_type=entry.result.mime_type,
                )

                # the core engine swallows errors: verify the new version landed
                current_ids = await index.point_ids({**identity, md.KEY_VERSION: entry.version})
                if not current_ids:
                    report.failed += 1
                    report.add_error(f"{item.path or item.item_id}: ingestion produced no chunks")
                    continue

                await index.delete_ids(previous_ids - current_ids)
                report.ingested += 1
            except Exception as e:  # noqa: BLE001 - one bad file must not stop the job
                report.failed += 1
                report.add_error(f"{item.path or item.item_id}: {type(e).__name__}: {e}")
            finally:
                _silent_remove(entry.path)

    async def _resolve_engine(self):
        lizard = self._ccat.lizard
        try:
            from cat.services.factory.ingestion import resolve_ingestion_engine

            engine = await resolve_ingestion_engine(lizard)
        except Exception as e:  # noqa: BLE001
            log.warning(f"[connectors] ingestion engine not resolvable, using the RabbitHole: {e}")
            engine = None
        return engine.ingest_file if engine is not None else lizard.rabbit_hole.ingest_file


def _silent_remove(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass
