"""Pre-signed URL connector (S3 pre-signed URLs, Azure Blob SAS URLs, ...).

The backend signs one read-only, short-lived URL per object: no credential ever
reaches the Cat. The URLs themselves are bearer secrets, so they are handled like
credentials: never logged, never stored, dropped after the download.

Allowed hosts come from the plugin settings (SSRF protection) and redirects are
not followed.
"""
from __future__ import annotations

import mimetypes
import posixpath
from typing import AsyncIterator, BinaryIO, ClassVar, Dict, Sequence
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import httpx

from .base import (
    ConnectorOptions,
    DownloadResult,
    HttpSourceConnector,
    ItemNotFoundError,
    SourceCredential,
    SourceItem,
    resolve_mime_type,
)


class PresignedUrlCredential(SourceCredential):
    """No secret here: the signature travels inside each URL."""


class PresignedUrlConnector(HttpSourceConnector[PresignedUrlCredential]):
    provider: ClassVar[str] = "presigned_url"
    display_name: ClassVar[str] = "Pre-signed URLs (S3, Azure SAS)"
    credential_model = PresignedUrlCredential
    reference_help: ClassVar[str] = "A read-only, short-lived pre-signed URL of a single object (treated as a secret)"

    @classmethod
    def validate_request(
        cls, credential: SourceCredential, references: Sequence[str], options: ConnectorOptions
    ) -> None:
        for reference in references:
            cls.check_url(reference, options, "pre-signed URL")

    @staticmethod
    def _identity(url: str) -> tuple[str, str]:
        """Stable, non-secret identity of the object: scheme, host and path, without the query."""
        parts = urlsplit(url)
        host = (parts.hostname or "").lower()
        path = parts.path or "/"
        return f"{parts.scheme}://{host}{path}", f"{host}{unquote(path)}"

    async def iter_items(self, reference: str, recursive: bool = True) -> AsyncIterator[SourceItem]:
        self.check_url(reference, self.options, "pre-signed URL")
        item_id, display_path = self._identity(reference)
        name = posixpath.basename(unquote(urlsplit(reference).path)) or "download"
        mime_type = resolve_mime_type(None, name, self.options.accepted_mime_types)
        yield SourceItem(
            provider=self.provider,
            item_id=item_id,
            name=name,
            mime_type=mime_type,
            mime_type_known=mime_type in self.options.accepted_mime_types,
            # the real version (ETag or content hash) is known only after the download
            version=f"pending:{uuid4().hex}",
            path=display_path,
            download_hints={"url": reference},
        )

    def _accepted_or_guess(self, content_type: str | None, name: str) -> str:
        return resolve_mime_type(content_type, name, self.options.accepted_mime_types)

    async def download(self, item: SourceItem, destination: BinaryIO) -> DownloadResult:
        info = await self._stream_to(item.download_hints["url"], None, destination, "downloading a pre-signed URL")
        mime_type = self._accepted_or_guess(info.content_type, item.name)
        extension = None if posixpath.splitext(item.name)[1] else mimetypes.guess_extension(mime_type)
        return DownloadResult(
            bytes_written=info.bytes_written,
            mime_type=mime_type,
            extension=extension,
            version=f"etag:{info.etag}" if info.etag else f"sha256:{info.sha256}",
        )

    def _auth_headers(self, method: str, url: str, params: Dict[str, str]) -> Dict[str, str]:
        return {}

    def _raise_for_status(self, response: httpx.Response, what: str) -> None:
        # each URL carries its own signature: a rejected one affects only that object
        if response.status_code in (401, 403):
            raise ItemNotFoundError(f"Signature rejected or expired ({response.status_code}) while {what}")
        super()._raise_for_status(response, what)
