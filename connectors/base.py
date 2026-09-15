"""Provider-agnostic abstractions for source connectors.

A *source connector* knows how to enumerate and download items from an external
storage (Google Drive, S3, Azure Blob, ...) using a short-lived credential that
belongs to the end user. It knows nothing about the Grinning Cat: ingestion,
metadata and access control live in the ``ingestion`` package.

Layers:

- ``SourceCredential``: pydantic model for the credential. Secrets are ``SecretStr``
  and validation errors never echo the input, so tokens cannot leak via logs,
  reprs or 422 responses.
- ``SourceConnector``: the provider-neutral contract (``iter_items`` + ``download``).
- ``HttpSourceConnector``: shared plumbing for REST-based providers (async httpx
  client, bounded streaming, HTTP status -> connector error mapping).

Security contract for every implementation:
- never log, persist or return the credential;
- never put the credential into item metadata;
- raise ``CredentialError`` on 401/403 so the job can stop early.
"""
from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import logging
import mimetypes
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncIterator, BinaryIO, ClassVar, Dict, Generic, Iterable, Mapping, Sequence, Type, TypeVar
from urllib.parse import quote, urlsplit

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError


# ---------------------------------------------------------------------------
# Log redaction
# ---------------------------------------------------------------------------
class _RedactUrlQueryFilter(logging.Filter):
    """httpx logs every request URL at INFO level: query strings may hold secrets
    (SAS tokens, pre-signed signatures), so they are stripped from its records."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.args, tuple) and record.args:
            record.args = tuple(self._redact(a) for a in record.args)
        if isinstance(record.msg, str) and "?" in record.msg:
            record.msg = self._redact(record.msg)
        return True

    @staticmethod
    def _redact(value: Any) -> Any:
        if isinstance(value, httpx.URL):
            return value.copy_with(query=b"redacted") if value.query else value
        if isinstance(value, str) and "://" in value and "?" in value:
            head, _, tail = value.partition("?")
            rest = tail.split(" ", 1)
            return f"{head}?redacted" + (f" {rest[1]}" if len(rest) > 1 else "")
        return value


def install_log_redaction() -> None:
    for name in ("httpx", "httpcore"):
        logger = logging.getLogger(name)
        # modules are reloaded by the plugin manager: avoid stacking filters
        if not any(type(f).__name__ == _RedactUrlQueryFilter.__name__ for f in logger.filters):
            logger.addFilter(_RedactUrlQueryFilter())


install_log_redaction()


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------
class ConnectorError(Exception):
    """Base error for connectors. Messages must never contain secrets."""


class CredentialError(ConnectorError):
    """The credential is invalid, expired or lacks the required scope. Fatal for the job."""


class ItemNotFoundError(ConnectorError):
    """The referenced item does not exist or is not visible with this credential."""


class ItemTooLargeError(ConnectorError):
    """The item exceeds the configured size limit."""


class UnsupportedItemError(ConnectorError):
    """The item cannot be downloaded in an ingestible format."""


class RetryableError(ConnectorError):
    """Transient failure (rate limit, 5xx). Retried by ``HttpSourceConnector``."""


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------
class SourceCredential(BaseModel):
    """Base credential. Subclasses declare the provider-specific fields as ``SecretStr``."""

    model_config = ConfigDict(extra="forbid", hide_input_in_errors=True, frozen=True)

    expires_at: datetime | None = None

    def is_expired(self, margin: timedelta = timedelta(seconds=30)) -> bool:
        if self.expires_at is None:
            return False
        expires_at = self.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) + margin >= expires_at


CredentialT = TypeVar("CredentialT", bound=SourceCredential)


# ---------------------------------------------------------------------------
# Items
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SourceItem:
    """A downloadable leaf item (a file), described in a provider-neutral way.

    ``version`` must change whenever the content changes (etag, md5, revision, ...):
    it drives incremental re-ingestion. ``download_hints`` is private to the
    connector that produced the item (e.g. the export format of a Google Doc).
    """

    provider: str
    item_id: str
    name: str
    mime_type: str
    version: str
    size: int | None = None
    path: str | None = None
    web_url: str | None = None
    extra_metadata: Mapping[str, Any] = field(default_factory=dict)
    download_hints: Mapping[str, Any] = field(default_factory=dict)
    #: False when the format can be known only after the download (no pre-filtering)
    mime_type_known: bool = True


@dataclass(frozen=True)
class DownloadResult:
    """Outcome of a download: the actual format may differ from the listed one (export fallbacks)."""

    bytes_written: int
    mime_type: str
    extension: str | None = None
    #: set when the version is known only after the download (e.g. ETag of a pre-signed URL)
    version: str | None = None


@dataclass(frozen=True)
class ConnectorOptions:
    """Provider-neutral knobs passed by the ingestion layer.

    ``accepted_mime_types`` are the MIME types the Cat can parse: connectors that
    convert content (e.g. Google Docs export) pick a target format among them.
    """

    max_file_bytes: int
    accepted_mime_types: frozenset[str] = frozenset()
    request_timeout_seconds: float = 60.0
    max_depth: int = 20
    max_retries: int = 4
    #: hosts (or parent domains) that caller-provided URLs may point to
    allowed_host_suffixes: frozenset[str] = frozenset()
    allow_http: bool = False


def resolve_mime_type(declared: str | None, name: str, accepted: Iterable[str]) -> str:
    """Pick the MIME type to use for a file: the declared one if the Cat can parse it,
    otherwise the one guessed from the extension (storages often declare octet-stream)."""
    accepted = set(accepted)
    declared = (declared or "").split(";")[0].strip().lower()
    if declared in accepted:
        return declared
    guessed = mimetypes.guess_type(name)[0]
    if guessed in accepted:
        return guessed  # type: ignore[return-value]
    return declared or guessed or "application/octet-stream"


# ---------------------------------------------------------------------------
# Connector contract
# ---------------------------------------------------------------------------
class SourceConnector(ABC, Generic[CredentialT]):
    """Provider-neutral connector. Use it as an async context manager."""

    #: unique provider key used in the API (e.g. ``google_drive``)
    provider: ClassVar[str]
    #: human-readable name
    display_name: ClassVar[str]
    #: pydantic model used to validate the raw credential
    credential_model: ClassVar[Type[SourceCredential]]
    #: short description of what a "reference" is for this provider
    reference_help: ClassVar[str] = ""

    def __init__(self, credential: CredentialT, options: ConnectorOptions):
        self._credential = credential
        self.options = options

    @classmethod
    def parse_credential(cls, raw: Dict[str, Any]) -> CredentialT:
        """Validate a raw credential without ever echoing its content."""
        try:
            credential = cls.credential_model.model_validate(raw)
        except ValidationError as e:
            # pydantic messages never contain the input value; the input itself is never echoed
            problems = sorted({
                f"{'.'.join(str(p) for p in err['loc']) or 'credential'}: {err['msg']}" for err in e.errors()
            })
            raise CredentialError(f"Invalid credential for '{cls.provider}': {'; '.join(problems)}") from None
        if credential.is_expired():
            raise CredentialError(f"The credential for '{cls.provider}' is expired")
        return credential  # type: ignore[return-value]

    @classmethod
    def validate_request(
        cls, credential: SourceCredential, references: Sequence[str], options: "ConnectorOptions"
    ) -> None:
        """Cheap, offline checks run before the job is queued (raise ``ConnectorError``)."""

    @staticmethod
    def check_url(url: str, options: "ConnectorOptions", what: str = "URL") -> None:
        """Reject URLs that could turn the Cat into an SSRF proxy.

        The host must match ``options.allowed_host_suffixes``; IP literals and
        credentials in the URL are refused; plain HTTP only if explicitly allowed.
        """
        try:
            parts = urlsplit(url)
            host = (parts.hostname or "").lower().rstrip(".")
        except ValueError:
            raise ConnectorError(f"Invalid {what}") from None
        allowed_schemes = {"https", "http"} if options.allow_http else {"https"}
        if parts.scheme not in allowed_schemes:
            raise ConnectorError(f"The {what} must use {' or '.join(sorted(allowed_schemes))}")
        if not host or parts.username or parts.password:
            raise ConnectorError(f"Invalid {what}")
        try:
            ipaddress.ip_address(host)
            raise ConnectorError(f"IP addresses are not allowed in the {what}")
        except ValueError:
            pass
        if not any(host == suffix or host.endswith(f".{suffix}") for suffix in options.allowed_host_suffixes):
            raise ConnectorError(f"The host of the {what} is not allowed: {host}")

    def ensure_credential_valid(self) -> None:
        if self._credential.is_expired():
            raise CredentialError(f"The credential for '{self.provider}' expired during the job")

    async def __aenter__(self):
        await self.open()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def open(self) -> None:  # noqa: B027 - optional hook
        """Allocate resources (clients, sessions)."""

    async def close(self) -> None:
        """Release resources and drop the reference to the credential."""
        self._credential = None  # type: ignore[assignment]

    @abstractmethod
    def iter_items(self, reference: str, recursive: bool = True) -> AsyncIterator[SourceItem]:
        """Yield the downloadable items reachable from ``reference`` (a file, a folder, a prefix...).

        Containers are expanded (recursively if requested); only leaf items are yielded.
        Raises ``ItemNotFoundError`` if the reference does not exist.
        """

    @abstractmethod
    async def download(self, item: SourceItem, destination: BinaryIO) -> DownloadResult:
        """Write the content of ``item`` into ``destination``.

        Must enforce ``options.max_file_bytes`` while streaming (raise ``ItemTooLargeError``).
        On failure the caller discards ``destination``: partial writes are fine.
        """


@dataclass(frozen=True)
class StreamInfo:
    bytes_written: int
    content_type: str | None
    etag: str | None
    sha256: str


class HttpSourceConnector(SourceConnector[CredentialT], ABC):
    """Base class for connectors backed by an HTTP/REST API."""

    #: redirects are off by default: a redirect could lead outside the allowed hosts
    FOLLOW_REDIRECTS: ClassVar[bool] = False
    RETRYABLE_STATUSES: ClassVar[frozenset[int]] = frozenset({429, 500, 502, 503, 504})

    def __init__(self, credential: CredentialT, options: ConnectorOptions):
        super().__init__(credential, options)
        self._client: httpx.AsyncClient | None = None

    def _auth_headers(self, method: str, url: str, params: Dict[str, Any]) -> Dict[str, str]:
        """Headers carrying the credential, computed per request (signatures may depend on it)."""
        return {}

    def _auth_params(self) -> Dict[str, str]:
        """Query parameters carrying the credential (e.g. SAS tokens)."""
        return {}

    async def open(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.options.request_timeout_seconds),
            follow_redirects=self.FOLLOW_REDIRECTS,
        )

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None
        await super().close()

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Connector not opened: use it as an async context manager")
        return self._client

    def _raise_for_status(self, response: httpx.Response, what: str) -> None:
        """Map an HTTP error to a connector error. Providers may override to refine (e.g. 403 reasons).

        The response body has already been read when this is called.
        """
        if response.is_success:
            return
        status = response.status_code
        if status in self.RETRYABLE_STATUSES:
            raise RetryableError(f"HTTP {status} while {what}")
        if status in (401, 403):
            raise CredentialError(f"Access denied ({status}) while {what}")
        if status == 404:
            raise ItemNotFoundError(f"Not found while {what}")
        if 300 <= status < 400:
            raise ConnectorError(f"Unexpected redirect ({status}) while {what}")
        raise ConnectorError(f"HTTP {status} while {what}")

    async def _with_retries(self, operation, what: str):
        attempts = max(1, self.options.max_retries + 1)
        for attempt in range(attempts):
            self.ensure_credential_valid()
            try:
                return await operation()
            except httpx.HTTPError as e:
                error: ConnectorError = RetryableError(f"Network error while {what}: {type(e).__name__}")
            except RetryableError as e:
                error = e
            if attempt == attempts - 1:
                raise ConnectorError(str(error)) from None
            await asyncio.sleep(min(2 ** attempt + random.random(), 30))
        raise ConnectorError(f"Unreachable while {what}")  # pragma: no cover

    @staticmethod
    def encode_query(params: Mapping[str, str]) -> str:
        """RFC 3986 encoding (spaces as %20): what signature-based APIs expect."""
        return "&".join(f"{quote(str(k), safe='-_.~')}={quote(str(v), safe='-_.~')}" for k, v in params.items())

    def _prepare(self, method: str, url: str, params: Dict[str, Any] | None):
        """Build the final URL (query encoded here, never by httpx) and the auth headers.

        ``url`` must not carry a query string unless ``params`` is empty and the
        connector sends it verbatim (e.g. pre-signed URLs).
        """
        merged = {k: str(v) for k, v in (params or {}).items()}
        headers = self._auth_headers(method, url, merged)
        merged.update(self._auth_params())
        if not merged:
            return url, headers
        separator = "&" if urlsplit(url).query else "?"
        return f"{url}{separator}{self.encode_query(merged)}", headers

    async def _get(self, url: str, params: Dict[str, Any] | None, what: str) -> httpx.Response:
        async def operation():
            final_url, headers = self._prepare("GET", url, params)
            response = await self.client.get(final_url, headers=headers)
            self._raise_for_status(response, what)
            return response

        return await self._with_retries(operation, what)

    async def _get_json(self, url: str, params: Dict[str, Any] | None, what: str) -> Dict[str, Any]:
        return (await self._get(url, params, what)).json()

    async def _stream_to(
        self, url: str, params: Dict[str, Any] | None, destination: BinaryIO, what: str
    ) -> StreamInfo:
        """Stream a response body into ``destination`` enforcing the size limit.

        On retry the destination is rewound and truncated, so partial writes never survive.
        """
        limit = self.options.max_file_bytes
        start = destination.tell()

        async def operation():
            destination.seek(start)
            destination.truncate()
            written = 0
            digest = hashlib.sha256()
            final_url, headers = self._prepare("GET", url, params)
            async with self.client.stream("GET", final_url, headers=headers) as response:
                if not response.is_success:
                    await response.aread()
                    self._raise_for_status(response, what)
                declared = response.headers.get("Content-Length")
                if declared and declared.isdigit() and int(declared) > limit:
                    raise ItemTooLargeError(f"Item exceeds {limit} bytes while {what}")
                async for chunk in response.aiter_bytes():
                    written += len(chunk)
                    if written > limit:
                        raise ItemTooLargeError(f"Item exceeds {limit} bytes while {what}")
                    destination.write(chunk)
                    digest.update(chunk)
                return StreamInfo(
                    bytes_written=written,
                    content_type=response.headers.get("Content-Type"),
                    etag=(response.headers.get("ETag") or "").strip('"') or None,
                    sha256=digest.hexdigest(),
                )

        return await self._with_retries(operation, what)
