"""Amazon S3 (and S3-compatible) connector, authenticated with temporary STS credentials.

The backend calls STS (``AssumeRole`` / ``AssumeRoleWithWebIdentity``) with a
session policy limited to ``s3:GetObject`` and ``s3:ListBucket`` on the user's
prefix, and a short duration. Only the resulting triple reaches the Cat.

Requests are signed with SigV4 (``aws_sigv4``) and sent with httpx: no boto
dependency, fully async.
"""
from __future__ import annotations

import posixpath
import re
import xml.etree.ElementTree as ET
from typing import Any, AsyncIterator, BinaryIO, ClassVar, Dict, Sequence, Tuple
from urllib.parse import quote, urlsplit

import httpx
from pydantic import SecretStr, field_validator

from .aws_sigv4 import sign_get
from .base import (
    ConnectorError,
    ConnectorOptions,
    CredentialError,
    DownloadResult,
    HttpSourceConnector,
    ItemNotFoundError,
    RetryableError,
    SourceCredential,
    SourceItem,
    has_dot_segments,
    resolve_mime_type,
    within_prefix,
)

NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"

_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]$")
_REGION_RE = re.compile(r"^[a-z0-9\-]{2,32}$")

_CREDENTIAL_CODES = {
    "InvalidAccessKeyId", "SignatureDoesNotMatch", "ExpiredToken", "InvalidToken",
    "TokenRefreshRequired", "AccountProblem", "AllAccessDisabled",
}
_RETRY_CODES = {"SlowDown", "InternalError", "ServiceUnavailable", "RequestTimeout"}
_NOT_FOUND_CODES = {"NoSuchBucket", "NoSuchKey"}


class S3Credential(SourceCredential):
    access_key_id: SecretStr
    secret_access_key: SecretStr
    session_token: SecretStr | None = None
    region: str
    #: S3-compatible services (MinIO, R2, ...): must match the allowed hosts
    endpoint_url: str | None = None
    force_path_style: bool = False

    @field_validator("region")
    @classmethod
    def _check_region(cls, value: str) -> str:
        if not _REGION_RE.match(value):
            raise ValueError("invalid region")
        return value


def parse_s3_reference(reference: str) -> Tuple[str, str]:
    """``s3://bucket/key-or-prefix`` -> (bucket, key)."""
    parts = urlsplit(reference.strip())
    if parts.scheme != "s3" or not parts.netloc or parts.query or parts.fragment:
        raise ItemNotFoundError("Invalid S3 reference: expected s3://bucket/key-or-prefix")
    bucket, key = parts.netloc, parts.path.lstrip("/")
    if not _BUCKET_RE.match(bucket) or ".." in bucket:
        raise ItemNotFoundError("Invalid S3 bucket name")
    if has_dot_segments(key):
        raise ItemNotFoundError("S3 references with '.' or '..' path segments are not supported")
    return bucket, key


class S3Connector(HttpSourceConnector[S3Credential]):
    provider: ClassVar[str] = "s3"
    display_name: ClassVar[str] = "Amazon S3"
    credential_model = S3Credential
    reference_help: ClassVar[str] = "s3://bucket/key for an object, s3://bucket/prefix/ for a folder"

    PAGE_SIZE: ClassVar[int] = 1000

    @classmethod
    def validate_request(
        cls, credential: SourceCredential, references: Sequence[str], options: ConnectorOptions
    ) -> None:
        endpoint = getattr(credential, "endpoint_url", None)
        if endpoint:
            cls.check_url(endpoint, options, "S3 endpoint URL")
            if urlsplit(endpoint).path.strip("/"):
                raise ConnectorError("The S3 endpoint URL must not contain a path")
        for reference in references:
            parse_s3_reference(reference)

    # -- addressing ------------------------------------------------------------
    def _endpoint(self) -> Tuple[str, str]:
        cred = self._credential
        if cred.endpoint_url:
            parts = urlsplit(cred.endpoint_url)
            return parts.scheme, parts.netloc
        return "https", f"s3.{cred.region}.amazonaws.com"

    def _bucket_url(self, bucket: str) -> str:
        scheme, host = self._endpoint()
        # dotted bucket names break TLS with virtual-hosted style
        if self._credential.force_path_style or "." in bucket:
            return f"{scheme}://{host}/{bucket}"
        return f"{scheme}://{bucket}.{host}"

    def _object_url(self, bucket: str, key: str) -> str:
        return f"{self._bucket_url(bucket)}/{quote(key, safe='/-_.~')}"

    def _item_id(self, bucket: str, key: str) -> str:
        cred = self._credential
        host = urlsplit(cred.endpoint_url).netloc if cred.endpoint_url else "aws"
        return f"{host}/{bucket}/{key}"

    # -- auth / errors ---------------------------------------------------------
    def _auth_headers(self, method: str, url: str, params: Dict[str, str]) -> Dict[str, str]:
        cred = self._credential
        return sign_get(
            url=url,
            params=params,
            region=cred.region,
            access_key_id=cred.access_key_id.get_secret_value(),
            secret_access_key=cred.secret_access_key.get_secret_value(),
            session_token=cred.session_token.get_secret_value() if cred.session_token else None,
        )

    @staticmethod
    def _error_code(response: httpx.Response) -> str:
        try:
            root = ET.fromstring(response.content)
        except ET.ParseError:
            return ""
        return (root.findtext("Code") or "").strip()

    def _raise_for_status(self, response: httpx.Response, what: str) -> None:
        if response.is_success:
            return
        code = self._error_code(response)
        if code in _CREDENTIAL_CODES:
            raise CredentialError(f"S3 rejected the credential ({code}) while {what}")
        if code in _RETRY_CODES:
            raise RetryableError(f"S3 {code} while {what}")
        if code in _NOT_FOUND_CODES:
            raise ItemNotFoundError(f"S3 {code} while {what}")
        if code == "AccessDenied":
            # the session policy does not cover this prefix/object: other references may still work
            raise ItemNotFoundError(f"S3 AccessDenied while {what}")
        if code == "PermanentRedirect" or response.status_code == 301:
            raise ConnectorError(f"The bucket is in a different region than the credential while {what}")
        if code and response.status_code not in self.RETRYABLE_STATUSES and response.status_code not in (401, 403, 404):
            raise ConnectorError(f"S3 {code} ({response.status_code}) while {what}")
        super()._raise_for_status(response, what)

    # -- enumeration -----------------------------------------------------------
    async def _list(
        self, bucket: str, prefix: str, recursive: bool, page_size: int | None = None
    ) -> AsyncIterator[Dict[str, Any]]:
        token: str | None = None
        while True:
            params = {"list-type": "2", "prefix": prefix, "max-keys": str(page_size or self.PAGE_SIZE)}
            if not recursive:
                params["delimiter"] = "/"
            if token:
                params["continuation-token"] = token
            response = await self._get(f"{self._bucket_url(bucket)}/", params, "listing an S3 prefix")
            root = ET.fromstring(response.content)
            for node in root.iter(f"{NS}Contents"):
                yield {
                    "key": node.findtext(f"{NS}Key") or "",
                    "size": node.findtext(f"{NS}Size"),
                    "etag": (node.findtext(f"{NS}ETag") or "").strip('"'),
                    "last_modified": node.findtext(f"{NS}LastModified"),
                }
            if (root.findtext(f"{NS}IsTruncated") or "").lower() != "true":
                return
            token = root.findtext(f"{NS}NextContinuationToken")
            if not token:
                return

    async def iter_items(self, reference: str, recursive: bool = True) -> AsyncIterator[SourceItem]:
        bucket, key = parse_s3_reference(reference)

        if key and not key.endswith("/"):
            # an object, or a prefix written without the trailing slash. Keys are listed in
            # lexicographic order, so if the object exists it is the first entry.
            first: Dict[str, Any] | None = None
            async for entry in self._list(bucket, key, recursive=False, page_size=1):
                first = entry
                break
            if first is not None and first["key"] == key:
                yield self._to_item(bucket, first)
                return
            key = f"{key}/"

        found = False
        async for entry in self._list(bucket, key, recursive):
            if entry["key"].endswith("/"):
                continue  # "folder" placeholder objects
            found = True
            yield self._to_item(bucket, entry)
        if not found:
            raise ItemNotFoundError("No objects found for the S3 reference")

    def _to_item(self, bucket: str, entry: Dict[str, Any]) -> SourceItem:
        key = entry["key"]
        name = posixpath.basename(key)
        size = entry.get("size")
        return SourceItem(
            provider=self.provider,
            item_id=self._item_id(bucket, key),
            name=name,
            mime_type=resolve_mime_type(None, name, self.options.accepted_mime_types),
            version=entry["etag"] or f"lm:{entry.get('last_modified')}",
            size=int(size) if size and size.isdigit() else None,
            path=f"{bucket}/{key}",
            extra_metadata={"s3_bucket": bucket, "s3_key": key},
            download_hints={"bucket": bucket, "key": key},
        )

    async def in_scope(self, item: SourceItem) -> bool:
        bucket, key = item.download_hints["bucket"], item.download_hints["key"]
        for reference in self._references:
            try:
                ref_bucket, ref_key = parse_s3_reference(reference)
            except ItemNotFoundError:
                continue
            if ref_bucket == bucket and within_prefix(key, ref_key):
                return True
        return False

    # -- download --------------------------------------------------------------
    async def download(self, item: SourceItem, destination: BinaryIO) -> DownloadResult:
        url = self._object_url(item.download_hints["bucket"], item.download_hints["key"])
        info = await self._stream_to(url, None, destination, "downloading an S3 object")
        return DownloadResult(bytes_written=info.bytes_written, mime_type=item.mime_type)
