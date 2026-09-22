"""Azure Blob Storage connector.

Two kinds of short-lived credential, both produced by the backend:

- a **SAS token** scoped to the container (or a directory), permissions ``rl``
  (read + list), short expiry;
- an **Entra ID bearer token** for ``https://storage.azure.com/.default``, with the
  *Storage Blob Data Reader* role on the container. It may come with its refresh
  token: the connector then renews the bearer token when it is about to expire or
  when Azure answers 401, using the Entra ID app configured in the plugin settings.
  The Entra ID authority follows the cloud of ``account_url`` (public, US Government,
  China).
  A SAS cannot be refreshed (a new one needs the account key or a user delegation key).

Talks to the Blob REST API with httpx.
"""
from __future__ import annotations

import posixpath
import re
import xml.etree.ElementTree as ET
from typing import Any, AsyncIterator, BinaryIO, ClassVar, Dict, Sequence, Tuple
from urllib.parse import parse_qsl, quote, unquote, urlsplit

import httpx
from pydantic import SecretStr, field_validator, model_validator

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
    TokenRejectedError,
    has_dot_segments,
    resolve_mime_type,
    within_prefix,
)

API_VERSION = "2023-11-03"
ENTRA_TOKEN_URL = "https://{authority}/{tenant}/oauth2/v2.0/token"
#: blob endpoint suffix -> Entra ID authority host, one entry per Azure cloud
ENTRA_AUTHORITIES = {
    "blob.core.windows.net": "login.microsoftonline.com",
    "blob.core.usgovcloudapi.net": "login.microsoftonline.us",
    "blob.core.chinacloudapi.cn": "login.chinacloudapi.cn",
}
STORAGE_SCOPE = "https://storage.azure.com/.default offline_access"

_CONTAINER_RE = re.compile(r"^(?!.*--)[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")

_CREDENTIAL_CODES = {
    "AuthenticationFailed", "InvalidAuthenticationInfo", "NoAuthenticationInformation",
    "AuthorizationFailure", "AuthorizationPermissionMismatch", "AuthorizationResourceTypeMismatch",
    "AuthorizationServiceMismatch", "AuthorizationProtocolMismatch", "AuthorizationSourceIPMismatch",
    "AccountIsDisabled",
}
_RETRY_CODES = {"ServerBusy", "OperationTimedOut", "InternalError"}
_NOT_FOUND_CODES = {"ContainerNotFound", "BlobNotFound", "ResourceNotFound"}


class AzureBlobCredential(SourceCredential):
    #: e.g. https://myaccount.blob.core.windows.net (must match the allowed hosts)
    account_url: str
    sas_token: SecretStr | None = None
    bearer_token: SecretStr | None = None
    #: optional, only with bearer_token: lets the connector renew it (needs the Entra ID app in the settings)
    refresh_token: SecretStr | None = None

    @field_validator("account_url")
    @classmethod
    def _normalize_url(cls, value: str) -> str:
        parts = urlsplit(value.strip())
        if parts.query or parts.fragment or parts.path.strip("/"):
            raise ValueError("account_url must be scheme://host only (no path, no SAS)")
        return f"{parts.scheme}://{parts.netloc}"

    @model_validator(mode="after")
    def _one_auth(self):
        if (self.sas_token is None) == (self.bearer_token is None):
            raise ValueError("provide exactly one of sas_token or bearer_token")
        if self.refresh_token is not None and self.bearer_token is None:
            raise ValueError("refresh_token is supported only with bearer_token")
        return self

    @property
    def refreshable(self) -> bool:
        return self.refresh_token is not None


class AzureBlobConnector(HttpSourceConnector[AzureBlobCredential]):
    provider: ClassVar[str] = "azure_blob"
    display_name: ClassVar[str] = "Azure Blob Storage"
    credential_model = AzureBlobCredential
    reference_help: ClassVar[str] = (
        "container/blob for a blob, container/prefix/ for a folder, or the blob URL without SAS"
    )

    PAGE_SIZE: ClassVar[int] = 5000

    @classmethod
    def validate_request(
        cls, credential: SourceCredential, references: Sequence[str], options: ConnectorOptions
    ) -> None:
        account_url = credential.account_url  # type: ignore[attr-defined]
        cls.check_url(account_url, options, "Azure account URL")
        if credential.refreshable and cls.entra_authority(account_url) is None:
            raise ConnectorError(
                "refresh_token needs an account in a known Azure cloud (public, US Government, China): "
                "the Entra ID authority cannot be derived from this account URL"
            )
        for reference in references:
            cls.parse_reference(reference, account_url)

    @staticmethod
    def entra_authority(account_url: str) -> str | None:
        """Entra ID authority host of the cloud the account belongs to, None for unknown hosts."""
        host = (urlsplit(account_url).hostname or "").lower().rstrip(".")
        for suffix, authority in ENTRA_AUTHORITIES.items():
            if host.endswith(f".{suffix}"):
                return authority
        return None

    @staticmethod
    def parse_reference(reference: str, account_url: str) -> Tuple[str, str]:
        """-> (container, blob name or prefix)."""
        reference = reference.strip()
        if reference.startswith(("https://", "http://")):
            parts = urlsplit(reference)
            if f"{parts.scheme}://{parts.netloc}".lower() != account_url.lower():
                raise ItemNotFoundError("The blob URL does not belong to the credential's account")
            if parts.query:
                raise ItemNotFoundError("Pass blob URLs without the SAS: the token goes in the credential")
            reference = unquote(parts.path)
        container, _, name = reference.lstrip("/").partition("/")
        if not _CONTAINER_RE.match(container):
            raise ItemNotFoundError("Invalid Azure container name")
        if has_dot_segments(name):
            raise ItemNotFoundError("Azure references with '.' or '..' path segments are not supported")
        return container, name

    # -- auth / errors ---------------------------------------------------------
    async def _obtain_refreshed_credential(self) -> AzureBlobCredential:
        credential = self._credential
        client = self.options.oauth_clients.get(self.provider)
        authority = self.entra_authority(credential.account_url)
        if client is None or not client.tenant_id or authority is None or credential.refresh_token is None:
            raise CredentialError("The Azure credential cannot be refreshed")
        token = await self._refresh_token_grant(
            ENTRA_TOKEN_URL.format(authority=authority, tenant=quote(client.tenant_id, safe="")),
            credential.refresh_token,
            client,
            {"scope": STORAGE_SCOPE},
        )
        return credential.model_copy(update=token.credential_update("bearer_token"))

    def _auth_headers(self, method: str, url: str, params: Dict[str, str]) -> Dict[str, str]:
        headers = {"x-ms-version": API_VERSION}
        if self._credential.bearer_token is not None:
            headers["Authorization"] = f"Bearer {self._credential.bearer_token.get_secret_value()}"
        return headers

    def _auth_params(self) -> Dict[str, str]:
        if self._credential.sas_token is None:
            return {}
        sas = self._credential.sas_token.get_secret_value().lstrip("?")
        # a literal "+" in a SAS is part of the signature, not an encoded space
        return dict(parse_qsl(sas.replace("+", "%2B"), keep_blank_values=True))

    @staticmethod
    def _error_code(response: httpx.Response) -> str:
        code = response.headers.get("x-ms-error-code")
        if code:
            return code
        try:
            return (ET.fromstring(response.content).findtext("Code") or "").strip()
        except ET.ParseError:
            return ""

    def _raise_for_status(self, response: httpx.Response, what: str) -> None:
        if response.is_success:
            return
        code = self._error_code(response)
        if code in _CREDENTIAL_CODES:
            error = TokenRejectedError if response.status_code == 401 else CredentialError
            raise error(f"Azure rejected the credential ({code}) while {what}")
        if code in _RETRY_CODES:
            raise RetryableError(f"Azure {code} while {what}")
        if code in _NOT_FOUND_CODES:
            raise ItemNotFoundError(f"Azure {code} while {what}")
        if code and response.status_code not in self.RETRYABLE_STATUSES and response.status_code not in (401, 403, 404):
            raise ConnectorError(f"Azure {code} ({response.status_code}) while {what}")
        super()._raise_for_status(response, what)

    # -- addressing ------------------------------------------------------------
    def _container_url(self, container: str) -> str:
        return f"{self._credential.account_url}/{container}"

    def _blob_url(self, container: str, name: str) -> str:
        return f"{self._container_url(container)}/{quote(name, safe='/-_.~')}"

    # -- enumeration -----------------------------------------------------------
    async def _list(
        self, container: str, prefix: str, recursive: bool, page_size: int | None = None
    ) -> AsyncIterator[Dict[str, Any]]:
        marker: str | None = None
        while True:
            params = {
                "restype": "container",
                "comp": "list",
                "prefix": prefix,
                "maxresults": str(page_size or self.PAGE_SIZE),
            }
            if not recursive:
                params["delimiter"] = "/"
            if marker:
                params["marker"] = marker
            response = await self._get(self._container_url(container), params, "listing an Azure container")
            root = ET.fromstring(response.content)
            for node in root.iter("Blob"):
                props = node.find("Properties")
                if props is None:
                    props = ET.Element("Properties")
                get = props.findtext
                yield {
                    "name": node.findtext("Name") or "",
                    "size": get("Content-Length"),
                    "content_type": get("Content-Type"),
                    "etag": (get("Etag") or "").strip('"'),
                    "content_md5": get("Content-MD5"),
                    "last_modified": get("Last-Modified"),
                    "resource_type": get("ResourceType"),
                }
            marker = root.findtext("NextMarker")
            if not marker:
                return

    async def iter_items(self, reference: str, recursive: bool = True) -> AsyncIterator[SourceItem]:
        container, name = self.parse_reference(reference, self._credential.account_url)

        if name and not name.endswith("/"):
            first: Dict[str, Any] | None = None
            async for entry in self._list(container, name, recursive=False, page_size=1):
                first = entry
                break
            if first is not None and first["name"] == name:
                if item := self._to_item(container, first):
                    yield item
                    return
            name = f"{name}/"

        found = False
        async for entry in self._list(container, name, recursive):
            item = self._to_item(container, entry)
            if item is not None:
                found = True
                yield item
        if not found:
            raise ItemNotFoundError("No blobs found for the Azure reference")

    def _to_item(self, container: str, entry: Dict[str, Any]) -> SourceItem | None:
        name = entry["name"]
        size = entry.get("size")
        # hierarchical namespace directories and folder placeholders
        if entry.get("resource_type") == "directory" or name.endswith("/") or size == "0":
            return None
        filename = posixpath.basename(name)
        return SourceItem(
            provider=self.provider,
            item_id=f"{urlsplit(self._credential.account_url).netloc}/{container}/{name}",
            name=filename,
            mime_type=resolve_mime_type(entry.get("content_type"), filename, self.options.accepted_mime_types),
            version=entry.get("etag") or entry.get("content_md5") or f"lm:{entry.get('last_modified')}",
            size=int(size) if size and size.isdigit() else None,
            path=f"{container}/{name}",
            web_url=self._blob_url(container, name),
            extra_metadata={"azure_container": container, "azure_blob": name},
            download_hints={"container": container, "name": name},
        )

    async def in_scope(self, item: SourceItem) -> bool:
        container, name = item.download_hints["container"], item.download_hints["name"]
        for reference in self._references:
            try:
                ref_container, ref_name = self.parse_reference(reference, self._credential.account_url)
            except ItemNotFoundError:
                continue
            if ref_container == container and within_prefix(name, ref_name):
                return True
        return False

    # -- download --------------------------------------------------------------
    async def download(self, item: SourceItem, destination: BinaryIO) -> DownloadResult:
        url = self._blob_url(item.download_hints["container"], item.download_hints["name"])
        info = await self._stream_to(url, None, destination, "downloading an Azure blob")
        return DownloadResult(bytes_written=info.bytes_written, mime_type=item.mime_type)

