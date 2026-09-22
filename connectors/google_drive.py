"""Google Drive connector, authenticated with a user's OAuth access token.

The backend obtains the token (Authorization Code flow) and passes the short-lived
access token. It may also pass the refresh token: the connector then renews the
access token when it is about to expire or when Drive answers 401, using the OAuth
client configured in the plugin settings. Required scopes: ``drive.readonly``
(restricted) or ``drive.file`` (only files picked by the user).

It talks to the Drive REST API v3 directly with httpx: fully async, streaming,
no blocking client library.
"""
from __future__ import annotations

import re
from typing import Any, AsyncIterator, BinaryIO, ClassVar, Dict, List, Sequence, Set, Tuple

import httpx
from pydantic import SecretStr

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
    UnsupportedItemError,
    resolve_mime_type,
)

API_BASE = "https://www.googleapis.com/drive/v3"
TOKEN_URL = "https://oauth2.googleapis.com/token"

FOLDER_MIME = "application/vnd.google-apps.folder"
SHORTCUT_MIME = "application/vnd.google-apps.shortcut"
GOOGLE_APPS_PREFIX = "application/vnd.google-apps."

FILE_FIELDS = (
    "id,name,mimeType,parents,size,modifiedTime,md5Checksum,version,webViewLink,trashed,"
    "shortcutDetails(targetId,targetMimeType),capabilities(canDownload)"
)

_ID_RE = re.compile(r"^[A-Za-z0-9_-]{10,}$")
_URL_ID_PATTERNS = (
    re.compile(r"/(?:file|document|spreadsheets|presentation|drawings)/d/([A-Za-z0-9_-]{10,})"),
    re.compile(r"/folders/([A-Za-z0-9_-]{10,})"),
    re.compile(r"[?&]id=([A-Za-z0-9_-]{10,})"),
)

# 403 reasons that concern the single item, not the credential
_ITEM_LEVEL_403 = {"cannotDownloadFile", "fileNotDownloadable", "exportSizeLimitExceeded", "cannotExportFile"}
_RATE_LIMIT_403 = {"rateLimitExceeded", "userRateLimitExceeded", "sharingRateLimitExceeded"}


class GoogleDriveCredential(SourceCredential):
    access_token: SecretStr
    #: optional: lets the connector renew the access token (needs the OAuth client in the settings)
    refresh_token: SecretStr | None = None

    @property
    def refreshable(self) -> bool:
        return self.refresh_token is not None


class GoogleDriveConnector(HttpSourceConnector[GoogleDriveCredential]):
    provider: ClassVar[str] = "google_drive"
    display_name: ClassVar[str] = "Google Drive"
    credential_model = GoogleDriveCredential
    reference_help: ClassVar[str] = "A Drive file or folder ID, or its Drive/Docs URL"

    #: export formats for native Google files, in order of preference
    EXPORT_FORMATS: ClassVar[Dict[str, List[Tuple[str, str]]]] = {
        "application/vnd.google-apps.document": [("text/markdown", ".md"), ("application/pdf", ".pdf"), ("text/plain", ".txt")],
        # CSV exports only the first sheet: PDF keeps all sheets but parses worse
        "application/vnd.google-apps.spreadsheet": [("text/csv", ".csv"), ("application/pdf", ".pdf")],
        "application/vnd.google-apps.presentation": [("application/pdf", ".pdf"), ("text/plain", ".txt")],
        "application/vnd.google-apps.drawing": [("application/pdf", ".pdf")],
    }

    # -- credential / errors ---------------------------------------------------
    # Drive answers downloads with redirects to googleusercontent.com
    FOLLOW_REDIRECTS: ClassVar[bool] = True

    def __init__(
        self, credential: GoogleDriveCredential, options: ConnectorOptions, references: Sequence[str] = ()
    ):
        super().__init__(credential, options, references)
        #: ids whose whole subtree is in scope: the references, plus the targets of referenced shortcuts
        self._scope_roots: Set[str] = set()
        for reference in self._references:
            try:
                self._scope_roots.add(self.parse_reference(reference))
            except ItemNotFoundError:
                continue
        #: folders reached from a scope root: their children are in scope without further checks
        self._inside: Set[str] = set()
        self._parents_cache: Dict[str, List[str]] = {}

    async def _obtain_refreshed_credential(self) -> GoogleDriveCredential:
        credential = self._credential
        client = self.options.oauth_clients.get(self.provider)
        if client is None or credential.refresh_token is None:
            raise CredentialError("The Google Drive credential cannot be refreshed")
        token = await self._refresh_token_grant(TOKEN_URL, credential.refresh_token, client)
        return credential.model_copy(update=token.credential_update("access_token"))

    def _auth_headers(self, method: str, url: str, params: Dict[str, Any]) -> Dict[str, str]:
        return {"Authorization": f"Bearer {self._credential.access_token.get_secret_value()}"}

    def _raise_for_status(self, response: httpx.Response, what: str) -> None:
        if response.status_code == 403:
            reasons = self._error_reasons(response)
            if reasons & _RATE_LIMIT_403:
                raise RetryableError(f"Drive rate limit while {what}")
            if reasons & _ITEM_LEVEL_403:
                raise UnsupportedItemError(f"Drive refused the download ({', '.join(sorted(reasons))}) while {what}")
            if "insufficientFilePermissions" in reasons:
                raise ItemNotFoundError(f"No access to the item while {what}")
        if response.status_code == 400 and response.request.url.path.endswith("/export"):
            # e.g. "The requested conversion is not supported"
            raise UnsupportedItemError(f"Drive rejected the export format while {what}")
        super()._raise_for_status(response, what)

    @staticmethod
    def _error_reasons(response: httpx.Response) -> Set[str]:
        try:
            errors = response.json().get("error", {}).get("errors", [])
        except (ValueError, AttributeError):
            return set()
        return {e.get("reason", "") for e in errors if isinstance(e, dict)}

    # -- references ------------------------------------------------------------
    @staticmethod
    def parse_reference(reference: str) -> str:
        reference = reference.strip()
        if _ID_RE.match(reference):
            return reference
        for pattern in _URL_ID_PATTERNS:
            if match := pattern.search(reference):
                return match.group(1)
        raise ItemNotFoundError("Invalid Google Drive reference: expected a file/folder ID or URL")

    # -- enumeration -----------------------------------------------------------
    async def _get_file(self, file_id: str) -> Dict[str, Any]:
        return await self._get_json(
            f"{API_BASE}/files/{file_id}",
            {"fields": FILE_FIELDS, "supportsAllDrives": "true"},
            "reading Drive file metadata",
        )

    async def _list_children(self, folder_id: str) -> AsyncIterator[Dict[str, Any]]:
        page_token: str | None = None
        while True:
            params = {
                "q": f"'{folder_id}' in parents and trashed = false",
                "fields": f"nextPageToken,files({FILE_FIELDS})",
                "pageSize": 1000,
                "supportsAllDrives": "true",
                "includeItemsFromAllDrives": "true",
                # "user" (the default) may miss items of shared drives
                "corpora": "allDrives",
            }
            if page_token:
                params["pageToken"] = page_token
            page = await self._get_json(f"{API_BASE}/files", params, "listing a Drive folder")
            for child in page.get("files", []):
                yield child
            page_token = page.get("nextPageToken")
            if not page_token:
                return

    async def iter_items(self, reference: str, recursive: bool = True) -> AsyncIterator[SourceItem]:
        root_id = self.parse_reference(reference)
        if root_id not in self._scope_roots:
            raise ConnectorError("The reference is not among the job references")
        root = await self._get_file(root_id)
        visited: Set[str] = set()
        async for item in self._walk(root, path="", depth=0, recursive=recursive, visited=visited):
            yield item

    async def _walk(
        self, meta: Dict[str, Any], path: str, depth: int, recursive: bool, visited: Set[str]
    ) -> AsyncIterator[SourceItem]:
        if meta.get("trashed") or meta["id"] in visited:
            return
        visited.add(meta["id"])

        if meta["mimeType"] == SHORTCUT_MIME:
            target_id = (meta.get("shortcutDetails") or {}).get("targetId")
            if not target_id or target_id in visited:
                return
            try:
                target = await self._get_file(target_id)
            except ItemNotFoundError:
                return
            if depth == 0 and meta["id"] in self._scope_roots:
                # a shortcut given as reference: its target is what was requested
                self._scope_roots.add(target["id"])
            elif target["mimeType"] == FOLDER_MIME and not await self._within_scope(target):
                # never list a folder outside the references; files are rejected by in_scope
                return
            async for item in self._walk(target, path, depth, recursive, visited):
                yield item
            return

        current_path = f"{path}/{meta['name']}" if path else meta["name"]

        if meta["mimeType"] == FOLDER_MIME:
            # the root folder is always expanded; subfolders only when recursive
            if depth > 0 and not recursive:
                return
            if depth >= self.options.max_depth:
                return
            # reached from a scope root (directly, or through a shortcut checked above)
            self._inside.add(meta["id"])
            async for child in self._list_children(meta["id"]):
                async for item in self._walk(child, current_path, depth + 1, recursive, visited):
                    yield item
            return

        yield self._to_item(meta, current_path)

    # -- scope -----------------------------------------------------------------
    async def in_scope(self, item: SourceItem) -> bool:
        return await self._within_scope({"id": item.item_id, "parents": item.download_hints.get("parents")})

    async def _within_scope(self, meta: Dict[str, Any]) -> bool:
        """Whether the file is a scope root or descends from one (walking up its parents)."""
        known_parents = {meta["id"]: meta.get("parents")}
        frontier = [meta["id"]]
        seen: Set[str] = set()
        for _ in range(self.options.max_depth + 1):
            upper: List[str] = []
            for file_id in frontier:
                if file_id in self._scope_roots or file_id in self._inside:
                    return True
                if file_id in seen:
                    continue
                seen.add(file_id)
                parents = known_parents.get(file_id)
                upper.extend(parents if parents is not None else await self._parents(file_id))
            if not upper:
                return False
            frontier = upper
        return False

    async def _parents(self, file_id: str) -> List[str]:
        if file_id not in self._parents_cache:
            try:
                meta = await self._get_json(
                    f"{API_BASE}/files/{file_id}",
                    {"fields": "parents", "supportsAllDrives": "true"},
                    "reading Drive file parents",
                )
                self._parents_cache[file_id] = list(meta.get("parents") or [])
            except ItemNotFoundError:
                self._parents_cache[file_id] = []
        return self._parents_cache[file_id]

    def _to_item(self, meta: Dict[str, Any], path: str) -> SourceItem:
        mime = meta["mimeType"]
        hints: Dict[str, Any] = {"parents": list(meta.get("parents") or [])}
        name = meta["name"]

        if mime.startswith(GOOGLE_APPS_PREFIX):
            candidates = [c for c in self.EXPORT_FORMATS.get(mime, []) if c[0] in self.options.accepted_mime_types]
            hints["export_candidates"] = candidates
            # the listed type is the preferred export; unsupported natives keep their own type
            delivered_mime, extension = candidates[0] if candidates else (mime, "")
            name = f"{name}{extension}"
            # native files have no md5: the revision number changes on every edit
            version = f"v{meta.get('version', '')}-{meta.get('modifiedTime', '')}"
        else:
            # Drive often reports generic types (e.g. octet-stream for .md): trust the extension
            delivered_mime = resolve_mime_type(mime, name, self.options.accepted_mime_types)
            version = meta.get("md5Checksum") or f"v{meta.get('version', '')}-{meta.get('modifiedTime', '')}"
            if (meta.get("capabilities") or {}).get("canDownload") is False:
                hints["not_downloadable"] = True

        size = meta.get("size")
        return SourceItem(
            provider=self.provider,
            item_id=meta["id"],
            name=name,
            mime_type=delivered_mime,
            version=version,
            size=int(size) if size is not None and str(size).isdigit() else None,
            path=path,
            web_url=meta.get("webViewLink"),
            extra_metadata={"drive_mime_type": mime, "drive_modified_time": meta.get("modifiedTime")},
            download_hints=hints,
        )

    # -- download --------------------------------------------------------------
    async def download(self, item: SourceItem, destination: BinaryIO) -> DownloadResult:
        if item.download_hints.get("not_downloadable"):
            raise UnsupportedItemError("The owner disabled downloads for this file")

        if "export_candidates" not in item.download_hints:
            info = await self._stream_to(
                f"{API_BASE}/files/{item.item_id}",
                {"alt": "media", "supportsAllDrives": "true"},
                destination,
                "downloading a Drive file",
            )
            return DownloadResult(info.bytes_written, item.mime_type, None)

        candidates: List[Tuple[str, str]] = list(item.download_hints["export_candidates"])
        if not candidates:
            raise UnsupportedItemError("No accepted export format for this Google file")

        last_error: ConnectorError | None = None
        for export_mime, extension in candidates:
            try:
                info = await self._stream_to(
                    f"{API_BASE}/files/{item.item_id}/export",
                    {"mimeType": export_mime},
                    destination,
                    "exporting a Google file",
                )
                return DownloadResult(info.bytes_written, export_mime, extension)
            except UnsupportedItemError as e:
                # e.g. format not available or export too large: try the next format
                last_error = e
            except CredentialError:
                raise
        raise last_error or UnsupportedItemError("Export failed")
