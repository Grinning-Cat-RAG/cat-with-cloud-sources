"""Plugin configuration model.

User credentials are per request and ephemeral. The only secrets here are the optional
OAuth client secrets, used to refresh Google Drive and Azure (Entra ID) access tokens:
their keys end with ``_secret``, so the core masks them towards readers without write
permission.
"""
from __future__ import annotations

from typing import Any, Dict, Literal

from pydantic import BaseModel, Field, SecretStr

from ..connectors.base import OAuthClient

Visibility = Literal["owner", "agent"]


class ConnectorsSettings(BaseModel):
    max_files_per_job: int = Field(default=500, ge=1, title="Max files per job")
    max_items_scanned: int = Field(
        default=20000, ge=1, title="Max items listed per job",
        description="Stops the enumeration of very large folders, buckets or containers.",
    )
    max_file_size_mb: int = Field(default=50, ge=1, title="Max size of a single file (MB)")
    max_total_size_mb: int = Field(default=1024, ge=1, title="Max total downloaded size per job (MB)")
    store_files: bool = Field(
        default=True,
        title="Store files in the Cat storage",
        description="Needed to re-embed the documents when the embedder changes.",
    )
    enforce_owner_acl: bool = Field(
        default=True,
        title="Enforce owner visibility on recall",
        description="Drop recalled chunks ingested with 'owner' visibility by another user.",
    )
    default_visibility: Visibility = Field(default="owner", title="Default visibility of ingested items")
    allowed_url_hosts: str = Field(
        default="amazonaws.com,blob.core.windows.net,blob.core.usgovcloudapi.net,blob.core.chinacloudapi.cn",
        title="Allowed hosts for caller-provided URLs",
        description="Comma-separated hosts or parent domains for pre-signed URLs, custom S3 endpoints "
                    "and Azure account URLs. Anything else is refused (SSRF protection).",
    )
    allow_http_urls: bool = Field(
        default=False,
        title="Allow plain HTTP URLs",
        description="Only for local emulators (MinIO, Azurite) on trusted networks.",
    )
    allow_agent_visibility: bool = Field(
        default=True,
        title="Allow agent-wide visibility",
        description="If disabled, every item is visible only to the user who ingested it.",
    )
    google_oauth_client_id: str = Field(
        default="",
        title="Google OAuth client ID",
        description="Needed only to refresh Google Drive access tokens, for requests that carry a refresh_token.",
    )
    google_oauth_client_secret: str = Field(
        default="",
        title="Google OAuth client secret",
        description="Secret of the same OAuth client that issued the refresh tokens.",
    )
    azure_tenant_id: str = Field(
        default="",
        # a hostname-like value or a GUID: it becomes part of the Entra ID token URL
        pattern=r"^$|^[A-Za-z0-9][A-Za-z0-9-]{0,62}(\.[A-Za-z0-9][A-Za-z0-9-]{0,62})*$",
        title="Azure tenant (Entra ID)",
        description="Tenant ID or domain of the Entra ID app, e.g. contoso.onmicrosoft.com. "
                    "Needed only to refresh Azure bearer tokens, for requests that carry a refresh_token.",
    )
    azure_client_id: str = Field(default="", title="Azure client ID (Entra ID app)")
    azure_client_secret: str = Field(
        default="",
        title="Azure client secret (Entra ID app)",
        description="Secret of the same Entra ID app that issued the refresh tokens.",
    )

    @property
    def max_file_bytes(self) -> int:
        return self.max_file_size_mb * 1024 * 1024

    @property
    def max_total_bytes(self) -> int:
        return self.max_total_size_mb * 1024 * 1024

    @property
    def allowed_host_suffixes(self) -> frozenset[str]:
        return frozenset(
            h.strip().lower().lstrip(".") for h in self.allowed_url_hosts.split(",") if h.strip()
        )

    @property
    def oauth_clients(self) -> Dict[str, OAuthClient]:
        """OAuth clients by provider key; a client is configured only when all its values are set."""
        clients: Dict[str, OAuthClient] = {}
        google_id, google_secret = self.google_oauth_client_id.strip(), self.google_oauth_client_secret.strip()
        if google_id and google_secret:
            clients["google_drive"] = OAuthClient(google_id, SecretStr(google_secret))
        azure_tenant, azure_id = self.azure_tenant_id.strip(), self.azure_client_id.strip()
        azure_secret = self.azure_client_secret.strip()
        if azure_tenant and azure_id and azure_secret:
            clients["azure_blob"] = OAuthClient(azure_id, SecretStr(azure_secret), tenant_id=azure_tenant)
        return clients

    @classmethod
    def from_raw(cls, raw: Dict[str, Any] | None) -> "ConnectorsSettings":
        return cls.model_validate(raw or {})


async def load_connectors_settings(plugin, agent_id: str) -> ConnectorsSettings:
    return ConnectorsSettings.from_raw(await plugin.load_settings(agent_id))
