"""Plugin configuration model. No secrets here: credentials are per request and ephemeral."""
from __future__ import annotations

from typing import Any, Dict, Literal

from pydantic import BaseModel, Field

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
        default="amazonaws.com,blob.core.windows.net",
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

    @classmethod
    def from_raw(cls, raw: Dict[str, Any] | None) -> "ConnectorsSettings":
        """Tolerant loader: unknown keys (e.g. legacy settings) are ignored."""
        known = {k: v for k, v in (raw or {}).items() if k in cls.model_fields}
        return cls.model_validate(known)


async def load_connectors_settings(plugin, agent_id: str) -> ConnectorsSettings:
    return ConnectorsSettings.from_raw(await plugin.load_settings(agent_id))
