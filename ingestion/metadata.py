"""Metadata written on every chunk ingested by a connector, and the visibility rules.

All keys share the ``connector_`` prefix: callers cannot set them, so ACL
metadata cannot be spoofed through the request.
"""
from __future__ import annotations

import hashlib
import os
import re
from typing import Any, Dict, Mapping

PREFIX = "connector_"

KEY_PROVIDER = f"{PREFIX}provider"
KEY_ITEM_ID = f"{PREFIX}item_id"
KEY_VERSION = f"{PREFIX}version"
KEY_OWNER = f"{PREFIX}owner"
KEY_VISIBILITY = f"{PREFIX}visibility"
KEY_PATH = f"{PREFIX}path"
KEY_URL = f"{PREFIX}url"
KEY_JOB = f"{PREFIX}job_id"

VISIBILITY_OWNER = "owner"
VISIBILITY_AGENT = "agent"
AGENT_OWNER = "*"

# keys managed by the core or by this plugin: never accepted from the caller
RESERVED_KEYS = frozenset({"source", "when", "hash", "chat_id", "page_content", "image_file"})

_UNSAFE_CHARS = re.compile(r"[^\w\-]+", re.UNICODE)


def sanitize_user_metadata(metadata: Mapping[str, Any] | None) -> Dict[str, Any]:
    clean: Dict[str, Any] = {}
    for key, value in (metadata or {}).items():
        if key.startswith(PREFIX) or key in RESERVED_KEYS:
            continue
        if isinstance(value, (str, int, float, bool)) or value is None:
            clean[key] = value
    return clean


def owner_for(visibility: str, user_id: str) -> str:
    return user_id if visibility == VISIBILITY_OWNER else AGENT_OWNER


def identity_filter(provider: str, item_id: str, owner: str, chat_id: str | None) -> Dict[str, Any]:
    """Metadata identifying all the chunks of one item for one owner (and chat, if chat-scoped)."""
    identity = {KEY_PROVIDER: provider, KEY_ITEM_ID: item_id, KEY_OWNER: owner}
    if chat_id:
        identity["chat_id"] = chat_id
    return identity


def build_metadata(
    *,
    provider: str,
    item_id: str,
    version: str,
    owner: str,
    visibility: str,
    job_id: str,
    path: str | None,
    web_url: str | None,
    item_extra: Mapping[str, Any],
    user_metadata: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    metadata = sanitize_user_metadata(user_metadata)
    for key, value in item_extra.items():
        if isinstance(value, (str, int, float, bool)):
            metadata[f"{PREFIX}{key}"] = value
    # ACL/identity keys last: they always win
    metadata.update({
        KEY_PROVIDER: provider,
        KEY_ITEM_ID: item_id,
        KEY_VERSION: version,
        KEY_OWNER: owner,
        KEY_VISIBILITY: visibility,
        KEY_JOB: job_id,
    })
    if path:
        metadata[KEY_PATH] = path
    if web_url:
        metadata[KEY_URL] = web_url
    return metadata


def source_name(name: str, extension: str | None, provider: str, item_id: str, owner: str) -> str:
    """Unique, stable source name: same Drive name in two folders (or two owners) must not collide.

    The core file manager stores files by source name, so it must be unique per item/owner.
    """
    stem, current_ext = os.path.splitext(name)
    ext = extension or current_ext
    if ext and not ext.startswith("."):
        ext = f".{ext}"
    safe_stem = _UNSAFE_CHARS.sub("_", stem).strip("_")[:80] or "file"
    digest = hashlib.sha256(f"{provider}\x00{item_id}\x00{owner}".encode()).hexdigest()[:10]
    return f"{safe_stem}--{provider}-{digest}{ext}"


def is_visible_to(metadata: Mapping[str, Any] | None, user_id: str | None) -> bool:
    """Visibility rule applied on recall. Chunks not produced by a connector are untouched."""
    if not metadata or KEY_PROVIDER not in metadata:
        return True
    visibility = metadata.get(KEY_VISIBILITY)
    if visibility == VISIBILITY_AGENT:
        return True
    # owner visibility, or anything unexpected: fail closed
    return user_id is not None and metadata.get(KEY_OWNER) == user_id
