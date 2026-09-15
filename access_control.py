"""Recall-time visibility filter for chunks ingested by connectors.

The vector store metadata filter supports only equality (AND), so "owned by me OR
agent-wide OR not from a connector" cannot be expressed as a pre-filter. The
chunks are filtered right after the recall, before any other hook or the LLM
sees them. On any internal error the filter fails closed.
"""
from cat import hook, log

from .ingestion import metadata as md
from .ingestion.config import load_connectors_settings


def _metadata_of(memory):
    document = getattr(memory, "document", None)
    return getattr(document, "metadata", None) or {}


# high priority: hooks run in descending priority, so this filter runs first
@hook(priority=1000)
async def after_cat_recalls_memories(config, cat) -> None:
    memories = cat.working_memory.context_memories
    if not memories:
        return

    try:
        settings = await load_connectors_settings(cat.plugin_manager.get_plugin(), cat.agent_key)
        if not settings.enforce_owner_acl:
            return
        user_id = cat.user.id if getattr(cat, "user", None) else None
        visible = [m for m in memories if md.is_visible_to(_metadata_of(m), user_id)]
    except Exception as e:  # noqa: BLE001
        log.error(f"[connectors] visibility filter failed, dropping all connector chunks: {type(e).__name__}: {e}")
        visible = [m for m in memories if md.KEY_PROVIDER not in _metadata_of(m)]

    if len(visible) != len(memories):
        log.debug(f"[connectors] visibility filter removed {len(memories) - len(visible)} recalled chunks")
    cat.working_memory.context_memories = visible
