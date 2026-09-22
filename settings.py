"""Settings hooks: the schema, and the storage with the OAuth client secrets encrypted at rest.

The core stores plugin settings in Redis as they are: the secrets are encrypted here with
the core ``StringCrypto`` (``CAT_CRYPTO_KEY``/``CAT_CRYPTO_SALT``) on save and decrypted on
load, so the core routes and the plugin code only ever see plaintext.
"""
from typing import Any, Dict

from cat import log, plugin
from cat.db.cruds import plugins as crud_plugins
from cat.services.string_crypto import StringCrypto

from .ingestion.config import ConnectorsSettings, decrypt_secrets, encrypt_secrets


@plugin
def settings_schema():
    return ConnectorsSettings.model_json_schema()


@plugin
def settings_model():
    return ConnectorsSettings


def _decrypted(stored: Dict[str, Any], agent_id: str) -> Dict[str, Any]:
    settings, failed = decrypt_secrets(stored, StringCrypto())
    for key in failed:
        log.error(
            f"[connectors] agent {agent_id}: cannot decrypt '{key}' (was CAT_CRYPTO_KEY changed?): "
            "it is ignored until saved again"
        )
    return settings


@plugin
async def load_settings(plugin_id: str, agent_id: str) -> Dict[str, Any]:
    stored = await crud_plugins.get_setting(agent_id, plugin_id)
    if stored is None:
        return ConnectorsSettings().model_dump()
    return _decrypted(stored, agent_id)


@plugin
async def save_settings(plugin_id: str, settings: Dict[str, Any], agent_id: str) -> Dict[str, Any]:
    stored = await crud_plugins.update_setting(agent_id, plugin_id, encrypt_secrets(settings, StringCrypto()))
    return _decrypted(stored, agent_id)
