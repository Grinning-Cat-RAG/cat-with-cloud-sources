"""Encryption at rest of the plugin secrets (OAuth client secrets) in the settings stored by the core.

Run from the root of the Cat core: ``python -m unittest discover -s cat/plugins/cat-with-cloud-sources/tests``.

The Cat imports every ``.py`` file of the plugin, tests included: at import time this module
needs only the stdlib, the plugin is loaded in ``setUpModule`` with the loader of the Cat.
"""
import enum
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

config = settings_hooks = None

PLUGIN_PATH = str(Path(__file__).resolve().parents[1])
PLUGIN_ID = "cat_with_cloud_sources"
AGENT_ID = "agent"


class FakeCrypto:
    """Reversible stand-in for the core StringCrypto: invalid ciphertexts raise like Fernet does."""

    def encrypt(self, plaintext: str) -> str:
        return f"enc:{plaintext[::-1]}"

    def decrypt(self, ciphertext: str) -> str:
        if not ciphertext.startswith("enc:"):
            raise ValueError("invalid token")
        return ciphertext[4:][::-1]


class FakePluginSettingsStore:
    """In-memory ``cat.db.cruds.plugins`` with the same merge semantics as the core."""

    def __init__(self):
        self.data = {}

    async def get_setting(self, agent_id, plugin_id):
        value = self.data.get((agent_id, plugin_id))
        return dict(value) if value is not None else None

    async def set_setting(self, agent_id, plugin_id, settings):
        self.data[(agent_id, plugin_id)] = dict(settings)
        return dict(settings)

    async def update_setting(self, agent_id, plugin_id, updated):
        current = await self.get_setting(agent_id, plugin_id) or {}
        current.update(updated)
        return await self.set_setting(agent_id, plugin_id, current)


STORE = FakePluginSettingsStore()
ERRORS = []


def setUpModule():
    global config, settings_hooks
    from cat.looking_glass.mad_hatter.plugin import Plugin

    plugin = Plugin(PLUGIN_PATH)
    plugin._load_decorated_functions()
    settings_hooks = sys.modules[plugin.overrides["load_settings"].function.__module__]
    config = sys.modules[settings_hooks.__name__.rsplit(".", 1)[0] + ".ingestion.config"]


class SecretsCodecTest(unittest.TestCase):
    def test_every_secret_field_is_encrypted(self):
        # the core masks keys containing "_secret": all of them must also be encrypted at rest
        secret_fields = {name for name in config.ConnectorsSettings.model_fields if "_secret" in name}
        self.assertEqual(set(config.SECRET_SETTINGS), secret_fields)

    def test_round_trip(self):
        plain = {"google_oauth_client_secret": "g-secret", "azure_client_secret": "a-secret", "google_oauth_client_id": "cid"}
        encrypted = config.encrypt_secrets(plain, FakeCrypto())
        self.assertNotIn("g-secret", str(encrypted))
        self.assertNotIn("a-secret", str(encrypted))
        self.assertEqual(encrypted["google_oauth_client_id"], "cid")
        self.assertEqual(plain["google_oauth_client_secret"], "g-secret", "the input must not be mutated")
        decrypted, failed = config.decrypt_secrets(encrypted, FakeCrypto())
        self.assertEqual((decrypted, failed), (plain, []))

    def test_empty_secret_stays_empty(self):
        encrypted = config.encrypt_secrets({"azure_client_secret": ""}, FakeCrypto())
        self.assertEqual(encrypted, {"azure_client_secret": ""})
        self.assertEqual(config.decrypt_secrets(encrypted, FakeCrypto()), ({"azure_client_secret": ""}, []))

    def test_undecryptable_secret_is_dropped(self):
        # e.g. CAT_CRYPTO_KEY changed: the secret is unusable, the rest of the settings still works
        decrypted, failed = config.decrypt_secrets(
            {"azure_client_secret": "not-a-token", "max_files_per_job": 5}, FakeCrypto()
        )
        self.assertEqual(decrypted, {"azure_client_secret": "", "max_files_per_job": 5})
        self.assertEqual(failed, ["azure_client_secret"])


class SettingsHooksTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        STORE.data.clear()
        ERRORS.clear()
        log = MagicMock()
        log.error.side_effect = lambda msg, *args, **kwargs: ERRORS.append(msg)
        for patcher in (
            patch.object(settings_hooks, "crud_plugins", STORE),
            patch.object(settings_hooks, "StringCrypto", FakeCrypto),
            patch.object(settings_hooks, "log", log),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    async def test_save_stores_ciphertext_and_returns_plaintext(self):
        payload = {**config.ConnectorsSettings().model_dump(), "google_oauth_client_secret": "g-secret"}
        returned = await settings_hooks.save_settings.function(PLUGIN_ID, payload, AGENT_ID)
        stored = STORE.data[(AGENT_ID, PLUGIN_ID)]
        self.assertNotIn("g-secret", str(stored))
        self.assertEqual(returned["google_oauth_client_secret"], "g-secret")
        self.assertEqual(payload["google_oauth_client_secret"], "g-secret", "the input must not be mutated")

    async def test_load_returns_plaintext(self):
        await settings_hooks.save_settings.function(PLUGIN_ID, {"azure_client_secret": "a-secret", "azure_client_id": "cid"}, AGENT_ID)
        loaded = await settings_hooks.load_settings.function(PLUGIN_ID, AGENT_ID)
        self.assertEqual(loaded["azure_client_secret"], "a-secret")
        self.assertEqual(loaded["azure_client_id"], "cid")

    async def test_partial_save_keeps_stored_secret_encrypted_once(self):
        await settings_hooks.save_settings.function(PLUGIN_ID, {"azure_client_secret": "a-secret"}, AGENT_ID)
        returned = await settings_hooks.save_settings.function(PLUGIN_ID, {"max_files_per_job": 9}, AGENT_ID)
        self.assertEqual(returned["azure_client_secret"], "a-secret")
        loaded = await settings_hooks.load_settings.function(PLUGIN_ID, AGENT_ID)
        self.assertEqual((loaded["azure_client_secret"], loaded["max_files_per_job"]), ("a-secret", 9))

    async def test_the_settings_model_and_schema(self):
        # the loader of the Cat reloads every module: compare the models, not the classes
        self.assertEqual(settings_hooks.settings_model.function().model_json_schema(), config.ConnectorsSettings.model_json_schema())
        self.assertEqual(settings_hooks.settings_schema.function(), config.ConnectorsSettings.model_json_schema())

    async def test_load_without_stored_settings_returns_defaults(self):
        self.assertEqual(await settings_hooks.load_settings.function(PLUGIN_ID, AGENT_ID), config.ConnectorsSettings().model_dump())

    async def test_defaults_are_json_values_as_the_stored_ones(self):
        # the stored settings are JSON (Redis): the defaults must have the same shape, e.g. an enum is its value
        class Mode(enum.Enum):
            FAST = "fast"

        class Settings(config.ConnectorsSettings):
            mode: Mode = Mode.FAST

        with patch.object(settings_hooks, "ConnectorsSettings", Settings):
            loaded = await settings_hooks.load_settings.function(PLUGIN_ID, AGENT_ID)
        self.assertEqual(loaded["mode"], "fast")
        self.assertEqual(json.loads(json.dumps(loaded)), loaded)

    async def test_load_with_undecryptable_secret_logs_and_disables_it(self):
        STORE.data[(AGENT_ID, PLUGIN_ID)] = {"azure_client_secret": "garbage", "azure_client_id": "cid"}
        loaded = await settings_hooks.load_settings.function(PLUGIN_ID, AGENT_ID)
        self.assertEqual(loaded["azure_client_secret"], "")
        self.assertEqual(len(ERRORS), 1)
        self.assertIn("azure_client_secret", ERRORS[0])
        self.assertNotIn("garbage", ERRORS[0])


if __name__ == "__main__":
    unittest.main()
