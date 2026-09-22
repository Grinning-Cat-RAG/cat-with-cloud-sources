"""Encryption at rest of the plugin secrets (OAuth client secrets) in the settings stored by the core.

Run from the plugin root: ``python -m unittest discover -s tests``.

The plugin modules are imported lazily in ``setUpModule``: the Cat imports every
``.py`` file of the plugin, and at import time this module needs only the stdlib.
"""
import enum
import importlib
import sys
import types
import unittest
from pathlib import Path

config = settings_hooks = None

PACKAGE = "cloud_sources_under_test"
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


def _module(name):
    if name not in sys.modules:
        sys.modules[name] = types.ModuleType(name)
    return sys.modules[name]


def _stub_cat() -> None:
    """Add what settings.py needs to the (possibly already stubbed) ``cat`` package."""
    noop = lambda *args, **kwargs: None  # noqa: E731
    cat = _module("cat")
    cat.log = types.SimpleNamespace(info=noop, warning=noop, debug=noop, error=lambda msg, *a, **k: ERRORS.append(msg))
    # like the core CatPluginDecorator: the core calls ``.function``
    cat.plugin = lambda function: types.SimpleNamespace(function=function, name=function.__name__)
    cruds = _module("cat.db.cruds")
    cruds.plugins = STORE
    _module("cat.db").cruds = cruds
    sys.modules["cat.db.cruds.plugins"] = STORE
    _module("cat.services.string_crypto").StringCrypto = FakeCrypto
    models = _module("cat.services.memory.models")
    if not hasattr(models, "VectorMemoryType"):
        models.VectorMemoryType = enum.Enum("VectorMemoryType", {"DECLARATIVE": "declarative", "EPISODIC": "episodic"})


def setUpModule():
    global config, settings_hooks
    if PACKAGE not in sys.modules:
        module = types.ModuleType(PACKAGE)
        module.__path__ = [str(Path(__file__).resolve().parents[1])]
        sys.modules[PACKAGE] = module
    _stub_cat()
    config = importlib.import_module(f"{PACKAGE}.ingestion.config")
    settings_hooks = importlib.import_module(f"{PACKAGE}.settings")


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

    async def test_load_without_stored_settings_returns_defaults(self):
        self.assertEqual(await settings_hooks.load_settings.function(PLUGIN_ID, AGENT_ID), config.ConnectorsSettings().model_dump())

    async def test_load_with_undecryptable_secret_logs_and_disables_it(self):
        STORE.data[(AGENT_ID, PLUGIN_ID)] = {"azure_client_secret": "garbage", "azure_client_id": "cid"}
        loaded = await settings_hooks.load_settings.function(PLUGIN_ID, AGENT_ID)
        self.assertEqual(loaded["azure_client_secret"], "")
        self.assertEqual(len(ERRORS), 1)
        self.assertIn("azure_client_secret", ERRORS[0])
        self.assertNotIn("garbage", ERRORS[0])


if __name__ == "__main__":
    unittest.main()
