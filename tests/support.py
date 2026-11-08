"""Shared setup of the tests (``python -m unittest discover``; pytest gets the same from ``conftest.py``).

The Cat does not import the tests of a plugin, but its security scanner reads them: no dynamic imports here either.
"""
import os


def use_test_secrets() -> None:
    """The core has no default for the secrets of its encryption (``CAT_CRYPTO_KEY``, ``CAT_CRYPTO_SALT``): it refuses
    to start without them, and loading a plugin reads them. The tests run outside the container: test-only values when
    the environment has none."""
    os.environ.setdefault("CAT_CRYPTO_KEY", "test-only-crypto-key")
    os.environ.setdefault("CAT_CRYPTO_SALT", "test-only-crypto-salt")
