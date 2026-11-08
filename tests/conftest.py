"""pytest configuration of the tests of the plugin.

pytest imports the ``cat`` package (setting up the packages holding the tests) before any ``setUpModule``: the test-only
secrets of ``support.use_test_secrets`` must be there first.
"""
import support

support.use_test_secrets()
