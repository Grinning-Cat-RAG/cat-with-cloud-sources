"""Every ``.py`` file of the plugin, tests included, passes the AST scanner of the core, which checks a plugin before
installing it (the Cat does not import the tests, but the scanner reads them).
"""
import os
import unittest

TESTS_PATH = os.path.dirname(os.path.abspath(__file__))
PLUGIN_PATH = os.path.dirname(TESTS_PATH)


class TestPluginSafety(unittest.TestCase):
    def test_every_file_passes_the_scanner_of_the_core(self):
        from cat.looking_glass.mad_hatter.plugin_extractor import PluginExtractor

        self.assertTrue(PluginExtractor._is_safe_plugin(PLUGIN_PATH))


if __name__ == "__main__":
    unittest.main()
