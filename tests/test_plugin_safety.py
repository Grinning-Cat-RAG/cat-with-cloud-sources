"""The Cat imports every ``.py`` file of the plugin, tests included, after checking it with its AST scanner: every file
passes the scanner, and the tests import only the standard library at import time.
"""
import ast
import os
import sys
import unittest
from pathlib import Path

TESTS_PATH = os.path.dirname(os.path.abspath(__file__))
PLUGIN_PATH = os.path.dirname(TESTS_PATH)


def _import_time_modules(tree: ast.AST):
    """The modules imported when the file is imported: the bodies of the functions run later."""
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            continue
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            yield "." * node.level + (node.module or "")
        yield from _import_time_modules(node)


class TestPluginSafety(unittest.TestCase):
    def test_every_file_passes_the_scanner_of_the_core(self):
        from cat.looking_glass.mad_hatter.plugin_extractor import PluginExtractor

        self.assertTrue(PluginExtractor._is_safe_plugin(PLUGIN_PATH))

    def test_tests_import_only_the_stdlib_at_import_time(self):
        for test_file in sorted(Path(TESTS_PATH).glob("*.py")):
            tree = ast.parse(test_file.read_text(encoding="utf-8"))
            for module in _import_time_modules(tree):
                with self.subTest(file=test_file.name, module=module):
                    self.assertIn(module.split(".")[0], sys.stdlib_module_names)


if __name__ == "__main__":
    unittest.main()
