"""Verify datatables/search/ subpackage structure, imports, and module paths.

Validates that search modules (builder, fixed, panes) are correctly located in
the subpackage, re-exported from __init__, and imported by consumers using new
paths. Ensures old top-level shim files are removed and logger names reflect
new module paths.
"""
from pathlib import Path

_SEARCH_PKG = Path(__file__).parent.parent.parent.parent.parent / "mongo_datatables" / "datatables" / "search"


# ---------------------------------------------------------------------------
# Package structure
# ---------------------------------------------------------------------------

class TestSearchSubpackageStructure:
    def test_init_exists(self):
        assert (_SEARCH_PKG / "__init__.py").exists()

    def test_builder_module_exists(self):
        assert (_SEARCH_PKG / "builder.py").exists()

    def test_fixed_module_exists(self):
        assert (_SEARCH_PKG / "fixed.py").exists()

    def test_panes_module_exists(self):
        assert (_SEARCH_PKG / "panes.py").exists()

    def test_old_top_level_files_removed(self):
        """Old top-level shim files must not exist."""
        for name in ("search_builder", "search_fixed", "search_panes"):
            path = Path(__file__).parent.parent.parent.parent.parent / "mongo_datatables" / f"{name}.py"
            assert not path.exists(), f"{name}.py shim must be deleted"


# ---------------------------------------------------------------------------
# Import paths
# ---------------------------------------------------------------------------

class TestSearchSubpackageImports:
    def test_builder_importable_from_new_path(self):
        from mongo_datatables.datatables.search.builder import parse_search_builder
        assert callable(parse_search_builder)

    def test_fixed_importable_from_new_path(self):
        from mongo_datatables.datatables.search.fixed import parse_search_fixed, parse_column_search_fixed
        assert callable(parse_search_fixed)
        assert callable(parse_column_search_fixed)

    def test_panes_importable_from_new_path(self):
        from mongo_datatables.datatables.search.panes import get_searchpanes_options, parse_searchpanes_filters
        assert callable(get_searchpanes_options)
        assert callable(parse_searchpanes_filters)

    def test_init_re_exports_all_public_names(self):
        import mongo_datatables.datatables.search as search_pkg
        for name in ("parse_search_builder", "parse_search_fixed", "parse_column_search_fixed",
                     "get_searchpanes_options", "parse_searchpanes_filters"):
            assert hasattr(search_pkg, name), f"search.__init__ must export {name}"

    def test_backward_compat_shims_removed(self):
        """Shim files must not exist."""
        for name in ("search_builder", "search_fixed", "search_panes"):
            path = Path(__file__).parent.parent.parent.parent.parent / "mongo_datatables" / f"{name}.py"
            assert not path.exists(), f"{name}.py shim must be deleted"


# ---------------------------------------------------------------------------
# Consumer import sites use new paths
# ---------------------------------------------------------------------------

class TestConsumerImportSites:
    _DT_ROOT = Path(__file__).parent.parent.parent.parent.parent / "mongo_datatables" / "datatables"

    def test_filter_imports_builder_from_search_subpackage(self):
        src = (self._DT_ROOT / "filter.py").read_text()
        assert "from .search.builder import" in src or "from mongo_datatables.datatables.search.builder import" in src

    def test_filter_imports_fixed_from_search_subpackage(self):
        src = (self._DT_ROOT / "filter.py").read_text()
        assert "from .search.fixed import" in src or "from mongo_datatables.datatables.search.fixed import" in src

    def test_filter_imports_panes_from_search_subpackage(self):
        src = (self._DT_ROOT / "filter.py").read_text()
        assert "from .search.panes import" in src or "from mongo_datatables.datatables.search.panes import" in src

    def test_core_imports_panes_from_search_subpackage(self):
        src = (self._DT_ROOT / "core.py").read_text()
        assert "from .search.panes import" in src or "from mongo_datatables.datatables.search.panes import" in src

    def test_filter_has_no_old_top_level_search_imports(self):
        src = (self._DT_ROOT / "filter.py").read_text()
        assert "from mongo_datatables.search_builder import" not in src
        assert "from mongo_datatables.search_fixed import" not in src
        assert "from mongo_datatables.search_panes import" not in src

    def test_core_has_no_old_top_level_search_imports(self):
        src = (self._DT_ROOT / "core.py").read_text()
        assert "from mongo_datatables.search_panes import" not in src

    def test_compat_has_no_old_top_level_search_imports(self):
        src = (self._DT_ROOT / "compat.py").read_text()
        assert "from mongo_datatables.search_builder import" not in src
        assert "from mongo_datatables.search_fixed import" not in src
        assert "from mongo_datatables.search_panes import" not in src


# ---------------------------------------------------------------------------
# fixed.py uses canonical FieldMapper import path
# ---------------------------------------------------------------------------

class TestFixedImportPath:
    _FIXED = Path(__file__).parent.parent.parent.parent.parent / "mongo_datatables" / "datatables" / "search" / "fixed.py"

    def test_fieldmapper_imported_from_utils_not_field_utils(self):
        src = self._FIXED.read_text()
        assert "from mongo_datatables.utils import" in src and "FieldMapper" in src

    def test_fieldmapper_not_imported_directly_from_field_utils(self):
        src = self._FIXED.read_text()
        assert "from mongo_datatables.field_utils import FieldMapper" not in src


# ---------------------------------------------------------------------------
# Logger names reflect new module paths
# ---------------------------------------------------------------------------

class TestLoggerNames:
    def test_builder_logger_name(self):
        import mongo_datatables.datatables.search.builder as mod
        assert mod._log.name == "mongo_datatables.datatables.search.builder"

    def test_panes_logger_name(self):
        import mongo_datatables.datatables.search.panes as mod
        assert mod.logger.name == "mongo_datatables.datatables.search.panes"


# ---------------------------------------------------------------------------
# Top-level DataTables import still works
# ---------------------------------------------------------------------------

def test_datatables_top_level_import():
    from mongo_datatables import DataTables
    assert callable(DataTables)
