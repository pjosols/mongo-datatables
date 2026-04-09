"""Tests for the datatables/search/ subpackage structure and imports."""
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

    def test_old_top_level_files_are_shims_only(self):
        """Old top-level files must only re-export, not define logic."""
        for name in ("search_builder", "search_fixed", "search_panes"):
            src = (Path(__file__).parent.parent.parent.parent.parent / "mongo_datatables" / f"{name}.py").read_text()
            assert "def " not in src, f"{name}.py must not define functions — it should only re-export"


# ---------------------------------------------------------------------------
# Import paths
# ---------------------------------------------------------------------------

class TestSearchSubpackageImports:
    def test_builder_importable_from_new_path(self):
        from mongo_datatables.datatables.search.builder import parse_search_builder, _sb_group, _sb_date, _sb_number, _sb_string
        assert callable(parse_search_builder)
        assert callable(_sb_group)

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

    def test_backward_compat_shim_search_builder(self):
        from mongo_datatables.search_builder import parse_search_builder, _sb_group
        from mongo_datatables.datatables.search.builder import parse_search_builder as canonical
        assert parse_search_builder is canonical

    def test_backward_compat_shim_search_fixed(self):
        from mongo_datatables.search_fixed import parse_search_fixed
        from mongo_datatables.datatables.search.fixed import parse_search_fixed as canonical
        assert parse_search_fixed is canonical

    def test_backward_compat_shim_search_panes(self):
        from mongo_datatables.search_panes import get_searchpanes_options
        from mongo_datatables.datatables.search.panes import get_searchpanes_options as canonical
        assert get_searchpanes_options is canonical


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
