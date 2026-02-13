# -*- coding: utf-8 -*-
"""Tests for the data_manager_functions procedural module."""

import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
import pandas as pd


class TestConstants:
    """Tests for module constants."""

    def test_agg_defaults(self):
        """Test aggregation defaults are properly defined."""
        from mpcaHydro.data_manager_functions import AGG_DEFAULTS
        assert 'cfs' in AGG_DEFAULTS
        assert AGG_DEFAULTS['cfs'] == 'mean'
        assert AGG_DEFAULTS['lb'] == 'sum'

    def test_unit_defaults(self):
        """Test unit defaults are properly defined."""
        from mpcaHydro.data_manager_functions import UNIT_DEFAULTS
        assert 'Q' in UNIT_DEFAULTS
        assert UNIT_DEFAULTS['Q'] == 'cfs'
        assert UNIT_DEFAULTS['TSS'] == 'mg/l'
        assert UNIT_DEFAULTS['WT'] == 'degf'


class TestGetDbPath:
    """Tests for get_db_path function."""

    def test_get_db_path_string(self):
        """Test get_db_path with string input."""
        from mpcaHydro.data_manager_functions import get_db_path
        result = get_db_path('/some/folder')
        assert result == Path('/some/folder/observations.duckdb')

    def test_get_db_path_path(self):
        """Test get_db_path with Path input."""
        from mpcaHydro.data_manager_functions import get_db_path
        result = get_db_path(Path('/another/folder'))
        assert result == Path('/another/folder/observations.duckdb')


class TestValidation:
    """Tests for validation constants."""

    def test_valid_constituents_in_unit_defaults(self):
        """Test that key constituents have unit defaults defined."""
        from mpcaHydro.data_manager_functions import UNIT_DEFAULTS
        expected_constituents = ['Q', 'TSS', 'TP', 'OP', 'TKN', 'N', 'WT']
        for const in expected_constituents:
            assert const in UNIT_DEFAULTS, f"Constituent {const} missing from UNIT_DEFAULTS"

    def test_valid_units_in_agg_defaults(self):
        """Test that common units have aggregation defaults defined."""
        from mpcaHydro.data_manager_functions import AGG_DEFAULTS
        expected_units = ['mg/l', 'lb', 'cfs', 'degf']
        for unit in expected_units:
            assert unit in AGG_DEFAULTS, f"Unit {unit} missing from AGG_DEFAULTS"


class TestDataManagerFunctionsInterface:
    """Test the interface of procedural functions."""

    def test_get_db_path_returns_path(self):
        """Test get_db_path returns a Path object."""
        from mpcaHydro.data_manager_functions import get_db_path
        result = get_db_path('/test/path')
        assert isinstance(result, Path)
        assert result.name == 'observations.duckdb'

    def test_constants_are_dicts(self):
        """Test constants are dictionaries."""
        from mpcaHydro.data_manager_functions import AGG_DEFAULTS, UNIT_DEFAULTS
        assert isinstance(AGG_DEFAULTS, dict)
        assert isinstance(UNIT_DEFAULTS, dict)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
