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


class TestDropStationFunctions:
    """Tests for drop station data functions."""

    def test_drop_wiski_station_data_exists(self):
        """Test drop_wiski_station_data function exists and is callable."""
        from mpcaHydro.data_manager_functions import drop_wiski_station_data
        assert callable(drop_wiski_station_data)

    def test_drop_equis_station_data_exists(self):
        """Test drop_equis_station_data function exists and is callable."""
        from mpcaHydro.data_manager_functions import drop_equis_station_data
        assert callable(drop_equis_station_data)


class TestDownloadFunctionsReplaceParameter:
    """Tests for replace parameter in download functions."""

    def test_download_wiski_data_has_replace_param(self):
        """Test download_wiski_data accepts replace parameter."""
        import inspect
        from mpcaHydro.data_manager_functions import download_wiski_data
        sig = inspect.signature(download_wiski_data)
        assert 'replace' in sig.parameters
        assert sig.parameters['replace'].default is False

    def test_download_equis_data_has_replace_param(self):
        """Test download_equis_data accepts replace parameter."""
        import inspect
        from mpcaHydro.data_manager_functions import download_equis_data
        sig = inspect.signature(download_equis_data)
        assert 'replace' in sig.parameters
        assert sig.parameters['replace'].default is False


class TestDataManagerWrapper:
    """Tests for DataManagerWrapper class."""

    def test_wrapper_exists(self):
        """Test DataManagerWrapper class exists."""
        from mpcaHydro.data_manager_functions import DataManagerWrapper
        assert DataManagerWrapper is not None

    def test_wrapper_can_be_instantiated(self):
        """Test DataManagerWrapper can be instantiated with a path."""
        from mpcaHydro.data_manager_functions import DataManagerWrapper
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            wrapper = DataManagerWrapper(db_path)
            assert wrapper.db_path == db_path

    def test_wrapper_has_expected_methods(self):
        """Test DataManagerWrapper has all expected methods."""
        from mpcaHydro.data_manager_functions import DataManagerWrapper
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            wrapper = DataManagerWrapper(db_path)
            
            # Check core methods exist
            expected_methods = [
                'update_views',
                'process_wiski_data',
                'process_equis_data',
                'process_all_data',
                'download_wiski_data',
                'download_equis_data',
                'get_outlets',
                'get_station_ids',
                'get_observation_data',
                'get_outlet_data',
                'get_station_data',
                'get_raw_data',
                'get_constituent_summary',
                'export_station_to_csv',
                'export_raw_to_csv',
                'get_equis_template',
                'get_wiski_template',
            ]
            
            for method_name in expected_methods:
                assert hasattr(wrapper, method_name), f"Missing method: {method_name}"
                assert callable(getattr(wrapper, method_name)), f"Not callable: {method_name}"

    def test_wrapper_download_methods_have_replace_param(self):
        """Test wrapper download methods have replace parameter."""
        import inspect
        from mpcaHydro.data_manager_functions import DataManagerWrapper
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            wrapper = DataManagerWrapper(db_path)
            
            wiski_sig = inspect.signature(wrapper.download_wiski_data)
            assert 'replace' in wiski_sig.parameters
            
            equis_sig = inspect.signature(wrapper.download_equis_data)
            assert 'replace' in equis_sig.parameters


class TestModuleExports:
    """Test that new functions are exported from data_manager_functions module."""

    def test_drop_functions_exported(self):
        """Test drop functions are exported from data_manager_functions."""
        from mpcaHydro.data_manager_functions import drop_wiski_station_data, drop_equis_station_data
        assert callable(drop_wiski_station_data)
        assert callable(drop_equis_station_data)

    def test_wrapper_exported(self):
        """Test DataManagerWrapper is exported from data_manager_functions."""
        from mpcaHydro.data_manager_functions import DataManagerWrapper
        assert DataManagerWrapper is not None


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
