# -*- coding: utf-8 -*-
"""Integration tests for data manager functions.

Note: Many tests require data files in src/mpcaHydro/data/ which may not be present.
These tests will be skipped if required files are missing.
"""

import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
import pytest
import pandas as pd


# Check if warehouse module can be imported (requires data files)
try:
    from mpcaHydro import warehouse
    WAREHOUSE_AVAILABLE = True
except Exception:
    WAREHOUSE_AVAILABLE = False
    warehouse = None

requires_warehouse = pytest.mark.skipif(
    not WAREHOUSE_AVAILABLE,
    reason="Warehouse module not available (missing data files)"
)


class TestDataManagerFunctionsIntegration:
    """Integration tests for data_manager_functions module."""

    def test_get_db_path(self):
        """Test get_db_path returns correct path."""
        from mpcaHydro.data_manager_functions import get_db_path
        
        result = get_db_path('/my/folder')
        assert result == Path('/my/folder/observations.duckdb')

    @requires_warehouse
    def test_init_warehouse_integration(self):
        """Test init_warehouse creates and returns path."""
        from mpcaHydro.data_manager_functions import init_warehouse
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            result = init_warehouse(db_path, reset=True)
            
            assert result == db_path
            assert db_path.exists()

    @requires_warehouse
    def test_update_views_integration(self):
        """Test update_views works with initialized warehouse."""
        from mpcaHydro.data_manager_functions import init_warehouse, update_views
        from mpcaHydro import warehouse
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            init_warehouse(db_path, reset=True)
            
            with warehouse.connect(db_path.as_posix(), read_only=False) as con:
                # Should not raise
                update_views(con)


class TestDataWarehouseWorkflow:
    """Tests for complete data warehouse workflow."""

    @requires_warehouse
    def test_full_warehouse_workflow_with_mock_data(self):
        """Test complete workflow: init, load staging, process to analytics."""
        from mpcaHydro.data_manager_functions import init_warehouse
        from mpcaHydro import warehouse
        
        # Create mock WISKI-style data
        mock_wiski_staging = pd.DataFrame({
            'station_no': ['H001', 'H001', 'H002'],
            'Timestamp': ['2020-01-01 00:00', '2020-01-01 01:00', '2020-01-01 00:00'],
            'Value': [100.0, 105.0, 200.0],
            'Quality Code': [1, 1, 1],
            'ts_unitsymbol': ['cfs', 'cfs', 'cfs'],
            'parametertype_id': ['11500', '11500', '11500'],
            'parametertype_name': ['Discharge', 'Discharge', 'Discharge'],
        })
        
        mock_wiski_analytics = pd.DataFrame({
            'station_id': ['H001', 'H001', 'H002'],
            'datetime': pd.to_datetime(['2020-01-01 00:00', '2020-01-01 01:00', '2020-01-01 00:00']),
            'value': [100.0, 105.0, 200.0],
            'constituent': ['Q', 'Q', 'Q'],
            'unit': ['cfs', 'cfs', 'cfs'],
            'station_origin': ['wiski', 'wiski', 'wiski'],
        })
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            
            # Step 1: Initialize warehouse
            init_warehouse(db_path, reset=True)
            
            with warehouse.connect(db_path.as_posix(), read_only=False) as con:
                # Step 2: Load staging data
                warehouse.load_df_to_table(con, mock_wiski_staging, 'staging.wiski')
                
                # Verify staging data
                staging_count = con.execute("SELECT COUNT(*) FROM staging.wiski").fetchone()[0]
                assert staging_count == 3
                
                # Step 3: Load analytics data
                warehouse.load_df_to_table(con, mock_wiski_analytics, 'analytics.wiski')
                
                # Verify analytics data
                analytics_count = con.execute("SELECT COUNT(*) FROM analytics.wiski").fetchone()[0]
                assert analytics_count == 3
                
                # Step 4: Update views
                warehouse.update_views(con)


class TestDropStationData:
    """Tests for dropping station data."""

    @requires_warehouse
    def test_drop_wiski_station_data(self):
        """Test dropping WISKI station data."""
        from mpcaHydro.data_manager_functions import init_warehouse, drop_wiski_station_data
        from mpcaHydro import warehouse
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            init_warehouse(db_path, reset=True)
            
            # Add test data
            staging_data = pd.DataFrame({
                'station_no': ['H001', 'H001', 'H002'],
                'Value': [1, 2, 3]
            })
            analytics_data = pd.DataFrame({
                'station_id': ['H001', 'H001', 'H002'],
                'value': [1.0, 2.0, 3.0]
            })
            
            with warehouse.connect(db_path.as_posix(), read_only=False) as con:
                warehouse.load_df_to_table(con, staging_data, 'staging.wiski')
                warehouse.load_df_to_table(con, analytics_data, 'analytics.wiski')
                
                # Verify initial data
                assert con.execute("SELECT COUNT(*) FROM staging.wiski WHERE station_no = 'H001'").fetchone()[0] == 2
                
                # Drop data for H001
                drop_wiski_station_data(con, ['H001'])
                
                # Verify H001 data is gone
                assert con.execute("SELECT COUNT(*) FROM staging.wiski WHERE station_no = 'H001'").fetchone()[0] == 0
                # H002 data should remain
                assert con.execute("SELECT COUNT(*) FROM staging.wiski WHERE station_no = 'H002'").fetchone()[0] == 1

    @requires_warehouse
    def test_drop_equis_station_data(self):
        """Test dropping EQuIS station data."""
        from mpcaHydro.data_manager_functions import init_warehouse, drop_equis_station_data
        from mpcaHydro import warehouse
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            init_warehouse(db_path, reset=True)
            
            # Add test data
            staging_data = pd.DataFrame({
                'SYS_LOC_CODE': ['S001', 'S001', 'S002'],
                'VALUE': [1, 2, 3]
            })
            analytics_data = pd.DataFrame({
                'station_id': ['S001', 'S001', 'S002'],
                'value': [1.0, 2.0, 3.0]
            })
            
            with warehouse.connect(db_path.as_posix(), read_only=False) as con:
                warehouse.load_df_to_table(con, staging_data, 'staging.equis')
                warehouse.load_df_to_table(con, analytics_data, 'analytics.equis')
                
                # Verify initial data
                assert con.execute("SELECT COUNT(*) FROM staging.equis WHERE SYS_LOC_CODE = 'S001'").fetchone()[0] == 2
                
                # Drop data for S001
                drop_equis_station_data(con, ['S001'])
                
                # Verify S001 data is gone
                assert con.execute("SELECT COUNT(*) FROM staging.equis WHERE SYS_LOC_CODE = 'S001'").fetchone()[0] == 0
                # S002 data should remain
                assert con.execute("SELECT COUNT(*) FROM staging.equis WHERE SYS_LOC_CODE = 'S002'").fetchone()[0] == 1


class TestDataManagerWrapper:
    """Tests for DataManagerWrapper class."""

    def test_wrapper_initialization(self):
        """Test DataManagerWrapper can be initialized."""
        from mpcaHydro.data_manager_functions import DataManagerWrapper
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            wrapper = DataManagerWrapper(db_path)
            
            assert wrapper.db_path == db_path

    def test_wrapper_has_connection_method(self):
        """Test DataManagerWrapper has _connect method."""
        from mpcaHydro.data_manager_functions import DataManagerWrapper
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            wrapper = DataManagerWrapper(db_path)
            
            assert hasattr(wrapper, '_connect')
            assert callable(wrapper._connect)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
