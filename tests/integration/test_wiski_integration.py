# -*- coding: utf-8 -*-
"""Integration tests for WISKI data operations.

Note: Network tests require access to the WISKI web service.
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


# Check if WISKI service is accessible
try:
    from mpcaHydro import pywisk
    is_up, _ = pywisk.test_connection()
    NETWORK_AVAILABLE = is_up
except Exception:
    NETWORK_AVAILABLE = False

skip_network = pytest.mark.skipif(
    not NETWORK_AVAILABLE,
    reason="WISKI network service not available"
)


class TestPyWiskConnection:
    """Tests for pywisk service connection."""

    @skip_network
    @pytest.mark.integration
    @pytest.mark.network
    def test_wiski_service_connection(self):
        """Test connection to WISKI web service (requires network)."""
        from mpcaHydro import pywisk
        
        is_up, message = pywisk.test_connection()
        # Note: This test may fail if the WISKI service is down
        # It's marked as integration/network test
        print(f"WISKI service status: {message}")


class TestWiskiDownload:
    """Tests for downloading WISKI data."""

    @skip_network
    @pytest.mark.integration
    @pytest.mark.network
    def test_download_station_data(self):
        """Test downloading data for a single station (requires network)."""
        from mpcaHydro import pywisk
        
        # Use a known station ID
        station_ids = ['H67014001']
        
        # Get timeseries IDs for the station
        df_ts = pywisk.get_ts_ids(station_nos=station_ids)
        
        assert not df_ts.empty
        assert 'ts_id' in df_ts.columns
        assert 'station_no' in df_ts.columns

    @skip_network
    @pytest.mark.integration
    @pytest.mark.network
    def test_get_stations_returns_dataframe(self):
        """Test that get_stations returns a DataFrame (requires network)."""
        from mpcaHydro import pywisk
        
        df = pywisk.get_stations(station_no=['H67014001'])
        
        assert isinstance(df, pd.DataFrame)
        assert 'station_no' in df.columns

    @skip_network
    @pytest.mark.integration
    @pytest.mark.network
    def test_get_ts_returns_data(self):
        """Test that get_ts returns timeseries data (requires network)."""
        from mpcaHydro import pywisk
        
        # First get a valid ts_id
        station_ids = ['H67014001']
        df_ts = pywisk.get_ts_ids(station_nos=station_ids, parametertype_id='11500')  # Flow
        
        if not df_ts.empty:
            ts_id = df_ts['ts_id'].iloc[0]
            df = pywisk.get_ts(ts_id, start_date='2020-01-01', end_date='2020-01-31')
            
            assert isinstance(df, pd.DataFrame)
            if not df.empty:
                assert 'Timestamp' in df.columns
                assert 'Value' in df.columns


class TestWiskiDataWithMock:
    """Tests for WISKI data operations using mocks."""

    @requires_warehouse
    def test_download_with_mock_data(self):
        """Test WISKI download flow with mocked service."""
        from mpcaHydro import data_manager_functions as dmf
        from mpcaHydro import warehouse
        
        # Create mock data that mimics WISKI download format
        mock_wiski_data = pd.DataFrame({
            'station_no': ['H67014001', 'H67014001'],
            'Timestamp': ['2020-01-01 00:00', '2020-01-01 01:00'],
            'Value': [100.0, 105.0],
            'Quality Code': [1, 1],
            'ts_unitsymbol': ['cfs', 'cfs'],
            'parametertype_id': ['11500', '11500'],
            'parametertype_name': ['Discharge', 'Discharge'],
        })
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            warehouse.init_db(db_path.as_posix(), reset=True)
            
            with warehouse.connect(db_path.as_posix(), read_only=False) as con:
                # Load mock data directly
                warehouse.load_df_to_table(con, mock_wiski_data, 'staging.wiski')
                
                # Verify data loaded
                result = con.execute("SELECT COUNT(*) FROM staging.wiski").fetchone()[0]
                assert result == 2


class TestWiskiTransform:
    """Tests for WISKI data transformation."""

    def test_transform_creates_analytics_data(self):
        """Test that WISKI transform creates properly formatted analytics data."""
        # This would test the wiski.transform() function
        # Currently skipping as it requires specific data format
        pass


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
