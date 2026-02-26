# -*- coding: utf-8 -*-
"""Integration tests for the pywisk module.

These tests make real network requests to the WISKI web service and require:
  1. Network access to wiskiweb01.pca.state.mn.us
  2. Known station_nos and ts_ids that have data

Run with:
    pytest tests/integration/test_pywisk_integration.py -v -m integration
"""

import pytest
import pandas as pd

# ---------------------------------------------------------------------------
# Network availability check
# ---------------------------------------------------------------------------

try:
    from mpcaHydro import pywisk
    is_up, _ = pywisk.test_connection()
    NETWORK_AVAILABLE = is_up
except Exception:
    NETWORK_AVAILABLE = False

skip_network = pytest.mark.skipif(
    not NETWORK_AVAILABLE,
    reason="WISKI network service not available",
)


# ---------------------------------------------------------------------------
# Known test data – fill in values that are known to have data
# ---------------------------------------------------------------------------

# TODO: Replace with station numbers known to have data in WISKI
KNOWN_STATION_NOS = ['H67014001']

# TODO: Replace with a ts_id known to have timeseries data
KNOWN_TS_ID = '424663010'

# TODO: Replace with a parametertype_id known to return results for the station above
KNOWN_PARAMETERTYPE_ID = '11500'

# TODO: Replace with a stationgroup_id known to return WPLMN results
KNOWN_STATIONGROUP_ID = '1319204'

# TODO: Replace with a HUC ID prefix known to match stations
KNOWN_HUC_ID = '0702'


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

@skip_network
@pytest.mark.integration
@pytest.mark.network
class TestPyWiskConnection:
    """Integration tests for WISKI service connectivity."""

    def test_service_is_up(self):
        """Test that the WISKI web service is reachable."""
        is_up, message = pywisk.test_connection()
        assert is_up, f"WISKI service is down: {message}"


# ---------------------------------------------------------------------------
# get_stations()
# ---------------------------------------------------------------------------

@skip_network
@pytest.mark.integration
@pytest.mark.network
class TestGetStationsIntegration:
    """Integration tests for pywisk.get_stations()."""

    def test_get_stations_returns_dataframe(self):
        """Test that get_stations returns a non-empty DataFrame."""
        df = pywisk.get_stations(station_no=KNOWN_STATION_NOS)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_get_stations_has_expected_columns(self):
        """Test that get_stations result contains expected columns."""
        df = pywisk.get_stations(station_no=KNOWN_STATION_NOS)
        assert 'station_no' in df.columns
        assert 'station_name' in df.columns

    def test_get_stations_returns_requested_station(self):
        """Test that get_stations returns the requested station."""
        df = pywisk.get_stations(station_no=KNOWN_STATION_NOS)
        assert KNOWN_STATION_NOS[0] in df['station_no'].values

    def test_get_stations_with_huc_filter(self):
        """Test that get_stations can filter by HUC ID."""
        df = pywisk.get_stations(huc_id=KNOWN_HUC_ID)
        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert all(df['stn_HUC12'].str.startswith(KNOWN_HUC_ID))

    def test_get_stations_with_parametertype(self):
        """Test that get_stations can filter by parametertype_id."""
        df = pywisk.get_stations(parametertype_id=KNOWN_PARAMETERTYPE_ID)
        assert isinstance(df, pd.DataFrame)


# ---------------------------------------------------------------------------
# get_ts_ids()
# ---------------------------------------------------------------------------

@skip_network
@pytest.mark.integration
@pytest.mark.network
class TestGetTsIdsIntegration:
    """Integration tests for pywisk.get_ts_ids()."""

    def test_get_ts_ids_returns_dataframe(self):
        """Test that get_ts_ids returns a non-empty DataFrame."""
        df = pywisk.get_ts_ids(station_nos=KNOWN_STATION_NOS)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty

    def test_get_ts_ids_has_expected_columns(self):
        """Test that get_ts_ids result contains expected columns."""
        df = pywisk.get_ts_ids(station_nos=KNOWN_STATION_NOS)
        assert 'ts_id' in df.columns
        assert 'station_no' in df.columns
        assert 'ts_name' in df.columns

    def test_get_ts_ids_with_parametertype(self):
        """Test that get_ts_ids can filter by parametertype_id."""
        df = pywisk.get_ts_ids(
            station_nos=KNOWN_STATION_NOS,
            parametertype_id=KNOWN_PARAMETERTYPE_ID,
        )
        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert all(df['parametertype_id'].astype(str) == str(KNOWN_PARAMETERTYPE_ID))

    def test_get_ts_ids_with_ts_name(self):
        """Test that get_ts_ids can filter by ts_name."""
        df = pywisk.get_ts_ids(
            station_nos=KNOWN_STATION_NOS,
            ts_name=['20.Day.Mean'],
        )
        assert isinstance(df, pd.DataFrame)

    def test_get_ts_ids_by_ts_id(self):
        """Test that get_ts_ids can look up a specific ts_id."""
        df = pywisk.get_ts_ids(ts_ids=KNOWN_TS_ID)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty


# ---------------------------------------------------------------------------
# get_ts()
# ---------------------------------------------------------------------------

@skip_network
@pytest.mark.integration
@pytest.mark.network
class TestGetTsIntegration:
    """Integration tests for pywisk.get_ts()."""

    def test_get_ts_returns_dataframe(self):
        """Test that get_ts returns a DataFrame for a known ts_id."""
        df = pywisk.get_ts(
            ts_id=KNOWN_TS_ID,
            start_date='2020-01-01',
            end_date='2020-12-31',
        )
        assert isinstance(df, pd.DataFrame)

    def test_get_ts_has_expected_columns(self):
        """Test that get_ts result contains expected columns."""
        df = pywisk.get_ts(
            ts_id=KNOWN_TS_ID,
            start_date='2020-01-01',
            end_date='2020-12-31',
        )
        if not df.empty:
            assert 'Timestamp' in df.columns
            assert 'Value' in df.columns

    def test_get_ts_with_aggregation(self):
        """Test that get_ts works with aggregation parameters."""
        df = pywisk.get_ts(
            ts_id=KNOWN_TS_ID,
            aggregation_interval='daily',
            aggregation_type='mean',
            start_date='2020-01-01',
            end_date='2020-12-31',
        )
        assert isinstance(df, pd.DataFrame)

    def test_get_ts_as_json(self):
        """Test that get_ts returns JSON when as_json=True."""
        result = pywisk.get_ts(
            ts_id=KNOWN_TS_ID,
            start_date='2020-01-01',
            end_date='2020-01-31',
            as_json=True,
        )
        assert isinstance(result, (list, dict))

    def test_get_ts_respects_date_range(self):
        """Test that get_ts returns data within the requested date range."""
        df = pywisk.get_ts(
            ts_id=KNOWN_TS_ID,
            start_date='2020-06-01',
            end_date='2020-06-30',
        )
        if not df.empty:
            timestamps = pd.to_datetime(df['Timestamp'])
            assert timestamps.min() >= pd.Timestamp('2020-06-01')
            assert timestamps.max() <= pd.Timestamp('2020-07-01')


# ---------------------------------------------------------------------------
# get_wplmn()  (scaffolding only – method has a known bug with `self`)
# ---------------------------------------------------------------------------

@skip_network
@pytest.mark.integration
@pytest.mark.network
class TestGetWplmnIntegration:
    """Integration tests for pywisk.get_wplmn().

    NOTE: get_wplmn() currently references `self` but is a module-level
    function. These tests are scaffolded for when that bug is fixed.
    """

    @pytest.mark.skip(reason="get_wplmn uses 'self' but is a module-level function; fix pending")
    def test_get_wplmn_returns_dataframe(self):
        """Test that get_wplmn returns a DataFrame."""
        df = pywisk.get_wplmn(station_nos=KNOWN_STATION_NOS)
        assert isinstance(df, pd.DataFrame)

    @pytest.mark.skip(reason="get_wplmn uses 'self' but is a module-level function; fix pending")
    def test_get_wplmn_empty_stations(self):
        """Test that get_wplmn handles stations with no WPLMN data."""
        df = pywisk.get_wplmn(station_nos=['NONEXISTENT_STATION'])
        assert isinstance(df, pd.DataFrame)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
