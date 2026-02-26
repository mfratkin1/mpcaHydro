# -*- coding: utf-8 -*-
"""Unit tests for the pywisk module.

These tests verify that URLs are constructed correctly for each method
without making any network requests.
"""

from unittest.mock import patch, MagicMock, PropertyMock
import pytest
import pandas as pd

from mpcaHydro.pywisk import (
    Service,
    construct_aggregation,
    validate_aggregation_type,
    validate_interval,
    validate_custom_interval,
    validate_percentile,
    VALID_AGGREGATION_TYPES,
    VALID_INTERVALS,
)


# ---------------------------------------------------------------------------
# Service.url() – core URL builder
# ---------------------------------------------------------------------------

class TestServiceUrl:
    """Tests for Service.url() URL construction."""

    def setup_method(self):
        self.service = Service()

    def test_url_with_simple_request(self):
        """Test URL construction with a simple request argument."""
        url = self.service.url({'request': 'getStationList'})
        expected = (
            'https://wiskiweb01.pca.state.mn.us/KiWIS/KiWIS?'
            'datasource=0&service=kisters&type=queryServices&format=json'
            '&request=getStationList'
        )
        assert url == expected

    def test_url_base_dict_always_included(self):
        """Test that base_dict keys (datasource, service, type, format) are always present."""
        url = self.service.url({'request': 'getStationList'})
        assert 'datasource=0' in url
        assert 'service=kisters' in url
        assert 'type=queryServices' in url
        assert 'format=json' in url

    def test_url_list_values_are_comma_joined(self):
        """Test that list values are joined with commas."""
        url = self.service.url({
            'request': 'getTimeseriesList',
            'returnfields': ['ts_id', 'ts_name', 'station_no'],
        })
        assert 'returnfields=ts_id,ts_name,station_no' in url

    def test_url_none_values_are_excluded(self):
        """Test that None values are excluded from the URL."""
        url = self.service.url({
            'request': 'getTimeseriesList',
            'station_no': None,
            'ts_id': '12345',
        })
        assert 'station_no' not in url
        assert 'ts_id=12345' in url

    def test_url_integer_list_values_converted_to_strings(self):
        """Test that integer list values are converted to strings and joined."""
        url = self.service.url({
            'request': 'getTimeseriesList',
            'station_no': [1, 2, 3],
        })
        assert 'station_no=1,2,3' in url

    def test_url_stored_on_instance(self):
        """Test that generated URL is stored on the Service._url attribute."""
        url = self.service.url({'request': 'getStationList'})
        assert self.service._url == url

    def test_url_user_args_override_base_dict(self):
        """Test that user-supplied args override base_dict values."""
        url = self.service.url({'format': 'csv', 'request': 'getStationList'})
        # The merge (base_dict | args_dict) means user values win
        assert 'format=csv' in url
        assert 'format=json' not in url


# ---------------------------------------------------------------------------
# get_ts() – URL construction
# ---------------------------------------------------------------------------

class TestGetTsUrl:
    """Tests for get_ts() URL construction (no network calls)."""

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_ts_basic_url(self, mock_service):
        """Test get_ts constructs correct URL for a basic timeseries request."""
        from mpcaHydro.pywisk import get_ts

        mock_service.get.return_value = pd.DataFrame()

        get_ts(ts_id='424663010', start_date='2020-01-01', end_date='2020-12-31')

        args = mock_service.get.call_args[0][0]
        assert args['request'] == 'getTimeseriesValues'
        assert args['ts_id'] == '424663010'
        assert args['from'] == '2020-01-01'
        assert args['to'] == '2020-12-31'
        assert args['metadata'] == 'true'

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_ts_with_aggregation(self, mock_service):
        """Test get_ts appends aggregation to ts_id when interval and type provided."""
        from mpcaHydro.pywisk import get_ts

        mock_service.get.return_value = pd.DataFrame()

        get_ts(
            ts_id='424663010',
            aggregation_interval='daily',
            aggregation_type='mean',
        )

        args = mock_service.get.call_args[0][0]
        assert args['ts_id'] == '424663010;aggregate(daily~mean)'

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_ts_without_aggregation(self, mock_service):
        """Test get_ts does not modify ts_id when no aggregation provided."""
        from mpcaHydro.pywisk import get_ts

        mock_service.get.return_value = pd.DataFrame()

        get_ts(ts_id='424663010')

        args = mock_service.get.call_args[0][0]
        assert args['ts_id'] == '424663010'

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_ts_returnfields(self, mock_service):
        """Test get_ts includes expected returnfields."""
        from mpcaHydro.pywisk import get_ts

        mock_service.get.return_value = pd.DataFrame()

        get_ts(ts_id='424663010')

        args = mock_service.get.call_args[0][0]
        assert 'Timestamp' in args['returnfields']
        assert 'Value' in args['returnfields']
        assert 'Quality Code' in args['returnfields']

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_ts_md_returnfields(self, mock_service):
        """Test get_ts includes expected metadata returnfields."""
        from mpcaHydro.pywisk import get_ts

        mock_service.get.return_value = pd.DataFrame()

        get_ts(ts_id='424663010')

        args = mock_service.get.call_args[0][0]
        expected_md_fields = [
            'ts_unitsymbol', 'ts_name', 'ts_id',
            'station_no', 'station_name',
            'station_latitude', 'station_longitude',
            'parametertype_id', 'parametertype_name',
            'stationparameter_no', 'stationparameter_name',
        ]
        for field in expected_md_fields:
            assert field in args['md_returnfields']

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_ts_as_json(self, mock_service):
        """Test get_ts calls get_json when as_json=True."""
        from mpcaHydro.pywisk import get_ts

        mock_service.get_json.return_value = {}

        get_ts(ts_id='424663010', as_json=True)

        mock_service.get_json.assert_called_once()
        mock_service.get.assert_not_called()

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_ts_default_timezone(self, mock_service):
        """Test get_ts uses GMT-6 as default timezone."""
        from mpcaHydro.pywisk import get_ts

        mock_service.get.return_value = pd.DataFrame()

        get_ts(ts_id='424663010')

        args = mock_service.get.call_args[0][0]
        assert args['timezone'] == 'GMT-6'


# ---------------------------------------------------------------------------
# get_stations() – URL construction
# ---------------------------------------------------------------------------

class TestGetStationsUrl:
    """Tests for get_stations() URL construction (no network calls)."""

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_stations_basic_url(self, mock_service):
        """Test get_stations constructs correct base args."""
        from mpcaHydro.pywisk import get_stations

        mock_df = pd.DataFrame({'station_no': [], 'stn_HUC12': []})
        mock_service.get.return_value = mock_df

        get_stations()

        args = mock_service.get.call_args[0][0]
        assert args['request'] == 'getStationList'

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_stations_with_station_no(self, mock_service):
        """Test get_stations passes station_no correctly."""
        from mpcaHydro.pywisk import get_stations

        mock_df = pd.DataFrame({'station_no': [], 'stn_HUC12': []})
        mock_service.get.return_value = mock_df

        get_stations(station_no=['H67014001'])

        args = mock_service.get.call_args[0][0]
        assert args['station_no'] == ['H67014001']

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_stations_returnfields_include_defaults(self, mock_service):
        """Test get_stations always includes ca_sta, station_no, station_name in returnfields."""
        from mpcaHydro.pywisk import get_stations

        mock_df = pd.DataFrame({'station_no': [], 'stn_HUC12': []})
        mock_service.get.return_value = mock_df

        get_stations()

        args = mock_service.get.call_args[0][0]
        assert 'ca_sta' in args['returnfields']
        assert 'station_no' in args['returnfields']
        assert 'station_name' in args['returnfields']

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_stations_ca_sta_returnfields(self, mock_service):
        """Test get_stations includes expected ca_sta_returnfields."""
        from mpcaHydro.pywisk import get_stations

        mock_df = pd.DataFrame({'station_no': [], 'stn_HUC12': []})
        mock_service.get.return_value = mock_df

        get_stations()

        args = mock_service.get.call_args[0][0]
        expected_ca_fields = ['stn_HUC12', 'stn_EQuIS_ID', 'stn_AUID',
                              'hydrounit_title', 'hydrounit_no', 'NearestTown']
        assert args['ca_sta_returnfields'] == expected_ca_fields

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_stations_huc_id_filter(self, mock_service):
        """Test get_stations filters by huc_id after fetching."""
        from mpcaHydro.pywisk import get_stations

        mock_df = pd.DataFrame({
            'station_no': ['S1', 'S2'],
            'stn_HUC12': ['070100050101', '070200060202'],
        })
        mock_service.get.return_value = mock_df

        result = get_stations(huc_id='0701')

        assert len(result) == 1
        assert result.iloc[0]['station_no'] == 'S1'

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_stations_with_parametertype_id(self, mock_service):
        """Test get_stations passes parametertype_id correctly."""
        from mpcaHydro.pywisk import get_stations

        mock_df = pd.DataFrame({'station_no': [], 'stn_HUC12': []})
        mock_service.get.return_value = mock_df

        get_stations(parametertype_id='11500')

        args = mock_service.get.call_args[0][0]
        assert args['parametertype_id'] == '11500'


# ---------------------------------------------------------------------------
# get_ts_ids() – URL construction
# ---------------------------------------------------------------------------

class TestGetTsIdsUrl:
    """Tests for get_ts_ids() URL construction (no network calls)."""

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_ts_ids_basic_url(self, mock_service):
        """Test get_ts_ids constructs correct base args."""
        from mpcaHydro.pywisk import get_ts_ids

        mock_service.get.return_value = pd.DataFrame()

        get_ts_ids()

        args = mock_service.get.call_args[0][0]
        assert args['request'] == 'getTimeseriesList'

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_ts_ids_with_station_nos(self, mock_service):
        """Test get_ts_ids passes station_nos correctly."""
        from mpcaHydro.pywisk import get_ts_ids

        mock_service.get.return_value = pd.DataFrame()

        get_ts_ids(station_nos=['H67014001', 'H67009001'])

        args = mock_service.get.call_args[0][0]
        assert args['station_no'] == ['H67014001', 'H67009001']

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_ts_ids_with_ts_ids(self, mock_service):
        """Test get_ts_ids passes ts_ids correctly."""
        from mpcaHydro.pywisk import get_ts_ids

        mock_service.get.return_value = pd.DataFrame()

        get_ts_ids(ts_ids='424663010')

        args = mock_service.get.call_args[0][0]
        assert args['ts_id'] == '424663010'

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_ts_ids_default_returnfields(self, mock_service):
        """Test get_ts_ids uses default returnfields when none provided."""
        from mpcaHydro.pywisk import get_ts_ids

        mock_service.get.return_value = pd.DataFrame()

        get_ts_ids()

        args = mock_service.get.call_args[0][0]
        expected_fields = [
            'ts_id', 'ts_name', 'ca_sta', 'station_no',
            'ts_unitsymbol',
            'parametertype_id', 'parametertype_name',
            'station_latitude', 'station_longitude',
            'stationparameter_no', 'stationparameter_name',
            'station_no', 'station_name',
            'coverage', 'ts_density',
        ]
        assert args['returnfields'] == expected_fields

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_ts_ids_custom_returnfields(self, mock_service):
        """Test get_ts_ids uses custom returnfields when provided."""
        from mpcaHydro.pywisk import get_ts_ids

        mock_service.get.return_value = pd.DataFrame()

        custom_fields = ['ts_id', 'station_no']
        get_ts_ids(returnfields=custom_fields)

        args = mock_service.get.call_args[0][0]
        assert args['returnfields'] == custom_fields

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_ts_ids_ca_sta_returnfields(self, mock_service):
        """Test get_ts_ids includes expected ca_sta_returnfields."""
        from mpcaHydro.pywisk import get_ts_ids

        mock_service.get.return_value = pd.DataFrame()

        get_ts_ids()

        args = mock_service.get.call_args[0][0]
        assert args['ca_sta_returnfields'] == ['stn_HUC12', 'stn_EQuIS_ID', 'stn_AUID']

    @patch('mpcaHydro.pywisk.SERVICE')
    def test_get_ts_ids_with_ts_name(self, mock_service):
        """Test get_ts_ids passes ts_name correctly."""
        from mpcaHydro.pywisk import get_ts_ids

        mock_service.get.return_value = pd.DataFrame()

        get_ts_ids(ts_name=['20.Day.Mean'])

        args = mock_service.get.call_args[0][0]
        assert args['ts_name'] == ['20.Day.Mean']


# ---------------------------------------------------------------------------
# construct_aggregation() and validation helpers
# ---------------------------------------------------------------------------

class TestConstructAggregation:
    """Tests for construct_aggregation() and validation functions."""

    def test_construct_aggregation_daily_mean(self):
        """Test constructing a daily mean aggregation string."""
        result = construct_aggregation('daily', 'mean')
        assert result == 'aggregate(daily~mean)'

    def test_construct_aggregation_yearly_total(self):
        """Test constructing a yearly total aggregation string."""
        result = construct_aggregation('yearly', 'total')
        assert result == 'aggregate(yearly~total)'

    def test_construct_aggregation_hourly_min(self):
        """Test constructing an hourly min aggregation string."""
        result = construct_aggregation('hourly', 'min')
        assert result == 'aggregate(hourly~min)'

    def test_construct_aggregation_monthly_max(self):
        """Test constructing a monthly max aggregation string."""
        result = construct_aggregation('monthly', 'max')
        assert result == 'aggregate(monthly~max)'

    def test_construct_aggregation_with_percentile(self):
        """Test constructing an aggregation string with percentile."""
        result = construct_aggregation('daily', 'perc-25')
        assert result == 'aggregate(daily~perc-25)'


class TestValidateAggregationType:
    """Tests for validate_aggregation_type()."""

    @pytest.mark.parametrize('agg_type', VALID_AGGREGATION_TYPES)
    def test_valid_aggregation_types(self, agg_type):
        """Test that all valid aggregation types pass validation."""
        assert validate_aggregation_type(agg_type) is True

    def test_invalid_aggregation_type_raises(self):
        """Test that an invalid aggregation type raises AssertionError."""
        with pytest.raises(AssertionError):
            validate_aggregation_type('invalid')

    def test_percentile_aggregation_type(self):
        """Test that percentile types are validated correctly."""
        assert validate_aggregation_type('perc-50') is True

    def test_invalid_percentile_raises(self):
        """Test that invalid percentile raises AssertionError."""
        with pytest.raises(AssertionError):
            validate_aggregation_type('perc-0')

    def test_percentile_out_of_range_raises(self):
        """Test that percentile out of range raises AssertionError."""
        with pytest.raises(AssertionError):
            validate_aggregation_type('perc-100')


class TestValidateInterval:
    """Tests for validate_interval()."""

    @pytest.mark.parametrize('interval', VALID_INTERVALS)
    def test_valid_intervals(self, interval):
        """Test that all valid intervals pass validation."""
        assert validate_interval(interval) is True

    def test_custom_interval_hhmmss(self):
        """Test that a custom HHMMSS interval passes validation."""
        assert validate_interval('010000') is True

    def test_invalid_interval_raises(self):
        """Test that an invalid interval raises AssertionError."""
        with pytest.raises(AssertionError):
            validate_interval('invalid')


class TestValidateCustomInterval:
    """Tests for validate_custom_interval()."""

    def test_valid_custom_interval(self):
        """Test valid HHMMSS custom interval."""
        assert validate_custom_interval('010000') is True

    def test_wrong_length_raises(self):
        """Test that interval with wrong length raises AssertionError."""
        with pytest.raises(AssertionError):
            validate_custom_interval('12345')

    def test_non_digit_raises(self):
        """Test that non-digit interval raises AssertionError."""
        with pytest.raises(AssertionError):
            validate_custom_interval('12ab56')

    def test_hours_out_of_range_raises(self):
        """Test that hours >= 24 raises AssertionError."""
        with pytest.raises(AssertionError):
            validate_custom_interval('250000')

    def test_minutes_out_of_range_raises(self):
        """Test that minutes >= 60 raises AssertionError."""
        with pytest.raises(AssertionError):
            validate_custom_interval('006100')

    def test_seconds_out_of_range_raises(self):
        """Test that seconds >= 60 raises AssertionError."""
        with pytest.raises(AssertionError):
            validate_custom_interval('000061')


class TestValidatePercentile:
    """Tests for validate_percentile()."""

    def test_valid_percentile(self):
        """Test valid percentile values."""
        assert validate_percentile('perc-50') is True
        assert validate_percentile('perc-1') is True
        assert validate_percentile('perc-99') is True

    def test_zero_percentile_raises(self):
        """Test that perc-0 raises AssertionError."""
        with pytest.raises(AssertionError):
            validate_percentile('perc-0')

    def test_100_percentile_raises(self):
        """Test that perc-100 raises AssertionError."""
        with pytest.raises(AssertionError):
            validate_percentile('perc-100')

    def test_non_perc_prefix_raises(self):
        """Test that non-perc prefix raises AssertionError."""
        with pytest.raises(AssertionError):
            validate_percentile('percent-50')


# ---------------------------------------------------------------------------
# Full URL construction – end-to-end through Service.url()
# ---------------------------------------------------------------------------

class TestFullUrlConstruction:
    """End-to-end tests verifying the complete URL produced by Service.url()
    for the argument dicts that each public function would build.

    Each test includes a TODO placeholder for the exact expected URL string.
    Fill in `expected_url` with the correct value for your environment.
    """

    def setup_method(self):
        self.service = Service()

    def test_get_ts_url_construction(self):
        """Test the full URL for a getTimeseriesValues request."""
        args = {
            'request': 'getTimeseriesValues',
            'ts_id': '424663010',
            'from': '2020-01-01',
            'to': '2020-12-31',
            'returnfields': ['Timestamp', 'Value', 'Quality Code', 'Quality Code Name'],
            'metadata': 'true',
            'md_returnfields': ['ts_unitsymbol', 'ts_name', 'ts_id',
                                'station_no', 'station_name',
                                'station_latitude', 'station_longitude',
                                'parametertype_id', 'parametertype_name',
                                'stationparameter_no', 'stationparameter_name'],
            'timezone': 'GMT-6',
            'ca_sta_returnfields': ['stn_HUC12', 'stn_EQuIS_ID'],
        }
        url = self.service.url(args)

        expected_url = (
            'https://wiskiweb01.pca.state.mn.us/KiWIS/KiWIS?'
            'datasource=0&service=kisters&type=queryServices&format=json'
            '&request=getTimeseriesValues'
            '&ts_id=424663010'
            '&from=2020-01-01'
            '&to=2020-12-31'
            '&returnfields=Timestamp,Value,Quality Code,Quality Code Name'
            '&metadata=true'
            '&md_returnfields=ts_unitsymbol,ts_name,ts_id,station_no,station_name,'
            'station_latitude,station_longitude,parametertype_id,parametertype_name,'
            'stationparameter_no,stationparameter_name'
            '&timezone=GMT-6'
            '&ca_sta_returnfields=stn_HUC12,stn_EQuIS_ID'
        )
        assert url == expected_url

    def test_get_stations_url_construction(self):
        """Test the full URL for a getStationList request."""
        args = {
            'request': 'getStationList',
            'stationparameter_no': None,
            'stationgroup_id': None,
            'parametertype_id': None,
            'station_no': ['H67014001'],
            'returnfields': ['ca_sta', 'station_no', 'station_name'],
            'ca_sta_returnfields': ['stn_HUC12', 'stn_EQuIS_ID', 'stn_AUID',
                                    'hydrounit_title', 'hydrounit_no', 'NearestTown'],
        }
        url = self.service.url(args)

        expected_url = (
            'https://wiskiweb01.pca.state.mn.us/KiWIS/KiWIS?'
            'datasource=0&service=kisters&type=queryServices&format=json'
            '&request=getStationList'
            '&station_no=H67014001'
            '&returnfields=ca_sta,station_no,station_name'
            '&ca_sta_returnfields=stn_HUC12,stn_EQuIS_ID,stn_AUID,'
            'hydrounit_title,hydrounit_no,NearestTown'
        )
        assert url == expected_url

    def test_get_ts_ids_url_construction(self):
        """Test the full URL for a getTimeseriesList request."""
        args = {
            'request': 'getTimeseriesList',
            'station_no': ['H67014001'],
            'ts_id': None,
            'parametertype_id': None,
            'stationparameter_no': None,
            'ts_name': None,
            'returnfields': ['ts_id', 'ts_name', 'ca_sta', 'station_no',
                             'ts_unitsymbol',
                             'parametertype_id', 'parametertype_name',
                             'station_latitude', 'station_longitude',
                             'stationparameter_no', 'stationparameter_name',
                             'station_no', 'station_name',
                             'coverage', 'ts_density'],
            'ca_sta_returnfields': ['stn_HUC12', 'stn_EQuIS_ID', 'stn_AUID'],
        }
        url = self.service.url(args)

        expected_url = (
            'https://wiskiweb01.pca.state.mn.us/KiWIS/KiWIS?'
            'datasource=0&service=kisters&type=queryServices&format=json'
            '&request=getTimeseriesList'
            '&station_no=H67014001'
            '&returnfields=ts_id,ts_name,ca_sta,station_no,ts_unitsymbol,'
            'parametertype_id,parametertype_name,station_latitude,station_longitude,'
            'stationparameter_no,stationparameter_name,station_no,station_name,'
            'coverage,ts_density'
            '&ca_sta_returnfields=stn_HUC12,stn_EQuIS_ID,stn_AUID'
        )
        assert url == expected_url


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
