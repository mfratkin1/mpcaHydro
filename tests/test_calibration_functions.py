# -*- coding: utf-8 -*-
"""Tests for the calibration_functions procedural module."""

import tempfile
from pathlib import Path
import pytest

from mpcaHydro.calibration_functions import (
    create_config,
    load_config_from_file,
    save_config_to_file,
    load_from_db,
    save_to_db,
    add_location,
    get_location_by_id,
    get_location_by_name,
    get_all_stations,
    get_all_station_ids,
    get_all_reach_ids,
    stations_to_dataframe,
    locations_to_dataframe,
    config_to_dict,
    config_from_dict,
)

from mpcaHydro.calibration_dataclasses import (
    Station,
    Location,
    Observation,
    Metric,
)


class TestCreateConfig:
    """Tests for config creation functions."""

    def test_create_config(self):
        """Test creating a new configuration."""
        config = create_config('TestRepo')
        assert config.repository_name == 'TestRepo'
        assert config.version == '1.0'
        assert len(config.locations) == 0

    def test_create_config_with_version(self):
        """Test creating configuration with custom version."""
        config = create_config('TestRepo', version='2.0')
        assert config.version == '2.0'


class TestAddLocation:
    """Tests for location management functions."""

    def test_add_location(self):
        """Test adding a location to config."""
        config = create_config('TestRepo')
        location = Location(
            location_id=1,
            location_name='Test Location',
            repository_name='TestRepo',
            reach_ids=[100, 101],
            stations=[]
        )
        result = add_location(config, location)
        assert len(result.locations) == 1
        assert result.locations[0].location_name == 'Test Location'

    def test_get_location_by_id(self):
        """Test getting location by ID."""
        config = create_config('TestRepo')
        location = Location(
            location_id=42,
            location_name='Location 42',
            repository_name='TestRepo',
            stations=[]
        )
        add_location(config, location)
        found = get_location_by_id(config, 42)
        assert found is not None
        assert found.location_name == 'Location 42'

    def test_get_location_by_id_not_found(self):
        """Test getting non-existent location by ID."""
        config = create_config('TestRepo')
        found = get_location_by_id(config, 999)
        assert found is None

    def test_get_location_by_name(self):
        """Test getting location by name."""
        config = create_config('TestRepo')
        location = Location(
            location_id=1,
            location_name='Named Location',
            repository_name='TestRepo',
            stations=[]
        )
        add_location(config, location)
        found = get_location_by_name(config, 'Named Location')
        assert found is not None
        assert found.location_id == 1


class TestStationFunctions:
    """Tests for station-related functions."""

    def test_get_all_stations(self):
        """Test getting all stations from config."""
        config = create_config('TestRepo')
        station1 = Station(station_id='S1', station_origin='wiski', repository_name='TestRepo')
        station2 = Station(station_id='S2', station_origin='equis', repository_name='TestRepo')
        location = Location(
            location_id=1,
            location_name='Test',
            repository_name='TestRepo',
            stations=[station1, station2]
        )
        add_location(config, location)
        stations = get_all_stations(config)
        assert len(stations) == 2

    def test_get_all_station_ids(self):
        """Test getting all station IDs."""
        config = create_config('TestRepo')
        station1 = Station(station_id='ABC', station_origin='wiski', repository_name='TestRepo')
        station2 = Station(station_id='DEF', station_origin='equis', repository_name='TestRepo')
        location = Location(
            location_id=1,
            location_name='Test',
            repository_name='TestRepo',
            stations=[station1, station2]
        )
        add_location(config, location)
        ids = get_all_station_ids(config)
        assert 'ABC' in ids
        assert 'DEF' in ids

    def test_get_all_reach_ids(self):
        """Test getting all reach IDs from config."""
        config = create_config('TestRepo')
        loc1 = Location(
            location_id=1,
            location_name='Loc1',
            repository_name='TestRepo',
            reach_ids=[100, 101],
            stations=[]
        )
        loc2 = Location(
            location_id=2,
            location_name='Loc2',
            repository_name='TestRepo',
            reach_ids=[101, 102],
            stations=[]
        )
        add_location(config, loc1)
        add_location(config, loc2)
        reach_ids = get_all_reach_ids(config)
        assert set(reach_ids) == {100, 101, 102}


class TestDataFrameFunctions:
    """Tests for DataFrame conversion functions."""

    def test_stations_to_dataframe(self):
        """Test converting stations to DataFrame."""
        config = create_config('TestRepo')
        station = Station(
            station_id='S1',
            station_origin='wiski',
            repository_name='TestRepo',
            true_reach_id=650,
            comments='Test'
        )
        location = Location(
            location_id=1,
            location_name='Test',
            repository_name='TestRepo',
            stations=[station]
        )
        add_location(config, location)
        df = stations_to_dataframe(config)
        assert len(df) == 1
        assert 'station_id' in df.columns
        assert df.iloc[0]['station_id'] == 'S1'

    def test_locations_to_dataframe(self):
        """Test converting locations to DataFrame."""
        config = create_config('TestRepo')
        location = Location(
            location_id=1,
            location_name='Test Location',
            repository_name='TestRepo',
            reach_ids=[100, 101],
            upstream_reach_ids=[99],
            flow_station_ids=['F1'],
            stations=[]
        )
        add_location(config, location)
        df = locations_to_dataframe(config)
        assert len(df) == 1
        assert 'location_name' in df.columns
        assert df.iloc[0]['location_name'] == 'Test Location'


class TestSerializationFunctions:
    """Tests for dict conversion functions."""

    def test_config_to_dict(self):
        """Test converting config to dict."""
        config = create_config('TestRepo', version='1.5')
        d = config_to_dict(config)
        assert d['repository_name'] == 'TestRepo'
        assert d['version'] == '1.5'

    def test_config_from_dict(self):
        """Test creating config from dict."""
        d = {
            'repository_name': 'FromDict',
            'locations': [],
            'default_metrics': [],
            'general_constraints': [],
            'version': '2.0'
        }
        config = config_from_dict(d)
        assert config.repository_name == 'FromDict'
        assert config.version == '2.0'

    def test_config_round_trip_dict(self):
        """Test config round-trip through dict."""
        original = create_config('RoundTrip')
        location = Location(
            location_id=1,
            location_name='Test',
            repository_name='RoundTrip',
            stations=[]
        )
        add_location(original, location)
        d = config_to_dict(original)
        restored = config_from_dict(d)
        assert restored.repository_name == original.repository_name
        assert len(restored.locations) == 1


class TestFileIO:
    """Tests for file I/O functions."""

    def test_save_and_load_json(self):
        """Test saving and loading JSON config."""
        config = create_config('TestRepo')
        station = Station(station_id='S1', station_origin='wiski', repository_name='TestRepo')
        location = Location(
            location_id=1,
            location_name='Test',
            repository_name='TestRepo',
            stations=[station]
        )
        add_location(config, location)

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / 'config.json'
            save_config_to_file(config, filepath)
            loaded = load_config_from_file(filepath)
            assert loaded.repository_name == 'TestRepo'
            assert len(loaded.locations) == 1


class TestDatabaseIO:
    """Tests for database I/O functions."""

    def test_save_and_load_db(self):
        """Test saving and loading config from database."""
        config = create_config('DBTest')
        station = Station(station_id='S1', station_origin='wiski', repository_name='DBTest')
        location = Location(
            location_id=1,
            location_name='DB Location',
            repository_name='DBTest',
            stations=[station]
        )
        add_location(config, location)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.sqlite'
            save_to_db(config, db_path)
            loaded = load_from_db(db_path, 'DBTest')
            assert loaded.repository_name == 'DBTest'
            assert len(loaded.locations) == 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
