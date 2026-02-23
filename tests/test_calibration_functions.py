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


class TestCreateLocation:
    """Tests for create_location function."""

    def test_create_location_basic(self):
        """Test creating a location with stations."""
        from mpcaHydro.calibration_functions import create_location
        location = create_location(
            location_id=1,
            location_name='Test Location',
            repository_name='TestRepo',
            reach_ids=[100, 101],
            station_ids=['A', 'B']
        )
        assert location.location_id == 1
        assert location.location_name == 'Test Location'
        assert location.reach_ids == [100, 101]
        assert len(location.stations) == 2
        assert location.stations[0].station_id == 'A'
        assert location.stations[1].station_id == 'B'
        assert all(s.station_origin == 'wiski' for s in location.stations)

    def test_create_location_with_origin(self):
        """Test creating a location with custom station origin."""
        from mpcaHydro.calibration_functions import create_location
        location = create_location(
            location_id=1,
            location_name='Test',
            repository_name='TestRepo',
            reach_ids=[100],
            station_ids=['A'],
            station_origin='equis'
        )
        assert location.stations[0].station_origin == 'equis'


class TestCreateConfigFromRecords:
    """Tests for create_config_from_records function."""

    def test_basic_records(self):
        """Test creating config from basic records."""
        from mpcaHydro.calibration_functions import create_config_from_records
        records = [
            {'location_name': 'loc1', 'reach_id': 1, 'station_id': 'A'},
            {'location_name': 'loc1', 'reach_id': 2, 'station_id': 'A'},
            {'location_name': 'loc2', 'reach_id': 50, 'station_id': 'B'},
            {'location_name': 'loc3', 'reach_id': 30, 'station_id': 'C'},
            {'location_name': 'loc3', 'reach_id': 30, 'station_id': 'D'},
        ]
        config = create_config_from_records(records, 'TestRepo')
        
        assert config.repository_name == 'TestRepo'
        assert len(config.locations) == 3
        
        # Check loc1: 2 reaches, 1 station
        loc1 = config.get_location_by_name('loc1')
        assert loc1 is not None
        assert set(loc1.reach_ids) == {1, 2}
        assert len(loc1.stations) == 1
        assert loc1.stations[0].station_id == 'A'
        
        # Check loc2: 1 reach, 1 station
        loc2 = config.get_location_by_name('loc2')
        assert loc2 is not None
        assert loc2.reach_ids == [50]
        assert len(loc2.stations) == 1
        
        # Check loc3: 1 reach, 2 stations
        loc3 = config.get_location_by_name('loc3')
        assert loc3 is not None
        assert loc3.reach_ids == [30]
        assert len(loc3.stations) == 2
        station_ids = [s.station_id for s in loc3.stations]
        assert 'C' in station_ids
        assert 'D' in station_ids

    def test_with_station_origin(self):
        """Test records with station origin column."""
        from mpcaHydro.calibration_functions import create_config_from_records
        records = [
            {'location_name': 'loc1', 'reach_id': 1, 'station_id': 'A', 'origin': 'wiski'},
            {'location_name': 'loc1', 'reach_id': 1, 'station_id': 'B', 'origin': 'equis'},
        ]
        config = create_config_from_records(
            records, 
            'TestRepo',
            station_origin_col='origin'
        )
        
        loc = config.locations[0]
        origins = {s.station_id: s.station_origin for s in loc.stations}
        assert origins['A'] == 'wiski'
        assert origins['B'] == 'equis'

    def test_custom_column_names(self):
        """Test records with custom column names."""
        from mpcaHydro.calibration_functions import create_config_from_records
        records = [
            {'loc': 'L1', 'rch': 100, 'stn': 'X'},
        ]
        config = create_config_from_records(
            records,
            'TestRepo',
            location_col='loc',
            reach_col='rch',
            station_col='stn'
        )
        assert len(config.locations) == 1
        assert config.locations[0].location_name == 'L1'
        assert config.locations[0].reach_ids == [100]
        assert config.locations[0].stations[0].station_id == 'X'


class TestCreateConfigFromDataFrame:
    """Tests for create_config_from_dataframe function."""

    def test_from_dataframe(self):
        """Test creating config from DataFrame."""
        import pandas as pd
        from mpcaHydro.calibration_functions import create_config_from_dataframe
        
        df = pd.DataFrame([
            {'location_name': 'loc1', 'reach_id': 1, 'station_id': 'A'},
            {'location_name': 'loc1', 'reach_id': 2, 'station_id': 'A'},
            {'location_name': 'loc2', 'reach_id': 50, 'station_id': 'B'},
            {'location_name': 'loc3', 'reach_id': 30, 'station_id': 'C'},
            {'location_name': 'loc3', 'reach_id': 30, 'station_id': 'D'},
        ])
        
        config = create_config_from_dataframe(df, 'TestRepo')
        
        assert config.repository_name == 'TestRepo'
        assert len(config.locations) == 3
        
        loc1 = config.get_location_by_name('loc1')
        assert set(loc1.reach_ids) == {1, 2}
        assert len(loc1.stations) == 1
        
        loc3 = config.get_location_by_name('loc3')
        assert len(loc3.stations) == 2


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
