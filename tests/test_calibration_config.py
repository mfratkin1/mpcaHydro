# -*- coding: utf-8 -*-
"""
Tests for the calibration_config module.

This module tests the calibration configuration system including:
- Data classes (Location, Station, Observation, Metric, etc.)
- Configuration file loading/saving (YAML, JSON, TOML)
- Database integration
"""

import json
import tempfile
from pathlib import Path
import pytest

# Import the module under test
from mpcaHydro.calibration_config import (
    Metric,
    Observation,
    LandcoverConstraint,
    WatershedConstraint,
    GeneralConstraint,
    Station,
    Location,
    CalibrationConfig,
    CalibrationManager,
    get_default_timeseries_metrics,
    get_default_discrete_metrics,
    load_config,
    save_config,
    create_example_config,
    init_calibration_db,
    save_config_to_db,
    load_config_from_db,
)


class TestMetric:
    """Tests for the Metric data class."""
    
    def test_metric_creation(self):
        """Test creating a Metric instance."""
        metric = Metric(name='NSE', target=0.5, weight=1.0, enabled=True)
        assert metric.name == 'NSE'
        assert metric.target == 0.5
        assert metric.weight == 1.0
        assert metric.enabled is True
    
    def test_metric_defaults(self):
        """Test Metric default values."""
        metric = Metric(name='NSE')
        assert metric.target is None
        assert metric.weight == 1.0
        assert metric.enabled is True
    
    def test_metric_to_dict(self):
        """Test Metric serialization to dict."""
        metric = Metric(name='NSE', target=0.5, weight=1.5, enabled=False)
        d = metric.to_dict()
        assert d['name'] == 'NSE'
        assert d['target'] == 0.5
        assert d['weight'] == 1.5
        assert d['enabled'] is False
    
    def test_metric_from_dict(self):
        """Test Metric deserialization from dict."""
        d = {'name': 'Pbias', 'target': 10.0, 'weight': 2.0, 'enabled': True}
        metric = Metric.from_dict(d)
        assert metric.name == 'Pbias'
        assert metric.target == 10.0
        assert metric.weight == 2.0
        assert metric.enabled is True


class TestObservation:
    """Tests for the Observation data class (includes constituent config)."""
    
    def test_observation_creation(self):
        """Test creating an Observation instance."""
        metrics = [Metric(name='NSE', target=0.5)]
        observation = Observation(
            constituent='Q',
            start_year=2000,
            end_year=2023,
            total_samples=8760,
            metrics=metrics,
            derived_from=[]
        )
        assert observation.constituent == 'Q'
        assert observation.start_year == 2000
        assert observation.end_year == 2023
        assert observation.total_samples == 8760
        assert len(observation.metrics) == 1
        assert observation.derived_from == []
    
    def test_derived_observation(self):
        """Test creating a derived observation."""
        observation = Observation(
            constituent='TP_load',
            metrics=[Metric(name='Pbias', target=30.0)],
            derived_from=['TP', 'Q']
        )
        assert observation.constituent == 'TP_load'
        assert 'TP' in observation.derived_from
        assert 'Q' in observation.derived_from
    
    def test_observation_round_trip(self):
        """Test serialization round-trip."""
        original = Observation(
            constituent='TSS',
            start_year=2005,
            end_year=2020,
            avg_samples_per_year=12.0,
            metrics=[Metric(name='Pbias', target=25.0)],
            derived_from=[]
        )
        d = original.to_dict()
        restored = Observation.from_dict(d)
        assert restored.constituent == original.constituent
        assert restored.start_year == original.start_year
        assert len(restored.metrics) == len(original.metrics)


class TestStation:
    """Tests for the Station data class."""
    
    def test_station_creation(self):
        """Test creating a Station instance."""
        station = Station(
            station_id='E66050001',
            station_origin='wiski',
            repository_name='Clearwater',
            true_reach_id=650
        )
        assert station.station_id == 'E66050001'
        assert station.station_origin == 'wiski'
        assert station.true_reach_id == 650
    
    def test_station_round_trip(self):
        """Test serialization round-trip."""
        original = Station(
            station_id='S002-118',
            station_origin='equis',
            repository_name='Clearwater',
            true_reach_id=650,
            comments='Test station'
        )
        d = original.to_dict()
        restored = Station.from_dict(d)
        assert restored.station_id == original.station_id
        assert restored.comments == original.comments


class TestLocation:
    """Tests for the Location data class."""
    
    def test_location_creation(self):
        """Test creating a Location instance."""
        station = Station(
            station_id='E66050001',
            station_origin='wiski',
            repository_name='Clearwater'
        )
        location = Location(
            location_id=1,
            location_name='Clearwater Outlet',
            repository_name='Clearwater',
            reach_ids=[650],
            stations=[station]
        )
        assert location.location_id == 1
        assert location.location_name == 'Clearwater Outlet'
        assert len(location.stations) == 1
        assert 650 in location.reach_ids
    
    def test_get_all_reach_ids(self):
        """Test getting all reach IDs from a location."""
        location = Location(
            location_id=1, location_name='Test',
            repository_name='Test',
            reach_ids=[100, 101, 102],
            stations=[]
        )
        reach_ids = location.get_all_reach_ids()
        assert set(reach_ids) == {100, 101, 102}
    
    def test_get_all_station_ids(self):
        """Test getting all station IDs from a location."""
        station1 = Station(
            station_id='S1', station_origin='wiski',
            repository_name='Test'
        )
        station2 = Station(
            station_id='S2', station_origin='equis',
            repository_name='Test'
        )
        location = Location(
            location_id=1, location_name='Test',
            repository_name='Test', stations=[station1, station2]
        )
        station_ids = location.get_all_station_ids()
        assert station_ids == ['S1', 'S2']


class TestCalibrationConfig:
    """Tests for the CalibrationConfig data class."""
    
    def test_config_creation(self):
        """Test creating a CalibrationConfig instance."""
        config = CalibrationConfig(
            repository_name='Clearwater',
            version='1.0'
        )
        assert config.repository_name == 'Clearwater'
        assert config.version == '1.0'
        assert len(config.locations) == 0
    
    def test_get_location_by_id(self):
        """Test getting a location by ID."""
        location = Location(
            location_id=1, location_name='Test',
            repository_name='Test', stations=[]
        )
        config = CalibrationConfig(
            repository_name='Test',
            locations=[location]
        )
        result = config.get_location_by_id(1)
        assert result is not None
        assert result.location_name == 'Test'
        
        # Test non-existent location
        assert config.get_location_by_id(999) is None
    
    def test_get_all_stations(self):
        """Test getting all stations from config."""
        station1 = Station(
            station_id='S1', station_origin='wiski',
            repository_name='Test'
        )
        station2 = Station(
            station_id='S2', station_origin='equis',
            repository_name='Test'
        )
        location1 = Location(
            location_id=1, location_name='Loc1',
            repository_name='Test', stations=[station1]
        )
        location2 = Location(
            location_id=2, location_name='Loc2',
            repository_name='Test', stations=[station2]
        )
        config = CalibrationConfig(
            repository_name='Test',
            locations=[location1, location2]
        )
        stations = config.get_all_stations()
        assert len(stations) == 2


class TestDefaultMetrics:
    """Tests for default metric functions."""
    
    def test_default_timeseries_metrics(self):
        """Test getting default timeseries metrics."""
        metrics = get_default_timeseries_metrics()
        metric_names = [m.name for m in metrics]
        assert 'NSE' in metric_names
        assert 'logNSE' in metric_names
        assert 'Pbias' in metric_names
    
    def test_default_discrete_metrics(self):
        """Test getting default discrete metrics."""
        metrics = get_default_discrete_metrics()
        metric_names = [m.name for m in metrics]
        assert 'Pbias' in metric_names
        assert 'monthly_average' in metric_names


class TestExampleConfig:
    """Tests for example configuration creation."""
    
    def test_create_example_config(self):
        """Test creating an example configuration."""
        config = create_example_config('TestRepo')
        assert config.repository_name == 'TestRepo'
        assert len(config.locations) == 1
        assert len(config.locations[0].stations) == 2


class TestConfigFileSerialization:
    """Tests for configuration file loading and saving."""
    
    def test_save_and_load_json(self):
        """Test saving and loading JSON configuration."""
        config = create_example_config('TestRepo')
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / 'config.json'
            save_config(config, filepath)
            
            assert filepath.exists()
            
            loaded = load_config(filepath)
            assert loaded.repository_name == config.repository_name
            assert len(loaded.locations) == len(config.locations)
    
    def test_save_and_load_yaml(self):
        """Test saving and loading YAML configuration."""
        config = create_example_config('TestRepo')
        
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / 'config.yaml'
            save_config(config, filepath)
            
            assert filepath.exists()
            
            loaded = load_config(filepath)
            assert loaded.repository_name == config.repository_name
            assert len(loaded.locations) == len(config.locations)
            
            # Check that stations and observations are preserved
            station = loaded.locations[0].stations[0]
            assert station.station_id == 'E66050001'
            assert len(station.observations) > 0
    
    def test_load_nonexistent_file(self):
        """Test loading a non-existent file raises error."""
        with pytest.raises(FileNotFoundError):
            load_config('/nonexistent/path/config.json')
    
    def test_load_unsupported_format(self):
        """Test loading unsupported format raises error."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / 'config.txt'
            filepath.write_text('test')
            with pytest.raises(ValueError):
                load_config(filepath)


class TestDatabaseIntegration:
    """Tests for database integration."""
    
    @pytest.mark.skip(reason="Database integration requires outlets module with data files")
    def test_init_calibration_db(self):
        """Test initializing calibration database schema."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            init_calibration_db(db_path)
            assert db_path.exists()
    
    @pytest.mark.skip(reason="Database integration requires outlets module with data files")
    def test_save_and_load_from_db(self):
        """Test saving and loading configuration from database."""
        config = create_example_config('TestRepo')
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            init_calibration_db(db_path)
            save_config_to_db(config, db_path)
            
            loaded = load_config_from_db(db_path, 'TestRepo')
            assert loaded.repository_name == config.repository_name
            assert len(loaded.locations) == len(config.locations)


class TestCalibrationManager:
    """Tests for the CalibrationManager class."""
    
    def test_manager_creation(self):
        """Test creating a CalibrationManager."""
        manager = CalibrationManager(repository_name='Test')
        assert manager.repository_name == 'Test'
    
    def test_manager_create_example(self):
        """Test creating example config through manager."""
        manager = CalibrationManager(repository_name='Test')
        config = manager.create_example_config()
        assert config.repository_name == 'Test'
        assert len(config.locations) == 1
    
    def test_manager_get_stations_as_dataframe(self):
        """Test getting stations as DataFrame."""
        manager = CalibrationManager(repository_name='Test')
        manager.create_example_config()
        df = manager.get_stations_as_dataframe()
        assert len(df) == 2
        assert 'station_id' in df.columns
    
    def test_manager_get_locations_as_dataframe(self):
        """Test getting locations as DataFrame."""
        manager = CalibrationManager(repository_name='Test')
        manager.create_example_config()
        df = manager.get_locations_as_dataframe()
        assert len(df) == 1
        assert 'location_name' in df.columns


class TestWatershedConstraints:
    """Tests for watershed and landcover constraints."""
    
    def test_landcover_constraint(self):
        """Test creating a landcover constraint."""
        constraint = LandcoverConstraint(
            landcover_type='forest',
            constituent='TP',
            target_rate=0.1,
            min_rate=0.05,
            max_rate=0.2
        )
        assert constraint.landcover_type == 'forest'
        assert constraint.target_rate == 0.1
    
    def test_watershed_constraint(self):
        """Test creating a watershed constraint."""
        lc = LandcoverConstraint(
            landcover_type='agricultural',
            constituent='TP',
            target_rate=0.8
        )
        constraint = WatershedConstraint(
            constituent='TP',
            target_rate=0.5,
            landcover_constraints=[lc]
        )
        assert constraint.constituent == 'TP'
        assert len(constraint.landcover_constraints) == 1
    
    def test_constraint_round_trip(self):
        """Test serialization round-trip for constraints."""
        lc = LandcoverConstraint(
            landcover_type='urban',
            constituent='TSS',
            target_rate=100.0,
            min_rate=50.0,
            max_rate=200.0
        )
        ws = WatershedConstraint(
            constituent='TSS',
            target_rate=150.0,
            landcover_constraints=[lc]
        )
        
        d = ws.to_dict()
        restored = WatershedConstraint.from_dict(d)
        
        assert restored.constituent == ws.constituent
        assert len(restored.landcover_constraints) == 1
        assert restored.landcover_constraints[0].landcover_type == 'urban'


class TestGeneralConstraint:
    """Tests for the GeneralConstraint data class."""
    
    def test_general_constraint_creation(self):
        """Test creating a GeneralConstraint instance."""
        constraint = GeneralConstraint(
            name='catchment_loading',
            constraint_type='loading_rate',
            parameters={'max_rate': 1.5},
            enabled=True,
            notes='Test constraint'
        )
        assert constraint.name == 'catchment_loading'
        assert constraint.constraint_type == 'loading_rate'
        assert constraint.parameters['max_rate'] == 1.5
    
    def test_general_constraint_round_trip(self):
        """Test serialization round-trip."""
        original = GeneralConstraint(
            name='surface_runoff',
            constraint_type='runoff_rate',
            parameters={'min_rate': 0.1, 'max_rate': 0.5},
            enabled=False,
            notes='Placeholder constraint'
        )
        d = original.to_dict()
        restored = GeneralConstraint.from_dict(d)
        
        assert restored.name == original.name
        assert restored.constraint_type == original.constraint_type
        assert restored.parameters == original.parameters
        assert restored.enabled == original.enabled


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
