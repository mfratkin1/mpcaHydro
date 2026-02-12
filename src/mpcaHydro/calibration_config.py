# -*- coding: utf-8 -*-
"""
Calibration Configuration Module

This module provides a systematic way of setting up calibration locations for HSPF hydrologic models.
It defines the following entities:
- Location: A grouping of one or more stations (e.g., stations with insufficient individual data)
- Station: A monitoring station with metadata and linkage to model reaches
- Observation: Observation metadata including constituents, date ranges, sample counts
- Metric: Metrics to calculate (NSE, logNSE, Pbias, etc.) with targets
- Constraint: Loading rate constraints for watersheds and landcovers

Users can pass in configuration files (YAML or JSON) to customize the calibration setup.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union
import json

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

import duckdb
import pandas as pd


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class Metric:
    """
    Represents a metric to be calculated during calibration.
    
    Attributes:
        name: Name of the metric (e.g., 'NSE', 'logNSE', 'Pbias')
        target: Target value for the metric
        weight: Optional weight for multi-objective optimization
        enabled: Whether this metric is active
    """
    name: str
    target: Optional[float] = None
    weight: float = 1.0
    enabled: bool = True

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'target': self.target,
            'weight': self.weight,
            'enabled': self.enabled
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Metric':
        return cls(
            name=data['name'],
            target=data.get('target'),
            weight=data.get('weight', 1.0),
            enabled=data.get('enabled', True)
        )


@dataclass
class ConstituentConfig:
    """
    Configuration for a constituent at a station.
    
    Attributes:
        name: Constituent name (e.g., 'Q', 'TSS', 'TP')
        metrics: List of metrics to calculate for this constituent
        derived_from: Optional list of other constituents used to derive this one
                     (e.g., load derived from flow and concentration)
    """
    name: str
    metrics: list[Metric] = field(default_factory=list)
    derived_from: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'metrics': [m.to_dict() for m in self.metrics],
            'derived_from': self.derived_from
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'ConstituentConfig':
        metrics = [Metric.from_dict(m) for m in data.get('metrics', [])]
        return cls(
            name=data['name'],
            metrics=metrics,
            derived_from=data.get('derived_from', [])
        )


@dataclass
class ObservationSummary:
    """
    Summary of observation data availability for a constituent at a station.
    
    Attributes:
        constituent: Constituent name
        start_year: First year with data
        end_year: Last year with data
        avg_samples_per_year: Average number of samples per calendar year
        median_samples_per_year: Median number of samples per calendar year
        years_with_data: Number of years with data collected
        total_samples: Total number of samples
    """
    constituent: str
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    avg_samples_per_year: Optional[float] = None
    median_samples_per_year: Optional[float] = None
    years_with_data: Optional[int] = None
    total_samples: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            'constituent': self.constituent,
            'start_year': self.start_year,
            'end_year': self.end_year,
            'avg_samples_per_year': self.avg_samples_per_year,
            'median_samples_per_year': self.median_samples_per_year,
            'years_with_data': self.years_with_data,
            'total_samples': self.total_samples
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'ObservationSummary':
        return cls(
            constituent=data['constituent'],
            start_year=data.get('start_year'),
            end_year=data.get('end_year'),
            avg_samples_per_year=data.get('avg_samples_per_year'),
            median_samples_per_year=data.get('median_samples_per_year'),
            years_with_data=data.get('years_with_data'),
            total_samples=data.get('total_samples')
        )


@dataclass
class LandcoverConstraint:
    """
    Loading rate constraint for a specific landcover type.
    
    Attributes:
        landcover_type: Type of landcover (e.g., 'forest', 'urban', 'agricultural')
        constituent: Constituent name for the loading rate
        target_rate: Target loading rate (units depend on constituent)
        min_rate: Minimum acceptable loading rate
        max_rate: Maximum acceptable loading rate
    """
    landcover_type: str
    constituent: str
    target_rate: Optional[float] = None
    min_rate: Optional[float] = None
    max_rate: Optional[float] = None

    def to_dict(self) -> dict:
        return {
            'landcover_type': self.landcover_type,
            'constituent': self.constituent,
            'target_rate': self.target_rate,
            'min_rate': self.min_rate,
            'max_rate': self.max_rate
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'LandcoverConstraint':
        return cls(
            landcover_type=data['landcover_type'],
            constituent=data['constituent'],
            target_rate=data.get('target_rate'),
            min_rate=data.get('min_rate'),
            max_rate=data.get('max_rate')
        )


@dataclass
class WatershedConstraint:
    """
    Loading rate constraint for the whole watershed.
    
    Attributes:
        constituent: Constituent name for the loading rate
        target_rate: Target loading rate for the whole watershed
        min_rate: Minimum acceptable loading rate
        max_rate: Maximum acceptable loading rate
        landcover_constraints: List of landcover-specific constraints
    """
    constituent: str
    target_rate: Optional[float] = None
    min_rate: Optional[float] = None
    max_rate: Optional[float] = None
    landcover_constraints: list[LandcoverConstraint] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            'constituent': self.constituent,
            'target_rate': self.target_rate,
            'min_rate': self.min_rate,
            'max_rate': self.max_rate,
            'landcover_constraints': [lc.to_dict() for lc in self.landcover_constraints]
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'WatershedConstraint':
        lc_constraints = [
            LandcoverConstraint.from_dict(lc) 
            for lc in data.get('landcover_constraints', [])
        ]
        return cls(
            constituent=data['constituent'],
            target_rate=data.get('target_rate'),
            min_rate=data.get('min_rate'),
            max_rate=data.get('max_rate'),
            landcover_constraints=lc_constraints
        )


@dataclass
class Station:
    """
    Represents a monitoring station.
    
    Attributes:
        station_id: Unique identifier for the station
        station_origin: Data source (e.g., 'wiski', 'equis')
        repository_name: Name of the model repository
        true_reach_id: The model reach the station is located on (one-to-one)
        reach_ids: The model output reaches that best map to the station (many-to-many)
        upstream_reach_ids: Optional upstream reach IDs for watershed loading calculations
        constituents: List of constituent configurations
        observation_summaries: List of observation summaries for available data
        flow_station_ids: Other station IDs that can provide supplemental flow data
        comments: Optional notes about the station
    """
    station_id: str
    station_origin: str
    repository_name: str
    true_reach_id: Optional[int] = None
    reach_ids: list[int] = field(default_factory=list)
    upstream_reach_ids: list[int] = field(default_factory=list)
    constituents: list[ConstituentConfig] = field(default_factory=list)
    observation_summaries: list[ObservationSummary] = field(default_factory=list)
    flow_station_ids: list[str] = field(default_factory=list)
    comments: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            'station_id': self.station_id,
            'station_origin': self.station_origin,
            'repository_name': self.repository_name,
            'true_reach_id': self.true_reach_id,
            'reach_ids': self.reach_ids,
            'upstream_reach_ids': self.upstream_reach_ids,
            'constituents': [c.to_dict() for c in self.constituents],
            'observation_summaries': [o.to_dict() for o in self.observation_summaries],
            'flow_station_ids': self.flow_station_ids,
            'comments': self.comments
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Station':
        constituents = [
            ConstituentConfig.from_dict(c) 
            for c in data.get('constituents', [])
        ]
        observation_summaries = [
            ObservationSummary.from_dict(o) 
            for o in data.get('observation_summaries', [])
        ]
        return cls(
            station_id=data['station_id'],
            station_origin=data['station_origin'],
            repository_name=data['repository_name'],
            true_reach_id=data.get('true_reach_id'),
            reach_ids=data.get('reach_ids', []),
            upstream_reach_ids=data.get('upstream_reach_ids', []),
            constituents=constituents,
            observation_summaries=observation_summaries,
            flow_station_ids=data.get('flow_station_ids', []),
            comments=data.get('comments')
        )


@dataclass
class Location:
    """
    Represents a calibration location (grouping of stations).
    
    A location can have one or more stations. Multiple stations may be grouped
    when individual stations have insufficient data for calibration but combined
    data is sufficient.
    
    Attributes:
        location_id: Unique identifier for the location
        location_name: Human-readable name for the location
        repository_name: Name of the model repository
        stations: List of stations at this location
        watershed_constraints: Loading rate constraints for the watershed
        notes: Optional notes about the location
    """
    location_id: int
    location_name: str
    repository_name: str
    stations: list[Station] = field(default_factory=list)
    watershed_constraints: list[WatershedConstraint] = field(default_factory=list)
    notes: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            'location_id': self.location_id,
            'location_name': self.location_name,
            'repository_name': self.repository_name,
            'stations': [s.to_dict() for s in self.stations],
            'watershed_constraints': [w.to_dict() for w in self.watershed_constraints],
            'notes': self.notes
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Location':
        stations = [Station.from_dict(s) for s in data.get('stations', [])]
        watershed_constraints = [
            WatershedConstraint.from_dict(w) 
            for w in data.get('watershed_constraints', [])
        ]
        return cls(
            location_id=data['location_id'],
            location_name=data['location_name'],
            repository_name=data['repository_name'],
            stations=stations,
            watershed_constraints=watershed_constraints,
            notes=data.get('notes')
        )

    def get_all_reach_ids(self) -> list[int]:
        """Get all reach IDs from all stations at this location."""
        reach_ids = set()
        for station in self.stations:
            reach_ids.update(station.reach_ids)
        return list(reach_ids)

    def get_all_station_ids(self) -> list[str]:
        """Get all station IDs at this location."""
        return [station.station_id for station in self.stations]


@dataclass
class CalibrationConfig:
    """
    Root configuration for calibration locations.
    
    Attributes:
        repository_name: Name of the model repository
        locations: List of calibration locations
        default_metrics: Default metrics to apply if not specified at station level
        version: Configuration version for tracking changes
    """
    repository_name: str
    locations: list[Location] = field(default_factory=list)
    default_metrics: list[Metric] = field(default_factory=list)
    version: str = "1.0"

    def to_dict(self) -> dict:
        return {
            'repository_name': self.repository_name,
            'locations': [loc.to_dict() for loc in self.locations],
            'default_metrics': [m.to_dict() for m in self.default_metrics],
            'version': self.version
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'CalibrationConfig':
        locations = [Location.from_dict(loc) for loc in data.get('locations', [])]
        default_metrics = [
            Metric.from_dict(m) 
            for m in data.get('default_metrics', [])
        ]
        return cls(
            repository_name=data['repository_name'],
            locations=locations,
            default_metrics=default_metrics,
            version=data.get('version', '1.0')
        )

    def get_location_by_id(self, location_id: int) -> Optional[Location]:
        """Get a location by its ID."""
        for loc in self.locations:
            if loc.location_id == location_id:
                return loc
        return None

    def get_location_by_name(self, location_name: str) -> Optional[Location]:
        """Get a location by its name."""
        for loc in self.locations:
            if loc.location_name == location_name:
                return loc
        return None

    def get_all_stations(self) -> list[Station]:
        """Get all stations across all locations."""
        stations = []
        for loc in self.locations:
            stations.extend(loc.stations)
        return stations


# ============================================================================
# Default Metrics Configuration
# ============================================================================

def get_default_timeseries_metrics() -> list[Metric]:
    """
    Get default metrics for timeseries observations (e.g., flow).
    """
    return [
        Metric(name='NSE', target=0.5, weight=1.0),
        Metric(name='logNSE', target=0.5, weight=1.0),
        Metric(name='Pbias', target=10.0, weight=1.0),
        Metric(name='monthly_average', target=15.0, weight=0.5),
        Metric(name='annual_average', target=15.0, weight=0.5),
        Metric(name='percentile_10', target=20.0, weight=0.5),
        Metric(name='percentile_90', target=20.0, weight=0.5),
        Metric(name='seasonal_average', target=15.0, weight=0.5),
    ]


def get_default_discrete_metrics() -> list[Metric]:
    """
    Get default metrics for discrete sample observations (e.g., water quality).
    """
    return [
        Metric(name='Pbias', target=25.0, weight=1.0),
        Metric(name='monthly_average', target=25.0, weight=0.5),
        Metric(name='seasonal_average', target=25.0, weight=0.5),
    ]


# ============================================================================
# Configuration File Loading/Saving
# ============================================================================

def load_config(filepath: Union[str, Path]) -> CalibrationConfig:
    """
    Load calibration configuration from a file.
    
    Supports YAML and JSON formats based on file extension.
    
    Args:
        filepath: Path to the configuration file
        
    Returns:
        CalibrationConfig object
        
    Raises:
        ValueError: If file format is not supported
        FileNotFoundError: If file does not exist
    """
    filepath = Path(filepath)
    
    if not filepath.exists():
        raise FileNotFoundError(f"Configuration file not found: {filepath}")
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    if filepath.suffix.lower() in ['.yaml', '.yml']:
        if not YAML_AVAILABLE:
            raise ImportError(
                "PyYAML is required to load YAML configuration files. "
                "Install it with: pip install pyyaml"
            )
        data = yaml.safe_load(content)
    elif filepath.suffix.lower() == '.json':
        data = json.loads(content)
    else:
        raise ValueError(
            f"Unsupported configuration file format: {filepath.suffix}. "
            "Supported formats: .yaml, .yml, .json"
        )
    
    return CalibrationConfig.from_dict(data)


def save_config(config: CalibrationConfig, filepath: Union[str, Path]) -> None:
    """
    Save calibration configuration to a file.
    
    Supports YAML and JSON formats based on file extension.
    
    Args:
        config: CalibrationConfig object to save
        filepath: Path to save the configuration file
        
    Raises:
        ValueError: If file format is not supported
    """
    filepath = Path(filepath)
    data = config.to_dict()
    
    # Ensure parent directory exists
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    if filepath.suffix.lower() in ['.yaml', '.yml']:
        if not YAML_AVAILABLE:
            raise ImportError(
                "PyYAML is required to save YAML configuration files. "
                "Install it with: pip install pyyaml"
            )
        with open(filepath, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
    elif filepath.suffix.lower() == '.json':
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    else:
        raise ValueError(
            f"Unsupported configuration file format: {filepath.suffix}. "
            "Supported formats: .yaml, .yml, .json"
        )


def create_example_config(repository_name: str) -> CalibrationConfig:
    """
    Create an example calibration configuration.
    
    Args:
        repository_name: Name of the model repository
        
    Returns:
        Example CalibrationConfig object
    """
    # Example station with timeseries observations (flow)
    flow_station = Station(
        station_id='E66050001',
        station_origin='wiski',
        repository_name=repository_name,
        true_reach_id=650,
        reach_ids=[650],
        upstream_reach_ids=[649, 648],
        constituents=[
            ConstituentConfig(
                name='Q',
                metrics=get_default_timeseries_metrics(),
                derived_from=[]
            ),
        ],
        observation_summaries=[
            ObservationSummary(
                constituent='Q',
                start_year=2000,
                end_year=2023,
                avg_samples_per_year=365.0,
                median_samples_per_year=365.0,
                years_with_data=24,
                total_samples=8760
            ),
        ],
        flow_station_ids=[],
        comments='Primary flow monitoring station'
    )

    # Example station with discrete sample observations (water quality)
    wq_station = Station(
        station_id='S002-118',
        station_origin='equis',
        repository_name=repository_name,
        true_reach_id=650,
        reach_ids=[650],
        upstream_reach_ids=[649, 648],
        constituents=[
            ConstituentConfig(
                name='TSS',
                metrics=get_default_discrete_metrics(),
                derived_from=[]
            ),
            ConstituentConfig(
                name='TP',
                metrics=get_default_discrete_metrics(),
                derived_from=[]
            ),
            ConstituentConfig(
                name='TP_load',
                metrics=[Metric(name='Pbias', target=30.0)],
                derived_from=['TP', 'Q']  # Load derived from concentration and flow
            ),
        ],
        observation_summaries=[
            ObservationSummary(
                constituent='TSS',
                start_year=2005,
                end_year=2020,
                avg_samples_per_year=12.0,
                median_samples_per_year=10.0,
                years_with_data=15,
                total_samples=180
            ),
            ObservationSummary(
                constituent='TP',
                start_year=2005,
                end_year=2020,
                avg_samples_per_year=12.0,
                median_samples_per_year=10.0,
                years_with_data=15,
                total_samples=180
            ),
        ],
        flow_station_ids=['E66050001'],  # Use flow from another station
        comments='Water quality monitoring station'
    )

    # Example location with multiple stations
    location = Location(
        location_id=1,
        location_name='Clearwater Outlet',
        repository_name=repository_name,
        stations=[flow_station, wq_station],
        watershed_constraints=[
            WatershedConstraint(
                constituent='TP',
                target_rate=0.5,  # lbs/acre/year
                min_rate=0.2,
                max_rate=1.0,
                landcover_constraints=[
                    LandcoverConstraint(
                        landcover_type='forest',
                        constituent='TP',
                        target_rate=0.1,
                        min_rate=0.05,
                        max_rate=0.2
                    ),
                    LandcoverConstraint(
                        landcover_type='agricultural',
                        constituent='TP',
                        target_rate=0.8,
                        min_rate=0.4,
                        max_rate=1.5
                    ),
                ]
            ),
        ],
        notes='Example calibration location combining flow and water quality stations'
    )

    return CalibrationConfig(
        repository_name=repository_name,
        locations=[location],
        default_metrics=get_default_timeseries_metrics(),
        version='1.0'
    )


# ============================================================================
# Database Schema and Integration
# ============================================================================

CALIBRATION_SCHEMA = """
-- Calibration configuration schema for DuckDB
-- Extends the outlets schema with calibration-specific tables

CREATE SCHEMA IF NOT EXISTS calibration;

-- Table: calibration_locations
-- Represents calibration locations (groupings of stations)
CREATE TABLE IF NOT EXISTS calibration.locations (
    location_id INTEGER PRIMARY KEY,
    location_name TEXT NOT NULL,
    repository_name TEXT NOT NULL,
    notes TEXT
);

-- Table: calibration_stations
-- Stations belonging to calibration locations
CREATE TABLE IF NOT EXISTS calibration.stations (
    station_id TEXT NOT NULL,
    station_origin TEXT NOT NULL,
    location_id INTEGER NOT NULL,
    repository_name TEXT NOT NULL,
    true_reach_id INTEGER,
    comments TEXT,
    CONSTRAINT pk_calibration_station PRIMARY KEY (station_id, station_origin),
    FOREIGN KEY (location_id) REFERENCES calibration.locations(location_id)
);

-- Table: calibration_station_reaches
-- Many-to-many relationship between stations and reaches
CREATE TABLE IF NOT EXISTS calibration.station_reaches (
    station_id TEXT NOT NULL,
    station_origin TEXT NOT NULL,
    reach_id INTEGER NOT NULL,
    is_upstream BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (station_id, station_origin) 
        REFERENCES calibration.stations(station_id, station_origin)
);

-- Table: calibration_flow_stations
-- Supplemental flow stations for water quality stations
CREATE TABLE IF NOT EXISTS calibration.flow_stations (
    station_id TEXT NOT NULL,
    station_origin TEXT NOT NULL,
    flow_station_id TEXT NOT NULL,
    FOREIGN KEY (station_id, station_origin) 
        REFERENCES calibration.stations(station_id, station_origin)
);

-- Table: calibration_constituents
-- Constituent configurations for stations
CREATE TABLE IF NOT EXISTS calibration.constituents (
    id INTEGER PRIMARY KEY,
    station_id TEXT NOT NULL,
    station_origin TEXT NOT NULL,
    constituent TEXT NOT NULL,
    FOREIGN KEY (station_id, station_origin) 
        REFERENCES calibration.stations(station_id, station_origin)
);

-- Table: calibration_metrics
-- Metrics to calculate for constituents
CREATE TABLE IF NOT EXISTS calibration.metrics (
    id INTEGER PRIMARY KEY,
    constituent_id INTEGER NOT NULL,
    metric_name TEXT NOT NULL,
    target DOUBLE,
    weight DOUBLE DEFAULT 1.0,
    enabled BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (constituent_id) REFERENCES calibration.constituents(id)
);

-- Table: calibration_derived_constituents
-- Derived constituents (e.g., load from flow and concentration)
CREATE TABLE IF NOT EXISTS calibration.derived_constituents (
    constituent_id INTEGER NOT NULL,
    source_constituent TEXT NOT NULL,
    FOREIGN KEY (constituent_id) REFERENCES calibration.constituents(id)
);

-- Table: calibration_watershed_constraints
-- Watershed loading rate constraints
CREATE TABLE IF NOT EXISTS calibration.watershed_constraints (
    id INTEGER PRIMARY KEY,
    location_id INTEGER NOT NULL,
    constituent TEXT NOT NULL,
    target_rate DOUBLE,
    min_rate DOUBLE,
    max_rate DOUBLE,
    FOREIGN KEY (location_id) REFERENCES calibration.locations(location_id)
);

-- Table: calibration_landcover_constraints
-- Landcover-specific loading rate constraints
CREATE TABLE IF NOT EXISTS calibration.landcover_constraints (
    id INTEGER PRIMARY KEY,
    watershed_constraint_id INTEGER NOT NULL,
    landcover_type TEXT NOT NULL,
    target_rate DOUBLE,
    min_rate DOUBLE,
    max_rate DOUBLE,
    FOREIGN KEY (watershed_constraint_id) 
        REFERENCES calibration.watershed_constraints(id)
);

-- View: calibration_station_summary
-- Summary view of stations with their locations and reaches
CREATE OR REPLACE VIEW calibration.station_summary AS
SELECT
    s.station_id,
    s.station_origin,
    s.location_id,
    l.location_name,
    s.repository_name,
    s.true_reach_id,
    s.comments,
    STRING_AGG(DISTINCT CAST(sr.reach_id AS TEXT), ',') 
        FILTER (WHERE NOT sr.is_upstream) AS reach_ids,
    STRING_AGG(DISTINCT CAST(sr.reach_id AS TEXT), ',') 
        FILTER (WHERE sr.is_upstream) AS upstream_reach_ids
FROM calibration.stations s
JOIN calibration.locations l ON s.location_id = l.location_id
LEFT JOIN calibration.station_reaches sr 
    ON s.station_id = sr.station_id AND s.station_origin = sr.station_origin
GROUP BY 
    s.station_id, s.station_origin, s.location_id, l.location_name,
    s.repository_name, s.true_reach_id, s.comments;

-- View: calibration_location_summary
-- Summary view of locations with station counts
CREATE OR REPLACE VIEW calibration.location_summary AS
SELECT
    l.location_id,
    l.location_name,
    l.repository_name,
    COUNT(DISTINCT s.station_id) AS station_count,
    STRING_AGG(DISTINCT s.station_id, ', ') AS station_ids,
    l.notes
FROM calibration.locations l
LEFT JOIN calibration.stations s ON l.location_id = s.location_id
GROUP BY l.location_id, l.location_name, l.repository_name, l.notes;
"""


def init_calibration_db(db_path: Union[str, Path], reset: bool = False):
    """
    Initialize the calibration schema in a DuckDB database.
    
    Args:
        db_path: Path to the DuckDB database
        reset: If True, drop and recreate the schema
    """
    db_path = Path(db_path)
    
    with duckdb.connect(db_path.as_posix(), read_only=False) as con:
        if reset:
            con.execute("DROP SCHEMA IF EXISTS calibration CASCADE")
        con.execute(CALIBRATION_SCHEMA)


def save_config_to_db(
    config: CalibrationConfig, 
    db_path: Union[str, Path],
    replace: bool = True
):
    """
    Save a calibration configuration to the database.
    
    Args:
        config: CalibrationConfig object to save
        db_path: Path to the DuckDB database
        replace: If True, replace existing data for the repository
    """
    db_path = Path(db_path)
    
    with duckdb.connect(db_path.as_posix(), read_only=False) as con:
        # Initialize schema if needed
        con.execute(CALIBRATION_SCHEMA)
        
        if replace:
            # Delete existing data for this repository
            con.execute(
                "DELETE FROM calibration.locations WHERE repository_name = ?",
                [config.repository_name]
            )
        
        constituent_id_counter = 1
        metric_id_counter = 1
        watershed_constraint_id_counter = 1
        landcover_constraint_id_counter = 1
        
        for location in config.locations:
            # Insert location
            con.execute(
                """INSERT INTO calibration.locations 
                   (location_id, location_name, repository_name, notes)
                   VALUES (?, ?, ?, ?)""",
                [location.location_id, location.location_name, 
                 location.repository_name, location.notes]
            )
            
            for station in location.stations:
                # Insert station
                con.execute(
                    """INSERT INTO calibration.stations
                       (station_id, station_origin, location_id, repository_name,
                        true_reach_id, comments)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    [station.station_id, station.station_origin, location.location_id,
                     station.repository_name, station.true_reach_id, station.comments]
                )
                
                # Insert reach mappings
                for reach_id in station.reach_ids:
                    con.execute(
                        """INSERT INTO calibration.station_reaches
                           (station_id, station_origin, reach_id, is_upstream)
                           VALUES (?, ?, ?, FALSE)""",
                        [station.station_id, station.station_origin, reach_id]
                    )
                
                for reach_id in station.upstream_reach_ids:
                    con.execute(
                        """INSERT INTO calibration.station_reaches
                           (station_id, station_origin, reach_id, is_upstream)
                           VALUES (?, ?, ?, TRUE)""",
                        [station.station_id, station.station_origin, reach_id]
                    )
                
                # Insert flow station references
                for flow_station_id in station.flow_station_ids:
                    con.execute(
                        """INSERT INTO calibration.flow_stations
                           (station_id, station_origin, flow_station_id)
                           VALUES (?, ?, ?)""",
                        [station.station_id, station.station_origin, flow_station_id]
                    )
                
                # Insert constituents and metrics
                for constituent_config in station.constituents:
                    con.execute(
                        """INSERT INTO calibration.constituents
                           (id, station_id, station_origin, constituent)
                           VALUES (?, ?, ?, ?)""",
                        [constituent_id_counter, station.station_id, 
                         station.station_origin, constituent_config.name]
                    )
                    
                    for metric in constituent_config.metrics:
                        con.execute(
                            """INSERT INTO calibration.metrics
                               (id, constituent_id, metric_name, target, weight, enabled)
                               VALUES (?, ?, ?, ?, ?, ?)""",
                            [metric_id_counter, constituent_id_counter, metric.name, metric.target,
                             metric.weight, metric.enabled]
                        )
                        metric_id_counter += 1
                    
                    for source in constituent_config.derived_from:
                        con.execute(
                            """INSERT INTO calibration.derived_constituents
                               (constituent_id, source_constituent)
                               VALUES (?, ?)""",
                            [constituent_id_counter, source]
                        )
                    
                    constituent_id_counter += 1
            
            # Insert watershed constraints
            for watershed_constraint in location.watershed_constraints:
                con.execute(
                    """INSERT INTO calibration.watershed_constraints
                       (id, location_id, constituent, target_rate, min_rate, max_rate)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    [watershed_constraint_id_counter, location.location_id,
                     watershed_constraint.constituent, watershed_constraint.target_rate,
                     watershed_constraint.min_rate, watershed_constraint.max_rate]
                )
                
                for lc_constraint in watershed_constraint.landcover_constraints:
                    con.execute(
                        """INSERT INTO calibration.landcover_constraints
                           (id, watershed_constraint_id, landcover_type, 
                            target_rate, min_rate, max_rate)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        [landcover_constraint_id_counter, watershed_constraint_id_counter,
                         lc_constraint.landcover_type, lc_constraint.target_rate,
                         lc_constraint.min_rate, lc_constraint.max_rate]
                    )
                    landcover_constraint_id_counter += 1
                
                watershed_constraint_id_counter += 1


def load_config_from_db(
    db_path: Union[str, Path], 
    repository_name: str
) -> CalibrationConfig:
    """
    Load a calibration configuration from the database.
    
    Args:
        db_path: Path to the DuckDB database
        repository_name: Name of the repository to load configuration for
        
    Returns:
        CalibrationConfig object
    """
    db_path = Path(db_path)
    
    with duckdb.connect(db_path.as_posix(), read_only=True) as con:
        # Load locations
        locations_df = con.execute(
            """SELECT * FROM calibration.locations WHERE repository_name = ?""",
            [repository_name]
        ).fetchdf()
        
        locations = []
        for _, loc_row in locations_df.iterrows():
            # Load stations for this location
            stations_df = con.execute(
                """SELECT * FROM calibration.stations 
                   WHERE location_id = ? AND repository_name = ?""",
                [loc_row['location_id'], repository_name]
            ).fetchdf()
            
            stations = []
            for _, sta_row in stations_df.iterrows():
                # Load reach mappings
                reaches_df = con.execute(
                    """SELECT reach_id, is_upstream FROM calibration.station_reaches
                       WHERE station_id = ? AND station_origin = ?""",
                    [sta_row['station_id'], sta_row['station_origin']]
                ).fetchdf()
                
                reach_ids = reaches_df[~reaches_df['is_upstream']]['reach_id'].tolist()
                upstream_reach_ids = reaches_df[reaches_df['is_upstream']]['reach_id'].tolist()
                
                # Load flow stations
                flow_df = con.execute(
                    """SELECT flow_station_id FROM calibration.flow_stations
                       WHERE station_id = ? AND station_origin = ?""",
                    [sta_row['station_id'], sta_row['station_origin']]
                ).fetchdf()
                flow_station_ids = flow_df['flow_station_id'].tolist()
                
                # Load constituents
                constituents_df = con.execute(
                    """SELECT * FROM calibration.constituents
                       WHERE station_id = ? AND station_origin = ?""",
                    [sta_row['station_id'], sta_row['station_origin']]
                ).fetchdf()
                
                constituents = []
                for _, const_row in constituents_df.iterrows():
                    # Load metrics for this constituent
                    metrics_df = con.execute(
                        """SELECT * FROM calibration.metrics
                           WHERE constituent_id = ?""",
                        [const_row['id']]
                    ).fetchdf()
                    
                    metrics = [
                        Metric(
                            name=m['metric_name'],
                            target=m['target'],
                            weight=m['weight'],
                            enabled=m['enabled']
                        )
                        for _, m in metrics_df.iterrows()
                    ]
                    
                    # Load derived constituents
                    derived_df = con.execute(
                        """SELECT source_constituent FROM calibration.derived_constituents
                           WHERE constituent_id = ?""",
                        [const_row['id']]
                    ).fetchdf()
                    derived_from = derived_df['source_constituent'].tolist()
                    
                    constituents.append(ConstituentConfig(
                        name=const_row['constituent'],
                        metrics=metrics,
                        derived_from=derived_from
                    ))
                
                stations.append(Station(
                    station_id=sta_row['station_id'],
                    station_origin=sta_row['station_origin'],
                    repository_name=sta_row['repository_name'],
                    true_reach_id=sta_row['true_reach_id'],
                    reach_ids=reach_ids,
                    upstream_reach_ids=upstream_reach_ids,
                    constituents=constituents,
                    observation_summaries=[],  # Not stored in DB
                    flow_station_ids=flow_station_ids,
                    comments=sta_row['comments']
                ))
            
            # Load watershed constraints
            ws_df = con.execute(
                """SELECT * FROM calibration.watershed_constraints
                   WHERE location_id = ?""",
                [loc_row['location_id']]
            ).fetchdf()
            
            watershed_constraints = []
            for _, ws_row in ws_df.iterrows():
                # Load landcover constraints
                lc_df = con.execute(
                    """SELECT * FROM calibration.landcover_constraints
                       WHERE watershed_constraint_id = ?""",
                    [ws_row['id']]
                ).fetchdf()
                
                landcover_constraints = [
                    LandcoverConstraint(
                        landcover_type=lc['landcover_type'],
                        constituent=ws_row['constituent'],
                        target_rate=lc['target_rate'],
                        min_rate=lc['min_rate'],
                        max_rate=lc['max_rate']
                    )
                    for _, lc in lc_df.iterrows()
                ]
                
                watershed_constraints.append(WatershedConstraint(
                    constituent=ws_row['constituent'],
                    target_rate=ws_row['target_rate'],
                    min_rate=ws_row['min_rate'],
                    max_rate=ws_row['max_rate'],
                    landcover_constraints=landcover_constraints
                ))
            
            locations.append(Location(
                location_id=loc_row['location_id'],
                location_name=loc_row['location_name'],
                repository_name=loc_row['repository_name'],
                stations=stations,
                watershed_constraints=watershed_constraints,
                notes=loc_row['notes']
            ))
        
        return CalibrationConfig(
            repository_name=repository_name,
            locations=locations,
            default_metrics=[],  # Not stored in DB
            version='1.0'
        )


# ============================================================================
# CalibrationManager Class
# ============================================================================

class CalibrationManager:
    """
    Manager class for calibration configuration.
    
    Provides a unified interface for loading, saving, and working with
    calibration configurations from files or databases.
    """
    
    def __init__(
        self, 
        repository_name: str,
        db_path: Optional[Union[str, Path]] = None,
        config_path: Optional[Union[str, Path]] = None
    ):
        """
        Initialize the CalibrationManager.
        
        Args:
            repository_name: Name of the model repository
            db_path: Optional path to the DuckDB database
            config_path: Optional path to a configuration file (YAML or JSON)
        """
        self.repository_name = repository_name
        self.db_path = Path(db_path) if db_path else None
        self.config_path = Path(config_path) if config_path else None
        self._config: Optional[CalibrationConfig] = None
    
    @property
    def config(self) -> CalibrationConfig:
        """Get the current configuration, loading if necessary."""
        if self._config is None:
            self._config = self.load()
        return self._config
    
    def load(self) -> CalibrationConfig:
        """
        Load configuration from file or database.
        
        Priority:
        1. Configuration file (if config_path is set)
        2. Database (if db_path is set)
        3. Create new example configuration
        """
        if self.config_path and self.config_path.exists():
            self._config = load_config(self.config_path)
        elif self.db_path and self.db_path.exists():
            try:
                self._config = load_config_from_db(self.db_path, self.repository_name)
            except Exception:
                self._config = CalibrationConfig(repository_name=self.repository_name)
        else:
            self._config = CalibrationConfig(repository_name=self.repository_name)
        
        return self._config
    
    def save(self, to_file: bool = True, to_db: bool = True):
        """
        Save configuration to file and/or database.
        
        Args:
            to_file: Save to configuration file
            to_db: Save to database
        """
        if self._config is None:
            raise ValueError("No configuration loaded. Call load() first.")
        
        if to_file and self.config_path:
            save_config(self._config, self.config_path)
        
        if to_db and self.db_path:
            init_calibration_db(self.db_path)
            save_config_to_db(self._config, self.db_path)
    
    def add_location(self, location: Location):
        """Add a location to the configuration."""
        self.config.locations.append(location)
    
    def get_location(self, location_id: int) -> Optional[Location]:
        """Get a location by ID."""
        return self.config.get_location_by_id(location_id)
    
    def get_all_stations(self) -> list[Station]:
        """Get all stations across all locations."""
        return self.config.get_all_stations()
    
    def get_stations_as_dataframe(self) -> pd.DataFrame:
        """Get all stations as a pandas DataFrame."""
        stations = self.get_all_stations()
        records = []
        for station in stations:
            records.append({
                'station_id': station.station_id,
                'station_origin': station.station_origin,
                'repository_name': station.repository_name,
                'true_reach_id': station.true_reach_id,
                'reach_ids': ','.join(str(r) for r in station.reach_ids),
                'upstream_reach_ids': ','.join(str(r) for r in station.upstream_reach_ids),
                'flow_station_ids': ','.join(station.flow_station_ids),
                'comments': station.comments
            })
        return pd.DataFrame(records)
    
    def get_locations_as_dataframe(self) -> pd.DataFrame:
        """Get all locations as a pandas DataFrame."""
        records = []
        for location in self.config.locations:
            records.append({
                'location_id': location.location_id,
                'location_name': location.location_name,
                'repository_name': location.repository_name,
                'station_count': len(location.stations),
                'station_ids': ','.join(location.get_all_station_ids()),
                'notes': location.notes
            })
        return pd.DataFrame(records)
    
    def create_example_config(self) -> CalibrationConfig:
        """Create and set an example configuration."""
        self._config = create_example_config(self.repository_name)
        return self._config
