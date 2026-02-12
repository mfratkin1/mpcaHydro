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
- GeneralConstraint: Scaffolding for general model-level constraints (to be defined)

Users can pass in configuration files (YAML, JSON, or TOML) to customize the calibration setup.
"""

from pathlib import Path
from typing import List, Optional, Union

import duckdb
import pandas as pd

# Import data classes from the separate module
from mpcaHydro.calibration_dataclasses import (
    Metric,
    Observation,
    LandcoverConstraint,
    WatershedConstraint,
    GeneralConstraint,
    Station,
    Location,
    CalibrationConfig,
    get_default_timeseries_metrics,
    get_default_discrete_metrics,
)

# Import config file I/O from the separate module
from mpcaHydro.calibration_io import (
    load_config,
    save_config,
)

# Re-export for backwards compatibility
__all__ = [
    'Metric',
    'Observation', 
    'LandcoverConstraint',
    'WatershedConstraint',
    'GeneralConstraint',
    'Station',
    'Location',
    'CalibrationConfig',
    'get_default_timeseries_metrics',
    'get_default_discrete_metrics',
    'load_config',
    'save_config',
    'create_example_config',
    'init_calibration_db',
    'save_config_to_db',
    'load_config_from_db',
    'CalibrationManager',
    'CALIBRATION_SCHEMA',
]


# ============================================================================
# Example Configuration
# ============================================================================

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
        observations=[
            Observation(
                constituent='Q',
                start_year=2000,
                end_year=2023,
                avg_samples_per_year=365.0,
                median_samples_per_year=365.0,
                years_with_data=24,
                total_samples=8760,
                metrics=get_default_timeseries_metrics(),
                derived_from=[]
            ),
        ],
        comments='Primary flow monitoring station'
    )

    # Example station with discrete sample observations (water quality)
    wq_station = Station(
        station_id='S002-118',
        station_origin='equis',
        repository_name=repository_name,
        true_reach_id=650,
        observations=[
            Observation(
                constituent='TSS',
                start_year=2005,
                end_year=2020,
                avg_samples_per_year=12.0,
                median_samples_per_year=10.0,
                years_with_data=15,
                total_samples=180,
                metrics=get_default_discrete_metrics(),
                derived_from=[]
            ),
            Observation(
                constituent='TP',
                start_year=2005,
                end_year=2020,
                avg_samples_per_year=12.0,
                median_samples_per_year=10.0,
                years_with_data=15,
                total_samples=180,
                metrics=get_default_discrete_metrics(),
                derived_from=[]
            ),
            Observation(
                constituent='TP_load',
                metrics=[Metric(name='Pbias', target=30.0)],
                derived_from=['TP', 'Q']  # Load derived from concentration and flow
            ),
        ],
        comments='Water quality monitoring station'
    )

    # Example location with multiple stations
    location = Location(
        location_id=1,
        location_name='Clearwater Outlet',
        repository_name=repository_name,
        reach_ids=[650],
        upstream_reach_ids=[649, 648],
        flow_station_ids=['E66050001'],  # Use flow from this station for WQ calculations
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
        general_constraints=[],  # Placeholder for future general constraints
        version='1.0'
    )


# ============================================================================
# Database Schema and Integration
# ============================================================================

# Note: outlets module is imported lazily in functions that need it
# to avoid circular imports and missing data file issues

CALIBRATION_SCHEMA = """
-- Calibration configuration schema for DuckDB
-- Extends the existing outlets schema with calibration-specific tables
-- This schema adds calibration metadata while reusing the outlets station/reach relationships

-- Create outlets schema first (contains base tables for stations and reaches)
-- Note: This assumes outlets.OUTLETS_SCHEMA is already run or included

-- Table: calibration_locations
-- Represents calibration locations (groupings based on outlet_id from outlets schema)
CREATE TABLE IF NOT EXISTS outlets.calibration_locations (
    location_id INTEGER PRIMARY KEY,
    location_name TEXT NOT NULL,
    repository_name TEXT NOT NULL,
    notes TEXT
);

-- Table: calibration_location_reaches
-- Reach mappings for calibration locations (at location level, not station)
CREATE TABLE IF NOT EXISTS outlets.calibration_location_reaches (
    location_id INTEGER NOT NULL,
    reach_id INTEGER NOT NULL,
    is_upstream BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (location_id) REFERENCES outlets.calibration_locations(location_id)
);

-- Table: calibration_flow_stations
-- Supplemental flow stations for locations (used for load calculations)
CREATE TABLE IF NOT EXISTS outlets.calibration_flow_stations (
    location_id INTEGER NOT NULL,
    flow_station_id TEXT NOT NULL,
    FOREIGN KEY (location_id) REFERENCES outlets.calibration_locations(location_id)
);

-- Table: calibration_location_stations
-- Links stations from outlet_stations to calibration locations
CREATE TABLE IF NOT EXISTS outlets.calibration_location_stations (
    location_id INTEGER NOT NULL,
    station_id TEXT NOT NULL,
    station_origin TEXT NOT NULL,
    FOREIGN KEY (location_id) REFERENCES outlets.calibration_locations(location_id)
);

-- Table: calibration_constituents
-- Constituent configurations for stations
CREATE TABLE IF NOT EXISTS outlets.calibration_constituents (
    id INTEGER PRIMARY KEY,
    station_id TEXT NOT NULL,
    station_origin TEXT NOT NULL,
    constituent TEXT NOT NULL
);

-- Table: calibration_metrics
-- Metrics to calculate for constituents
CREATE TABLE IF NOT EXISTS outlets.calibration_metrics (
    id INTEGER PRIMARY KEY,
    constituent_id INTEGER NOT NULL,
    metric_name TEXT NOT NULL,
    target DOUBLE,
    weight DOUBLE DEFAULT 1.0,
    enabled BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (constituent_id) REFERENCES outlets.calibration_constituents(id)
);

-- Table: calibration_derived_constituents
-- Derived constituents (e.g., load from flow and concentration)
CREATE TABLE IF NOT EXISTS outlets.calibration_derived_constituents (
    constituent_id INTEGER NOT NULL,
    source_constituent TEXT NOT NULL,
    FOREIGN KEY (constituent_id) REFERENCES outlets.calibration_constituents(id)
);

-- Table: calibration_watershed_constraints
-- Watershed loading rate constraints
CREATE TABLE IF NOT EXISTS outlets.calibration_watershed_constraints (
    id INTEGER PRIMARY KEY,
    location_id INTEGER NOT NULL,
    constituent TEXT NOT NULL,
    target_rate DOUBLE,
    min_rate DOUBLE,
    max_rate DOUBLE,
    FOREIGN KEY (location_id) REFERENCES outlets.calibration_locations(location_id)
);

-- Table: calibration_landcover_constraints
-- Landcover-specific loading rate constraints
CREATE TABLE IF NOT EXISTS outlets.calibration_landcover_constraints (
    id INTEGER PRIMARY KEY,
    watershed_constraint_id INTEGER NOT NULL,
    landcover_type TEXT NOT NULL,
    target_rate DOUBLE,
    min_rate DOUBLE,
    max_rate DOUBLE,
    FOREIGN KEY (watershed_constraint_id) 
        REFERENCES outlets.calibration_watershed_constraints(id)
);

-- View: calibration_location_summary
-- Summary view of calibration locations with their stations and reaches
CREATE OR REPLACE VIEW outlets.calibration_location_summary AS
SELECT
    l.location_id,
    l.location_name,
    l.repository_name,
    COUNT(DISTINCT ls.station_id) AS station_count,
    STRING_AGG(DISTINCT ls.station_id, ', ') AS station_ids,
    STRING_AGG(DISTINCT CAST(lr.reach_id AS TEXT), ',') 
        FILTER (WHERE NOT lr.is_upstream) AS reach_ids,
    STRING_AGG(DISTINCT CAST(lr.reach_id AS TEXT), ',') 
        FILTER (WHERE lr.is_upstream) AS upstream_reach_ids,
    STRING_AGG(DISTINCT fs.flow_station_id, ', ') AS flow_station_ids,
    l.notes
FROM outlets.calibration_locations l
LEFT JOIN outlets.calibration_location_stations ls ON l.location_id = ls.location_id
LEFT JOIN outlets.calibration_location_reaches lr ON l.location_id = lr.location_id
LEFT JOIN outlets.calibration_flow_stations fs ON l.location_id = fs.location_id
GROUP BY l.location_id, l.location_name, l.repository_name, l.notes;
"""


def init_calibration_db(db_path: Union[str, Path], reset: bool = False):
    """
    Initialize the calibration schema in a DuckDB database.
    
    This integrates with the existing outlets schema and adds calibration-specific tables.
    
    Args:
        db_path: Path to the DuckDB database
        reset: If True, drop and recreate the calibration tables
    """
    from mpcaHydro import outlets
    
    db_path = Path(db_path)
    
    with duckdb.connect(db_path.as_posix(), read_only=False) as con:
        # First initialize the outlets schema
        con.execute(outlets.OUTLETS_SCHEMA)
        
        if reset:
            # Drop calibration-specific tables
            con.execute("DROP TABLE IF EXISTS outlets.calibration_landcover_constraints CASCADE")
            con.execute("DROP TABLE IF EXISTS outlets.calibration_watershed_constraints CASCADE")
            con.execute("DROP TABLE IF EXISTS outlets.calibration_derived_constituents CASCADE")
            con.execute("DROP TABLE IF EXISTS outlets.calibration_metrics CASCADE")
            con.execute("DROP TABLE IF EXISTS outlets.calibration_constituents CASCADE")
            con.execute("DROP TABLE IF EXISTS outlets.calibration_flow_stations CASCADE")
            con.execute("DROP TABLE IF EXISTS outlets.calibration_location_reaches CASCADE")
            con.execute("DROP TABLE IF EXISTS outlets.calibration_location_stations CASCADE")
            con.execute("DROP TABLE IF EXISTS outlets.calibration_locations CASCADE")
            con.execute("DROP VIEW IF EXISTS outlets.calibration_location_summary CASCADE")
        
        con.execute(CALIBRATION_SCHEMA)


def save_config_to_db(
    config: CalibrationConfig, 
    db_path: Union[str, Path],
    replace: bool = True
):
    """
    Save a calibration configuration to the database.
    
    This saves to the outlets schema, integrating with the existing station/reach tables.
    
    Args:
        config: CalibrationConfig object to save
        db_path: Path to the DuckDB database
        replace: If True, replace existing data for the repository
    """
    from mpcaHydro import outlets
    
    db_path = Path(db_path)
    
    with duckdb.connect(db_path.as_posix(), read_only=False) as con:
        # Initialize schema if needed (includes outlets schema)
        con.execute(outlets.OUTLETS_SCHEMA)
        con.execute(CALIBRATION_SCHEMA)
        
        if replace:
            # Delete existing data for this repository
            con.execute(
                "DELETE FROM outlets.calibration_locations WHERE repository_name = ?",
                [config.repository_name]
            )
        
        constituent_id_counter = 1
        metric_id_counter = 1
        watershed_constraint_id_counter = 1
        landcover_constraint_id_counter = 1
        
        for location in config.locations:
            # Insert location
            con.execute(
                """INSERT INTO outlets.calibration_locations 
                   (location_id, location_name, repository_name, notes)
                   VALUES (?, ?, ?, ?)""",
                [location.location_id, location.location_name, 
                 location.repository_name, location.notes]
            )
            
            # Insert location reach mappings (at location level)
            for reach_id in location.reach_ids:
                con.execute(
                    """INSERT INTO outlets.calibration_location_reaches
                       (location_id, reach_id, is_upstream)
                       VALUES (?, ?, FALSE)""",
                    [location.location_id, reach_id]
                )
            
            for reach_id in location.upstream_reach_ids:
                con.execute(
                    """INSERT INTO outlets.calibration_location_reaches
                       (location_id, reach_id, is_upstream)
                       VALUES (?, ?, TRUE)""",
                    [location.location_id, reach_id]
                )
            
            # Insert flow station references (at location level)
            for flow_station_id in location.flow_station_ids:
                con.execute(
                    """INSERT INTO outlets.calibration_flow_stations
                       (location_id, flow_station_id)
                       VALUES (?, ?)""",
                    [location.location_id, flow_station_id]
                )
            
            for station in location.stations:
                # Link station to location
                con.execute(
                    """INSERT INTO outlets.calibration_location_stations
                       (location_id, station_id, station_origin)
                       VALUES (?, ?, ?)""",
                    [location.location_id, station.station_id, station.station_origin]
                )
                
                # Insert observations (includes constituent config with metrics)
                for observation in station.observations:
                    con.execute(
                        """INSERT INTO outlets.calibration_constituents
                           (id, station_id, station_origin, constituent)
                           VALUES (?, ?, ?, ?)""",
                        [constituent_id_counter, station.station_id, 
                         station.station_origin, observation.constituent]
                    )
                    
                    for metric in observation.metrics:
                        con.execute(
                            """INSERT INTO outlets.calibration_metrics
                               (id, constituent_id, metric_name, target, weight, enabled)
                               VALUES (?, ?, ?, ?, ?, ?)""",
                            [metric_id_counter, constituent_id_counter, metric.name, metric.target,
                             metric.weight, metric.enabled]
                        )
                        metric_id_counter += 1
                    
                    for source in observation.derived_from:
                        con.execute(
                            """INSERT INTO outlets.calibration_derived_constituents
                               (constituent_id, source_constituent)
                               VALUES (?, ?)""",
                            [constituent_id_counter, source]
                        )
                    
                    constituent_id_counter += 1
            
            # Insert watershed constraints
            for watershed_constraint in location.watershed_constraints:
                con.execute(
                    """INSERT INTO outlets.calibration_watershed_constraints
                       (id, location_id, constituent, target_rate, min_rate, max_rate)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    [watershed_constraint_id_counter, location.location_id,
                     watershed_constraint.constituent, watershed_constraint.target_rate,
                     watershed_constraint.min_rate, watershed_constraint.max_rate]
                )
                
                for lc_constraint in watershed_constraint.landcover_constraints:
                    con.execute(
                        """INSERT INTO outlets.calibration_landcover_constraints
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
            """SELECT * FROM outlets.calibration_locations WHERE repository_name = ?""",
            [repository_name]
        ).fetchdf()
        
        locations = []
        for _, loc_row in locations_df.iterrows():
            # Load reach mappings (at location level)
            reaches_df = con.execute(
                """SELECT reach_id, is_upstream FROM outlets.calibration_location_reaches
                   WHERE location_id = ?""",
                [loc_row['location_id']]
            ).fetchdf()
            
            reach_ids = reaches_df[~reaches_df['is_upstream']]['reach_id'].tolist() if not reaches_df.empty else []
            upstream_reach_ids = reaches_df[reaches_df['is_upstream']]['reach_id'].tolist() if not reaches_df.empty else []
            
            # Load flow stations (at location level)
            flow_df = con.execute(
                """SELECT flow_station_id FROM outlets.calibration_flow_stations
                   WHERE location_id = ?""",
                [loc_row['location_id']]
            ).fetchdf()
            flow_station_ids = flow_df['flow_station_id'].tolist() if not flow_df.empty else []
            
            # Load stations for this location
            stations_df = con.execute(
                """SELECT * FROM outlets.calibration_location_stations 
                   WHERE location_id = ?""",
                [loc_row['location_id']]
            ).fetchdf()
            
            stations = []
            for _, sta_row in stations_df.iterrows():
                # Load observations (includes constituent config with metrics)
                observations_df = con.execute(
                    """SELECT * FROM outlets.calibration_constituents
                       WHERE station_id = ? AND station_origin = ?""",
                    [sta_row['station_id'], sta_row['station_origin']]
                ).fetchdf()
                
                observations = []
                for _, obs_row in observations_df.iterrows():
                    # Load metrics for this observation
                    metrics_df = con.execute(
                        """SELECT * FROM outlets.calibration_metrics
                           WHERE constituent_id = ?""",
                        [obs_row['id']]
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
                        """SELECT source_constituent FROM outlets.calibration_derived_constituents
                           WHERE constituent_id = ?""",
                        [obs_row['id']]
                    ).fetchdf()
                    derived_from = derived_df['source_constituent'].tolist() if not derived_df.empty else []
                    
                    observations.append(Observation(
                        constituent=obs_row['constituent'],
                        metrics=metrics,
                        derived_from=derived_from
                    ))
                
                # Try to get station details from outlets.outlet_stations if available
                try:
                    outlet_sta_df = con.execute(
                        """SELECT true_opnid, comments FROM outlets.outlet_stations
                           WHERE station_id = ? AND station_origin = ?""",
                        [sta_row['station_id'], sta_row['station_origin']]
                    ).fetchdf()
                    true_reach_id = outlet_sta_df['true_opnid'].iloc[0] if not outlet_sta_df.empty else None
                    comments = outlet_sta_df['comments'].iloc[0] if not outlet_sta_df.empty else None
                except Exception:
                    true_reach_id = None
                    comments = None
                
                stations.append(Station(
                    station_id=sta_row['station_id'],
                    station_origin=sta_row['station_origin'],
                    repository_name=repository_name,
                    true_reach_id=true_reach_id,
                    observations=observations,
                    comments=comments
                ))
            
            # Load watershed constraints
            ws_df = con.execute(
                """SELECT * FROM outlets.calibration_watershed_constraints
                   WHERE location_id = ?""",
                [loc_row['location_id']]
            ).fetchdf()
            
            watershed_constraints = []
            for _, ws_row in ws_df.iterrows():
                # Load landcover constraints
                lc_df = con.execute(
                    """SELECT * FROM outlets.calibration_landcover_constraints
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
                reach_ids=reach_ids,
                upstream_reach_ids=upstream_reach_ids,
                flow_station_ids=flow_station_ids,
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
    
    def get_all_stations(self) -> List[Station]:
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
                'reach_ids': ','.join(str(r) for r in location.reach_ids),
                'upstream_reach_ids': ','.join(str(r) for r in location.upstream_reach_ids),
                'flow_station_ids': ','.join(location.flow_station_ids),
                'station_count': len(location.stations),
                'station_ids': ','.join(location.get_all_station_ids()),
                'notes': location.notes
            })
        return pd.DataFrame(records)
    
    def create_example_config(self) -> CalibrationConfig:
        """Create and set an example configuration."""
        self._config = create_example_config(self.repository_name)
        return self._config
