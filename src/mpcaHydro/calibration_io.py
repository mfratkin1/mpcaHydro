# -*- coding: utf-8 -*-
"""
Calibration Configuration I/O Module

This module handles loading and saving calibration configurations from/to:
- Files (YAML, JSON, and TOML formats)
- SQLite database

This module is separate from the data classes to allow for independent development
and to be worked on later once the data class structure is established.
"""

from pathlib import Path
from typing import Union
import json
import sqlite3

from mpcaHydro.calibration_dataclasses import (
    CalibrationConfig,
    Location,
    Station,
    Observation,
    Metric,
    WatershedConstraint,
    LandcoverConstraint,
)

# Check for optional dependencies
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

try:
    import tomllib  # Python 3.11+
    TOML_READ_AVAILABLE = True
except ImportError:
    try:
        import tomli as tomllib  # Fallback for Python < 3.11
        TOML_READ_AVAILABLE = True
    except ImportError:
        TOML_READ_AVAILABLE = False

try:
    import tomli_w
    TOML_WRITE_AVAILABLE = True
except ImportError:
    TOML_WRITE_AVAILABLE = False


# ============================================================================
# File I/O Functions
# ============================================================================

def load_config(filepath: Union[str, Path]) -> CalibrationConfig:
    """
    Load calibration configuration from a file.
    
    Supports YAML, JSON, and TOML formats based on file extension.
    
    Args:
        filepath: Path to the configuration file
        
    Returns:
        CalibrationConfig object
        
    Raises:
        ValueError: If file format is not supported
        FileNotFoundError: If file does not exist
        ImportError: If required library for file format is not installed
    """
    filepath = Path(filepath)
    
    if not filepath.exists():
        raise FileNotFoundError(f"Configuration file not found: {filepath}")
    
    with open(filepath, 'r') as f:
        content = f.read()
    
    suffix = filepath.suffix.lower()
    
    if suffix in ['.yaml', '.yml']:
        if not YAML_AVAILABLE:
            raise ImportError(
                "PyYAML is required to load YAML configuration files. "
                "Install it with: pip install pyyaml"
            )
        data = yaml.safe_load(content)
    elif suffix == '.json':
        data = json.loads(content)
    elif suffix == '.toml':
        if not TOML_READ_AVAILABLE:
            raise ImportError(
                "tomllib (Python 3.11+) or tomli is required to load TOML configuration files. "
                "Install tomli with: pip install tomli"
            )
        # TOML requires binary mode for tomllib
        with open(filepath, 'rb') as f:
            data = tomllib.load(f)
    else:
        raise ValueError(
            f"Unsupported configuration file format: {suffix}. "
            "Supported formats: .yaml, .yml, .json, .toml"
        )
    
    return CalibrationConfig.from_dict(data)


def save_config(config: CalibrationConfig, filepath: Union[str, Path]) -> None:
    """
    Save calibration configuration to a file.
    
    Supports YAML, JSON, and TOML formats based on file extension.
    
    Args:
        config: CalibrationConfig object to save
        filepath: Path to save the configuration file
        
    Raises:
        ValueError: If file format is not supported
        ImportError: If required library for file format is not installed
    """
    filepath = Path(filepath)
    data = config.to_dict()
    
    # Ensure parent directory exists
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    suffix = filepath.suffix.lower()
    
    if suffix in ['.yaml', '.yml']:
        if not YAML_AVAILABLE:
            raise ImportError(
                "PyYAML is required to save YAML configuration files. "
                "Install it with: pip install pyyaml"
            )
        with open(filepath, 'w') as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
    elif suffix == '.json':
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
    elif suffix == '.toml':
        if not TOML_WRITE_AVAILABLE:
            raise ImportError(
                "tomli-w is required to save TOML configuration files. "
                "Install it with: pip install tomli-w"
            )
        with open(filepath, 'wb') as f:
            tomli_w.dump(data, f)
    else:
        raise ValueError(
            f"Unsupported configuration file format: {suffix}. "
            "Supported formats: .yaml, .yml, .json, .toml"
        )


# ============================================================================
# SQLite Database Schema
# ============================================================================

from mpcaHydro import sql_loader

# Load schema from SQL file
CALIBRATION_SCHEMA = sql_loader.get_calibration_schema_sql()


# ============================================================================
# SQLite Database I/O Functions
# ============================================================================

def init_calibration_db(db_path: Union[str, Path], reset: bool = False):
    """Initialize the calibration schema in a SQLite database."""
    db_path = Path(db_path)
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    try:
        if reset:
            # Drop calibration-specific tables in reverse dependency order
            cursor.execute("DROP TABLE IF EXISTS calibration_landcover_constraints")
            cursor.execute("DROP TABLE IF EXISTS calibration_watershed_constraints")
            cursor.execute("DROP TABLE IF EXISTS calibration_derived_constituents")
            cursor.execute("DROP TABLE IF EXISTS calibration_metrics")
            cursor.execute("DROP TABLE IF EXISTS calibration_observations")
            cursor.execute("DROP TABLE IF EXISTS calibration_stations")
            cursor.execute("DROP TABLE IF EXISTS calibration_flow_stations")
            cursor.execute("DROP TABLE IF EXISTS calibration_location_reaches")
            cursor.execute("DROP TABLE IF EXISTS calibration_locations")
        
        # Create tables
        cursor.executescript(CALIBRATION_SCHEMA)
        conn.commit()
    finally:
        conn.close()


def save_config_to_db(
    config: CalibrationConfig, 
    db_path: Union[str, Path],
    replace: bool = True
):
    """
    Save a calibration configuration to the SQLite database.
    
    Args:
        config: CalibrationConfig object to save
        db_path: Path to the SQLite database
        replace: If True, replace existing data for the repository
    """
    db_path = Path(db_path)
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    try:
        # Initialize schema if needed
        cursor.executescript(CALIBRATION_SCHEMA)
        
        if replace:
            # Delete existing data for this repository
            cursor.execute(
                "DELETE FROM calibration_locations WHERE repository_name = ?",
                (config.repository_name,)
            )
        
        for location in config.locations:
            # Insert location
            cursor.execute(
                """INSERT INTO calibration_locations 
                   (location_id, location_name, repository_name, notes)
                   VALUES (?, ?, ?, ?)""",
                (location.location_id, location.location_name, 
                 location.repository_name, location.notes)
            )
            
            # Insert location reach mappings (at location level)
            # Note: negative reach_ids can be used for subtraction
            for reach_id in location.reach_ids:
                cursor.execute(
                    """INSERT INTO calibration_location_reaches
                       (location_id, reach_id, is_upstream)
                       VALUES (?, ?, 0)""",
                    (location.location_id, reach_id)
                )
            
            for reach_id in location.upstream_reach_ids:
                cursor.execute(
                    """INSERT INTO calibration_location_reaches
                       (location_id, reach_id, is_upstream)
                       VALUES (?, ?, 1)""",
                    (location.location_id, reach_id)
                )
            
            # Insert flow station references (at location level)
            for flow_station_id in location.flow_station_ids:
                cursor.execute(
                    """INSERT INTO calibration_flow_stations
                       (location_id, flow_station_id)
                       VALUES (?, ?)""",
                    (location.location_id, flow_station_id)
                )
            
            for station in location.stations:
                # Insert station
                cursor.execute(
                    """INSERT INTO calibration_stations
                       (location_id, station_id, station_origin, repository_name, 
                        true_reach_id, comments)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (location.location_id, station.station_id, station.station_origin,
                     station.repository_name, station.true_reach_id, station.comments)
                )
                
                # Insert observations (includes constituent config with metrics)
                for observation in station.observations:
                    cursor.execute(
                        """INSERT INTO calibration_observations
                           (station_id, station_origin, constituent, start_year, end_year,
                            avg_samples_per_year, median_samples_per_year, years_with_data,
                            total_samples)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (station.station_id, station.station_origin, observation.constituent,
                         observation.start_year, observation.end_year,
                         observation.avg_samples_per_year, observation.median_samples_per_year,
                         observation.years_with_data, observation.total_samples)
                    )
                    observation_id = cursor.lastrowid
                    
                    for metric in observation.metrics:
                        cursor.execute(
                            """INSERT INTO calibration_metrics
                               (observation_id, metric_name, target, weight, enabled)
                               VALUES (?, ?, ?, ?, ?)""",
                            (observation_id, metric.name, metric.target,
                             metric.weight, 1 if metric.enabled else 0)
                        )
                    
                    for source in observation.derived_from:
                        cursor.execute(
                            """INSERT INTO calibration_derived_constituents
                               (observation_id, source_constituent)
                               VALUES (?, ?)""",
                            (observation_id, source)
                        )
            
            # Insert watershed constraints
            for watershed_constraint in location.watershed_constraints:
                cursor.execute(
                    """INSERT INTO calibration_watershed_constraints
                       (location_id, constituent, target_rate, min_rate, max_rate)
                       VALUES (?, ?, ?, ?, ?)""",
                    (location.location_id, watershed_constraint.constituent,
                     watershed_constraint.target_rate, watershed_constraint.min_rate,
                     watershed_constraint.max_rate)
                )
                watershed_constraint_id = cursor.lastrowid
                
                for lc_constraint in watershed_constraint.landcover_constraints:
                    cursor.execute(
                        """INSERT INTO calibration_landcover_constraints
                           (watershed_constraint_id, landcover_type, 
                            target_rate, min_rate, max_rate)
                           VALUES (?, ?, ?, ?, ?)""",
                        (watershed_constraint_id, lc_constraint.landcover_type,
                         lc_constraint.target_rate, lc_constraint.min_rate,
                         lc_constraint.max_rate)
                    )
        
        conn.commit()
    finally:
        conn.close()


def load_config_from_db(
    db_path: Union[str, Path], 
    repository_name: str
) -> CalibrationConfig:
    """
    Load a calibration configuration from the SQLite database.
    
    Args:
        db_path: Path to the SQLite database
        repository_name: Name of the repository to load configuration for
        
    Returns:
        CalibrationConfig object
    """
    db_path = Path(db_path)
    
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        # Load locations
        cursor.execute(
            "SELECT * FROM calibration_locations WHERE repository_name = ?",
            (repository_name,)
        )
        locations_rows = cursor.fetchall()
        
        locations = []
        for loc_row in locations_rows:
            # Load reach mappings (at location level)
            cursor.execute(
                """SELECT reach_id, is_upstream FROM calibration_location_reaches
                   WHERE location_id = ?""",
                (loc_row['location_id'],)
            )
            reaches_rows = cursor.fetchall()
            
            reach_ids = [r['reach_id'] for r in reaches_rows if not r['is_upstream']]
            upstream_reach_ids = [r['reach_id'] for r in reaches_rows if r['is_upstream']]
            
            # Load flow stations (at location level)
            cursor.execute(
                """SELECT flow_station_id FROM calibration_flow_stations
                   WHERE location_id = ?""",
                (loc_row['location_id'],)
            )
            flow_rows = cursor.fetchall()
            flow_station_ids = [r['flow_station_id'] for r in flow_rows]
            
            # Load stations for this location
            cursor.execute(
                """SELECT * FROM calibration_stations WHERE location_id = ?""",
                (loc_row['location_id'],)
            )
            stations_rows = cursor.fetchall()
            
            stations = []
            for sta_row in stations_rows:
                # Load observations (includes constituent config with metrics)
                cursor.execute(
                    """SELECT * FROM calibration_observations
                       WHERE station_id = ? AND station_origin = ?""",
                    (sta_row['station_id'], sta_row['station_origin'])
                )
                observations_rows = cursor.fetchall()
                
                observations = []
                for obs_row in observations_rows:
                    # Load metrics for this observation
                    cursor.execute(
                        """SELECT * FROM calibration_metrics WHERE observation_id = ?""",
                        (obs_row['id'],)
                    )
                    metrics_rows = cursor.fetchall()
                    
                    metrics = [
                        Metric(
                            name=m['metric_name'],
                            target=m['target'],
                            weight=m['weight'],
                            enabled=bool(m['enabled'])
                        )
                        for m in metrics_rows
                    ]
                    
                    # Load derived constituents
                    cursor.execute(
                        """SELECT source_constituent FROM calibration_derived_constituents
                           WHERE observation_id = ?""",
                        (obs_row['id'],)
                    )
                    derived_rows = cursor.fetchall()
                    derived_from = [r['source_constituent'] for r in derived_rows]
                    
                    observations.append(Observation(
                        constituent=obs_row['constituent'],
                        start_year=obs_row['start_year'],
                        end_year=obs_row['end_year'],
                        avg_samples_per_year=obs_row['avg_samples_per_year'],
                        median_samples_per_year=obs_row['median_samples_per_year'],
                        years_with_data=obs_row['years_with_data'],
                        total_samples=obs_row['total_samples'],
                        metrics=metrics,
                        derived_from=derived_from
                    ))
                
                stations.append(Station(
                    station_id=sta_row['station_id'],
                    station_origin=sta_row['station_origin'],
                    repository_name=sta_row['repository_name'],
                    true_reach_id=sta_row['true_reach_id'],
                    observations=observations,
                    comments=sta_row['comments']
                ))
            
            # Load watershed constraints
            cursor.execute(
                """SELECT * FROM calibration_watershed_constraints WHERE location_id = ?""",
                (loc_row['location_id'],)
            )
            ws_rows = cursor.fetchall()
            
            watershed_constraints = []
            for ws_row in ws_rows:
                # Load landcover constraints
                cursor.execute(
                    """SELECT * FROM calibration_landcover_constraints
                       WHERE watershed_constraint_id = ?""",
                    (ws_row['id'],)
                )
                lc_rows = cursor.fetchall()
                
                landcover_constraints = [
                    LandcoverConstraint(
                        landcover_type=lc['landcover_type'],
                        constituent=ws_row['constituent'],
                        target_rate=lc['target_rate'],
                        min_rate=lc['min_rate'],
                        max_rate=lc['max_rate']
                    )
                    for lc in lc_rows
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
    finally:
        conn.close()
