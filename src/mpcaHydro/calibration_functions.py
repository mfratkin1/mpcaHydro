# -*- coding: utf-8 -*-
"""Procedural functions for calibration configuration management."""

from pathlib import Path
from typing import List, Optional, Union

import pandas as pd

from mpcaHydro.calibration_dataclasses import (
    CalibrationConfig,
    Location,
    Station,
)

from mpcaHydro.calibration_io import (
    load_config,
    save_config,
    init_calibration_db,
    save_config_to_db,
    load_config_from_db,
)


def create_config(repository_name: str, version: str = "1.0") -> CalibrationConfig:
    """Create a new empty calibration configuration."""
    return CalibrationConfig(repository_name=repository_name, version=version)


def load_config_from_file(filepath: Union[str, Path]) -> CalibrationConfig:
    """Load calibration configuration from a file (YAML, JSON, or TOML)."""
    return load_config(filepath)


def save_config_to_file(config: CalibrationConfig, filepath: Union[str, Path]) -> None:
    """Save calibration configuration to a file (YAML, JSON, or TOML)."""
    save_config(config, filepath)


def load_from_db(db_path: Union[str, Path], repository_name: str) -> CalibrationConfig:
    """Load calibration configuration from SQLite database."""
    return load_config_from_db(db_path, repository_name)


def save_to_db(config: CalibrationConfig, db_path: Union[str, Path]) -> None:
    """Save calibration configuration to SQLite database."""
    init_calibration_db(db_path)
    save_config_to_db(config, db_path)


def add_location(config: CalibrationConfig, location: Location) -> CalibrationConfig:
    """Add a location to the configuration and return the modified config."""
    config.locations.append(location)
    return config


def get_location_by_id(config: CalibrationConfig, location_id: int) -> Optional[Location]:
    """Get a location from the configuration by its ID."""
    return config.get_location_by_id(location_id)


def get_location_by_name(config: CalibrationConfig, location_name: str) -> Optional[Location]:
    """Get a location from the configuration by its name."""
    return config.get_location_by_name(location_name)


def get_all_stations(config: CalibrationConfig) -> List[Station]:
    """Get all stations from all locations in the configuration."""
    return config.get_all_stations()


def get_all_station_ids(config: CalibrationConfig) -> List[str]:
    """Get all station IDs from all locations in the configuration."""
    return [station.station_id for station in get_all_stations(config)]


def get_all_reach_ids(config: CalibrationConfig) -> List[int]:
    """Get all reach IDs from all locations in the configuration."""
    reach_ids = []
    for location in config.locations:
        reach_ids.extend(location.reach_ids)
    return list(set(reach_ids))


def stations_to_dataframe(config: CalibrationConfig) -> pd.DataFrame:
    """Convert all stations in the configuration to a pandas DataFrame."""
    stations = get_all_stations(config)
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


def locations_to_dataframe(config: CalibrationConfig) -> pd.DataFrame:
    """Convert all locations in the configuration to a pandas DataFrame."""
    records = []
    for location in config.locations:
        records.append({
            'location_id': location.location_id,
            'location_name': location.location_name,
            'repository_name': location.repository_name,
            'reach_ids': ','.join(str(r) for r in (location.reach_ids or [])),
            'upstream_reach_ids': ','.join(str(r) for r in (location.upstream_reach_ids or [])),
            'flow_station_ids': ','.join(location.flow_station_ids or []),
            'station_count': len(location.stations or []),
            'station_ids': ','.join(location.get_all_station_ids()),
            'notes': location.notes
        })
    return pd.DataFrame(records)


def config_to_dict(config: CalibrationConfig) -> dict:
    """Convert a calibration configuration to a dictionary."""
    return config.to_dict()


def config_from_dict(data: dict) -> CalibrationConfig:
    """Create a calibration configuration from a dictionary."""
    return CalibrationConfig.from_dict(data)
