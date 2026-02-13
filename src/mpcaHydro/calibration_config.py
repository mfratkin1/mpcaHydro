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
    init_calibration_db,
    save_config_to_db,
    load_config_from_db,
    CALIBRATION_SCHEMA,
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
    'init_calibration_db',
    'save_config_to_db',
    'load_config_from_db',
    'CalibrationManager',
    'CALIBRATION_SCHEMA',
]


# ============================================================================
# CalibrationManager Class
# ============================================================================

class CalibrationManager:
    """
    Manager class for calibration configuration.
    
    Provides a unified interface for loading, saving, and working with
    calibration configurations from files or SQLite databases.
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
            db_path: Optional path to the SQLite database
            config_path: Optional path to a configuration file (YAML, JSON, or TOML)
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
        3. Create new empty configuration
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
            to_db: Save to SQLite database
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
