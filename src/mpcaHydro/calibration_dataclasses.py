# -*- coding: utf-8 -*-
"""
Calibration Data Classes Module

This module contains all the data classes used for calibration configuration.
These can be imported here or in other modules as needed.

Data Classes:
- Metric: Metrics to calculate (NSE, logNSE, Pbias, etc.)
- Observation: Observation metadata including constituents, date ranges, sample counts
- LandcoverConstraint: Loading rate constraints for specific landcover types
- WatershedConstraint: Loading rate constraints for the watershed
- GeneralConstraint: Scaffolding for general model-level constraints (to be defined)
- Station: A monitoring station with metadata
- Location: A grouping of one or more stations
- CalibrationConfig: Root configuration for calibration locations
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ============================================================================
# Metric Data Class
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


# ============================================================================
# Observation Data Class (includes constituent config)
# ============================================================================

@dataclass
class Observation:
    """
    Observation data for a constituent at a station.
    
    This class combines observation metadata (data availability) with
    constituent configuration (metrics and derivations).
    
    Attributes:
        constituent: Constituent name (e.g., 'Q', 'TSS', 'TP')
        start_year: First year with data
        end_year: Last year with data
        avg_samples_per_year: Average number of samples per calendar year
        median_samples_per_year: Median number of samples per calendar year
        years_with_data: Number of years with data collected
        total_samples: Total number of samples
        metrics: List of metrics to calculate for this constituent
        derived_from: Optional list of other constituents used to derive this one
                     (e.g., load derived from flow and concentration)
    """
    constituent: str
    start_year: Optional[int] = None
    end_year: Optional[int] = None
    avg_samples_per_year: Optional[float] = None
    median_samples_per_year: Optional[float] = None
    years_with_data: Optional[int] = None
    total_samples: Optional[int] = None
    metrics: List[Metric] = field(default_factory=list)
    derived_from: List[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            'constituent': self.constituent,
            'start_year': self.start_year,
            'end_year': self.end_year,
            'avg_samples_per_year': self.avg_samples_per_year,
            'median_samples_per_year': self.median_samples_per_year,
            'years_with_data': self.years_with_data,
            'total_samples': self.total_samples,
            'metrics': [m.to_dict() for m in self.metrics],
            'derived_from': self.derived_from
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Observation':
        metrics = [Metric.from_dict(m) for m in data.get('metrics', [])]
        return cls(
            constituent=data['constituent'],
            start_year=data.get('start_year'),
            end_year=data.get('end_year'),
            avg_samples_per_year=data.get('avg_samples_per_year'),
            median_samples_per_year=data.get('median_samples_per_year'),
            years_with_data=data.get('years_with_data'),
            total_samples=data.get('total_samples'),
            metrics=metrics,
            derived_from=data.get('derived_from', [])
        )


# ============================================================================
# Constraint Data Classes
# ============================================================================

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
    landcover_constraints: List[LandcoverConstraint] = field(default_factory=list)

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
class GeneralConstraint:
    """
    Scaffolding for general model-level constraints.
    
    This is a placeholder for future constraints that apply to the entire model,
    not a specific location. Examples might include:
    - Catchment loading rates
    - Surface runoff constraints
    - Other model-wide parameters
    
    Attributes:
        name: Name/identifier for the constraint
        constraint_type: Type of constraint (to be defined)
        parameters: Dictionary of constraint parameters (flexible for future needs)
        enabled: Whether this constraint is active
        notes: Optional notes about the constraint
    """
    name: str
    constraint_type: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True
    notes: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'constraint_type': self.constraint_type,
            'parameters': self.parameters,
            'enabled': self.enabled,
            'notes': self.notes
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'GeneralConstraint':
        return cls(
            name=data['name'],
            constraint_type=data.get('constraint_type', ''),
            parameters=data.get('parameters', {}),
            enabled=data.get('enabled', True),
            notes=data.get('notes')
        )


# ============================================================================
# Station Data Class
# ============================================================================

@dataclass
class Station:
    """
    Represents a monitoring station.
    
    Attributes:
        station_id: Unique identifier for the station
        station_origin: Data source (e.g., 'wiski', 'equis')
        repository_name: Name of the model repository
        true_reach_id: The model reach the station is located on (one-to-one)
        observations: List of observations for available data (includes constituent config)
        comments: Optional notes about the station
    """
    station_id: str
    station_origin: str
    repository_name: str
    true_reach_id: Optional[int] = None
    observations: List[Observation] = field(default_factory=list)
    comments: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            'station_id': self.station_id,
            'station_origin': self.station_origin,
            'repository_name': self.repository_name,
            'true_reach_id': self.true_reach_id,
            'observations': [o.to_dict() for o in self.observations],
            'comments': self.comments
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'Station':
        observations = [
            Observation.from_dict(o) 
            for o in data.get('observations', [])
        ]
        return cls(
            station_id=data['station_id'],
            station_origin=data['station_origin'],
            repository_name=data['repository_name'],
            true_reach_id=data.get('true_reach_id'),
            observations=observations,
            comments=data.get('comments')
        )


# ============================================================================
# Location Data Class
# ============================================================================

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
        reach_ids: The model output reaches that best map to this location (many-to-many)
        upstream_reach_ids: Optional upstream reach IDs for watershed loading calculations
        flow_station_ids: Station IDs that can provide supplemental flow data
        stations: List of stations at this location
        watershed_constraints: Loading rate constraints for the watershed
        notes: Optional notes about the location
    """
    location_id: int
    location_name: str
    repository_name: str
    reach_ids: List[int] = field(default_factory=list)
    upstream_reach_ids: List[int] = field(default_factory=list)
    flow_station_ids: List[str] = field(default_factory=list)
    stations: List[Station] = field(default_factory=list)
    watershed_constraints: List[WatershedConstraint] = field(default_factory=list)
    notes: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            'location_id': self.location_id,
            'location_name': self.location_name,
            'repository_name': self.repository_name,
            'reach_ids': self.reach_ids,
            'upstream_reach_ids': self.upstream_reach_ids,
            'flow_station_ids': self.flow_station_ids,
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
            reach_ids=data.get('reach_ids', []),
            upstream_reach_ids=data.get('upstream_reach_ids', []),
            flow_station_ids=data.get('flow_station_ids', []),
            stations=stations,
            watershed_constraints=watershed_constraints,
            notes=data.get('notes')
        )

    def get_all_reach_ids(self) -> List[int]:
        """Get all reach IDs for this location."""
        return self.reach_ids

    def get_all_station_ids(self) -> List[str]:
        """Get all station IDs at this location."""
        return [station.station_id for station in self.stations]


# ============================================================================
# CalibrationConfig Data Class
# ============================================================================

@dataclass
class CalibrationConfig:
    """
    Root configuration for calibration locations.
    
    Attributes:
        repository_name: Name of the model repository
        locations: List of calibration locations
        default_metrics: Default metrics to apply if not specified at station level
        general_constraints: General constraints that apply to the entire model
        version: Configuration version for tracking changes
    """
    repository_name: str
    locations: List[Location] = field(default_factory=list)
    default_metrics: List[Metric] = field(default_factory=list)
    general_constraints: List[GeneralConstraint] = field(default_factory=list)
    version: str = "1.0"

    def to_dict(self) -> dict:
        return {
            'repository_name': self.repository_name,
            'locations': [loc.to_dict() for loc in self.locations],
            'default_metrics': [m.to_dict() for m in self.default_metrics],
            'general_constraints': [gc.to_dict() for gc in self.general_constraints],
            'version': self.version
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'CalibrationConfig':
        locations = [Location.from_dict(loc) for loc in data.get('locations', [])]
        default_metrics = [
            Metric.from_dict(m) 
            for m in data.get('default_metrics', [])
        ]
        general_constraints = [
            GeneralConstraint.from_dict(gc) 
            for gc in data.get('general_constraints', [])
        ]
        return cls(
            repository_name=data['repository_name'],
            locations=locations,
            default_metrics=default_metrics,
            general_constraints=general_constraints,
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

    def get_all_stations(self) -> List[Station]:
        """Get all stations across all locations."""
        stations = []
        for loc in self.locations:
            stations.extend(loc.stations)
        return stations


# ============================================================================
# Helper Functions for Default Metrics
# ============================================================================

def get_default_timeseries_metrics() -> List[Metric]:
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


def get_default_discrete_metrics() -> List[Metric]:
    """
    Get default metrics for discrete sample observations (e.g., water quality).
    """
    return [
        Metric(name='Pbias', target=25.0, weight=1.0),
        Metric(name='monthly_average', target=25.0, weight=0.5),
        Metric(name='seasonal_average', target=25.0, weight=0.5),
    ]
