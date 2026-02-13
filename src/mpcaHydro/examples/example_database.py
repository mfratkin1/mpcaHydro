# Example: Building SQLite Database for Calibration Configuration
# ================================================================
# This example demonstrates how to create and use a SQLite database
# to store calibration configurations for HSPF hydrologic models.

from pathlib import Path
import tempfile

from mpcaHydro.calibration_config import (
    CalibrationConfig,
    CalibrationManager,
    Location,
    Station,
    Observation,
    Metric,
    WatershedConstraint,
    LandcoverConstraint,
    GeneralConstraint,
    save_config,
    load_config,
    init_calibration_db,
    save_config_to_db,
    load_config_from_db,
    get_default_timeseries_metrics,
    get_default_discrete_metrics,
)


def create_example_config(repository_name: str) -> CalibrationConfig:
    """
    Create an example calibration configuration.
    
    This function is in the examples module to keep implementation and examples separate.
    
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


def example_build_database():
    """
    Example: Build a SQLite database with calibration configuration.
    
    This is separate from the outlets.py DuckDB database.
    """
    
    # 1. Create a calibration configuration programmatically
    # -------------------------------------------------------
    
    # Define stations at the calibration location
    # Note: Observation now includes constituent config (metrics, derived_from)
    flow_station = Station(
        station_id='E66050001',
        station_origin='wiski',
        repository_name='Clearwater',
        true_reach_id=650,
        observations=[
            Observation(
                constituent='Q',
                start_year=2000,
                end_year=2023,
                avg_samples_per_year=365.0,
                years_with_data=24,
                total_samples=8760,
                metrics=[
                    Metric(name='NSE', target=0.5, weight=1.0),
                    Metric(name='logNSE', target=0.5, weight=1.0),
                    Metric(name='Pbias', target=10.0, weight=1.0),
                ],
                derived_from=[]
            ),
        ],
        comments='USGS flow monitoring station'
    )

    wq_station = Station(
        station_id='S002-118',
        station_origin='equis',
        repository_name='Clearwater',
        true_reach_id=650,
        observations=[
            Observation(
                constituent='TP',
                start_year=2005,
                end_year=2020,
                avg_samples_per_year=12.0,
                years_with_data=15,
                total_samples=180,
                metrics=[
                    Metric(name='Pbias', target=25.0, weight=1.0),
                ],
                derived_from=[]
            ),
            Observation(
                constituent='TP_load',
                metrics=[
                    Metric(name='Pbias', target=30.0, weight=1.0),
                ],
                derived_from=['TP', 'Q']  # Derived from TP concentration and Q flow
            ),
        ],
        comments='MPCA water quality station'
    )

    # Define a calibration location with reach mappings and flow station refs
    # Note: negative reach_ids can be used to indicate reaches to subtract
    location = Location(
        location_id=1,
        location_name='Clearwater Outlet',
        repository_name='Clearwater',
        reach_ids=[650],                    # Model reaches for this location
        upstream_reach_ids=[649, 648],       # Upstream reaches for loading calcs
        flow_station_ids=['E66050001'],      # Flow station for load calculations
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
                        target_rate=0.1
                    ),
                    LandcoverConstraint(
                        landcover_type='agricultural',
                        constituent='TP',
                        target_rate=0.8
                    ),
                ]
            ),
        ],
        notes='Main calibration outlet'
    )

    # Create the full configuration
    config = CalibrationConfig(
        repository_name='Clearwater',
        locations=[location],
        default_metrics=[
            Metric(name='NSE', target=0.5),
            Metric(name='Pbias', target=10.0),
        ],
        general_constraints=[],  # Placeholder for future general model constraints
        version='1.0'
    )

    # 2. Save to SQLite database
    # --------------------------
    
    db_path = Path('calibration.sqlite')
    
    # Initialize the database (creates calibration tables)
    init_calibration_db(db_path, reset=True)
    
    # Save the configuration
    save_config_to_db(config, db_path)
    
    print(f"Saved configuration to {db_path}")
    
    # 3. Load from database
    # ---------------------
    loaded_config = load_config_from_db(db_path, 'Clearwater')
    
    print(f"Loaded configuration: {loaded_config.repository_name}")
    print(f"  Locations: {len(loaded_config.locations)}")
    for loc in loaded_config.locations:
        print(f"    - {loc.location_name}: {len(loc.stations)} stations")
        print(f"      Reach IDs: {loc.reach_ids}")
        print(f"      Flow stations: {loc.flow_station_ids}")


def example_using_manager():
    """
    Example: Using CalibrationManager for convenient access.
    """
    
    # CalibrationManager provides a unified interface
    manager = CalibrationManager(
        repository_name='Clearwater',
        db_path='calibration.sqlite',
        config_path='calibration.yaml'
    )
    
    # Create example configuration and set it
    example_config = create_example_config('Clearwater')
    manager._config = example_config
    
    # Save to both file and database
    manager.save(to_file=True, to_db=True)
    
    # Get data as DataFrames for analysis
    locations_df = manager.get_locations_as_dataframe()
    stations_df = manager.get_stations_as_dataframe()
    
    print("Locations DataFrame:")
    print(locations_df)
    print("\nStations DataFrame:")
    print(stations_df)


def example_from_yaml():
    """
    Example: Load configuration from YAML file and save to database.
    """
    
    # Load from YAML
    config = load_config('calibration_config_template.yaml')
    
    # Save to database
    db_path = Path('from_yaml.sqlite')
    init_calibration_db(db_path, reset=True)
    save_config_to_db(config, db_path)
    
    print(f"Configuration loaded from YAML and saved to {db_path}")


if __name__ == '__main__':
    # Run a simple example without database (to demonstrate file operations)
    print("Creating example configuration...")
    config = create_example_config('TestRepo')
    
    print(f"\nConfiguration: {config.repository_name}")
    print(f"Version: {config.version}")
    print(f"Locations: {len(config.locations)}")
    
    for loc in config.locations:
        print(f"\n  Location: {loc.location_name}")
        print(f"    Reach IDs: {loc.reach_ids}")
        print(f"    Upstream Reach IDs: {loc.upstream_reach_ids}")
        print(f"    Flow Station IDs: {loc.flow_station_ids}")
        print(f"    Stations: {len(loc.stations)}")
        for sta in loc.stations:
            print(f"      - {sta.station_id} ({sta.station_origin})")
            print(f"        Observations: {[o.constituent for o in sta.observations]}")
    
    # Save to YAML
    with tempfile.TemporaryDirectory() as tmpdir:
        yaml_path = Path(tmpdir) / 'config.yaml'
        save_config(config, yaml_path)
        print(f"\nSaved to YAML: {yaml_path}")
        
        # Load back
        loaded = load_config(yaml_path)
        print(f"Loaded from YAML: {loaded.repository_name}")
    
    print("\n--- CalibrationManager Example ---")
    manager = CalibrationManager(repository_name='Example')
    # Set the example config
    manager._config = create_example_config('Example')
    
    print("\nLocations DataFrame:")
    print(manager.get_locations_as_dataframe())
    
    print("\nStations DataFrame:")
    print(manager.get_stations_as_dataframe())
