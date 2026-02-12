# Example: Building DuckDB Database for Calibration Configuration
# ================================================================
# This example demonstrates how to create and use a DuckDB database
# to store calibration configurations for HSPF hydrologic models.

from pathlib import Path
import tempfile

from mpcaHydro.calibration_config import (
    CalibrationConfig,
    CalibrationManager,
    Location,
    Station,
    ConstituentConfig,
    Observation,
    Metric,
    WatershedConstraint,
    LandcoverConstraint,
    create_example_config,
    save_config,
    load_config,
    init_calibration_db,
    save_config_to_db,
    load_config_from_db
)


def example_build_database():
    """
    Example: Build a DuckDB database with calibration configuration.
    
    This integrates with the existing outlets schema from mpcaHydro.outlets.
    """
    
    # 1. Create a calibration configuration programmatically
    # -------------------------------------------------------
    
    # Define stations at the calibration location
    flow_station = Station(
        station_id='E66050001',
        station_origin='wiski',
        repository_name='Clearwater',
        true_reach_id=650,
        constituents=[
            ConstituentConfig(
                name='Q',
                metrics=[
                    Metric(name='NSE', target=0.5, weight=1.0),
                    Metric(name='logNSE', target=0.5, weight=1.0),
                    Metric(name='Pbias', target=10.0, weight=1.0),
                ],
                derived_from=[]
            ),
        ],
        observations=[
            Observation(
                constituent='Q',
                start_year=2000,
                end_year=2023,
                avg_samples_per_year=365.0,
                years_with_data=24,
                total_samples=8760
            ),
        ],
        comments='USGS flow monitoring station'
    )

    wq_station = Station(
        station_id='S002-118',
        station_origin='equis',
        repository_name='Clearwater',
        true_reach_id=650,
        constituents=[
            ConstituentConfig(
                name='TP',
                metrics=[
                    Metric(name='Pbias', target=25.0, weight=1.0),
                ],
                derived_from=[]
            ),
            ConstituentConfig(
                name='TP_load',
                metrics=[
                    Metric(name='Pbias', target=30.0, weight=1.0),
                ],
                derived_from=['TP', 'Q']  # Derived from TP concentration and Q flow
            ),
        ],
        observations=[
            Observation(
                constituent='TP',
                start_year=2005,
                end_year=2020,
                avg_samples_per_year=12.0,
                years_with_data=15,
                total_samples=180
            ),
        ],
        comments='MPCA water quality station'
    )

    # Define a calibration location with reach mappings and flow station refs
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
        version='1.0'
    )

    # 2. Save to DuckDB database
    # --------------------------
    # Note: This requires the outlets module data files to be present
    
    db_path = Path('calibration.duckdb')
    
    # Initialize the database (creates outlets schema + calibration tables)
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
        db_path='calibration.duckdb',
        config_path='calibration.yaml'
    )
    
    # Create example configuration
    manager.create_example_config()
    
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
    db_path = Path('from_yaml.duckdb')
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
            print(f"        Constituents: {[c.name for c in sta.constituents]}")
    
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
    manager.create_example_config()
    
    print("\nLocations DataFrame:")
    print(manager.get_locations_as_dataframe())
    
    print("\nStations DataFrame:")
    print(manager.get_stations_as_dataframe())
