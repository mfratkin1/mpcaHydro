# -*- coding: utf-8 -*-
"""Integration tests for warehouse operations.

Note: Many tests require data files in src/mpcaHydro/data/ which may not be present.
These tests will be skipped if required files are missing.
"""

import tempfile
from pathlib import Path
import pytest
import pandas as pd


# Check if warehouse module can be imported (requires data files)
try:
    from mpcaHydro import warehouse
    WAREHOUSE_AVAILABLE = True
except Exception:
    WAREHOUSE_AVAILABLE = False
    warehouse = None

requires_warehouse = pytest.mark.skipif(
    not WAREHOUSE_AVAILABLE,
    reason="Warehouse module not available (missing data files)"
)


class TestWarehouseInitialization:
    """Tests for initializing the data warehouse."""

    @requires_warehouse
    def test_init_warehouse_creates_database(self):
        """Test that init_warehouse creates a database file."""
        from mpcaHydro import warehouse
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            warehouse.init_db(db_path.as_posix(), reset=True)
            
            assert db_path.exists()

    @requires_warehouse
    def test_init_warehouse_creates_schemas(self):
        """Test that init_warehouse creates required schemas."""
        from mpcaHydro import warehouse
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            warehouse.init_db(db_path.as_posix(), reset=True)
            
            with warehouse.connect(db_path.as_posix(), read_only=True) as con:
                schemas = con.execute(
                    "SELECT schema_name FROM information_schema.schemata"
                ).fetchall()
                schema_names = [s[0] for s in schemas]
                
                assert 'staging' in schema_names
                assert 'analytics' in schema_names
                assert 'mappings' in schema_names

    @requires_warehouse
    def test_init_warehouse_creates_staging_tables(self):
        """Test that init_warehouse creates staging tables."""
        from mpcaHydro import warehouse
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            warehouse.init_db(db_path.as_posix(), reset=True)
            
            with warehouse.connect(db_path.as_posix(), read_only=True) as con:
                tables = con.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'staging'"
                ).fetchall()
                table_names = [t[0] for t in tables]
                
                assert 'wiski' in table_names
                assert 'equis' in table_names

    @requires_warehouse
    def test_init_warehouse_creates_analytics_tables(self):
        """Test that init_warehouse creates analytics tables."""
        from mpcaHydro import warehouse
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            warehouse.init_db(db_path.as_posix(), reset=True)
            
            with warehouse.connect(db_path.as_posix(), read_only=True) as con:
                tables = con.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'analytics'"
                ).fetchall()
                table_names = [t[0] for t in tables]
                
                assert 'wiski' in table_names
                assert 'equis' in table_names

    @requires_warehouse
    def test_init_warehouse_reset_clears_data(self):
        """Test that reset=True clears existing database."""
        from mpcaHydro import warehouse
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            
            # First initialization
            warehouse.init_db(db_path.as_posix(), reset=True)
            
            # Insert some test data
            with warehouse.connect(db_path.as_posix(), read_only=False) as con:
                con.execute("INSERT INTO staging.wiski (station_no) VALUES ('TEST123')")
                count1 = con.execute("SELECT COUNT(*) FROM staging.wiski").fetchone()[0]
                assert count1 == 1
            
            # Reset and check data is cleared
            warehouse.init_db(db_path.as_posix(), reset=True)
            
            with warehouse.connect(db_path.as_posix(), read_only=True) as con:
                count2 = con.execute("SELECT COUNT(*) FROM staging.wiski").fetchone()[0]
                assert count2 == 0


class TestWarehouseConnection:
    """Tests for warehouse connection management."""

    @requires_warehouse
    def test_connect_creates_parent_directory(self):
        """Test that connect creates parent directories if needed."""
        from mpcaHydro import warehouse
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'subdir' / 'nested' / 'test.duckdb'
            
            # Initialize should create directories
            warehouse.init_db(db_path.as_posix(), reset=True)
            
            assert db_path.exists()

    @requires_warehouse
    def test_connect_read_only_mode(self):
        """Test that read_only mode prevents writes."""
        from mpcaHydro import warehouse
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            warehouse.init_db(db_path.as_posix(), reset=True)
            
            # Read-only connection should not allow INSERT
            with warehouse.connect(db_path.as_posix(), read_only=True) as con:
                with pytest.raises(Exception):
                    con.execute("INSERT INTO staging.wiski (station_no) VALUES ('TEST')")


class TestWarehouseDataLoading:
    """Tests for loading data into the warehouse."""

    @requires_warehouse
    def test_load_df_to_table(self):
        """Test loading a DataFrame into a table."""
        from mpcaHydro import warehouse
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            warehouse.init_db(db_path.as_posix(), reset=True)
            
            df = pd.DataFrame({
                'station_no': ['A001', 'A002'],
                'value': [1.0, 2.0]
            })
            
            with warehouse.connect(db_path.as_posix(), read_only=False) as con:
                warehouse.load_df_to_table(con, df, 'staging.test_table')
                
                result = con.execute("SELECT * FROM staging.test_table").fetchdf()
                assert len(result) == 2
                assert 'station_no' in result.columns

    @requires_warehouse
    def test_load_df_to_staging(self):
        """Test loading a DataFrame to staging schema."""
        from mpcaHydro import warehouse
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            warehouse.init_db(db_path.as_posix(), reset=True)
            
            df = pd.DataFrame({
                'id': [1, 2, 3],
                'name': ['a', 'b', 'c']
            })
            
            with warehouse.connect(db_path.as_posix(), read_only=False) as con:
                warehouse.load_df_to_staging(con, df, 'test_staging')
                
                result = con.execute("SELECT * FROM staging.test_staging").fetchdf()
                assert len(result) == 3


class TestWarehouseViews:
    """Tests for warehouse view operations."""

    @requires_warehouse
    def test_update_views_succeeds(self):
        """Test that update_views runs without error."""
        from mpcaHydro import warehouse
        
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / 'test.duckdb'
            warehouse.init_db(db_path.as_posix(), reset=True)
            
            with warehouse.connect(db_path.as_posix(), read_only=False) as con:
                # Should not raise
                warehouse.update_views(con)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
