# Integration Tests

This directory contains integration tests for the mpcaHydro package.

## Test Categories

### Network Tests (`@pytest.mark.network`)
These tests require network access to external services:
- WISKI web service (wiskiweb01.pca.state.mn.us)
- Oracle EQuIS database

To run network tests: `pytest -m network`

### Integration Tests (`@pytest.mark.integration`)
These tests require:
- Full package dependencies (geopandas, etc.)
- Data files in `src/mpcaHydro/data/`

To run integration tests: `pytest -m integration`

### Credential Tests (`@pytest.mark.credentials`)
These tests require environment variables:
- `ORACLE_USER`: Oracle database username
- `ORACLE_PASSWORD`: Oracle database password

## Running Tests

```bash
# Run all unit tests (exclude integration)
pytest tests/ --ignore=tests/integration/

# Run integration tests only
pytest tests/integration/ -m "integration"

# Run network tests (requires network access)
pytest tests/integration/ -m "network"

# Skip network tests
pytest tests/integration/ -m "not network"
```

## Missing Data Files

Some tests require data files that may not be present:
- `src/mpcaHydro/data/stations_wiski.gpkg`
- `src/mpcaHydro/data/stations_equis.gpkg`

These tests will be skipped if files are missing.
