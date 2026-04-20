"""Parquet file storage helpers. Does not replace warehouse — lives alongside it."""

from pathlib import Path
from typing import List
import pandas as pd
from mpcaHydro.sources import wiski, equis

DEFAULT_DATA_DIR = Path("data")


# Natural keys for each source — the columns that uniquely identify a record.
# Used to filter out duplicates when appending new downloads.
NATURAL_KEYS = {'wiski': ['ts_id', 'Timestamp'],
                'equis': ['SYS_LOC_CODE', 'SAMPLE_DATE']}


def staging_dir(data_dir: Path, source: str) -> Path:
    p = data_dir / "staging" / source
    p.mkdir(parents=True, exist_ok=True)
    return p

def derived_dir(data_dir: Path, name: str) -> Path:
    p = data_dir / "derived" / name
    p.mkdir(parents=True, exist_ok=True)
    return p


def staging_path(data_dir: Path, source: str, station_id: str) -> Path:
    return staging_dir(data_dir, source) / f"{station_id}.parquet"


def save_staging(
    df: pd.DataFrame,
    data_dir: Path,
    source: str,
    station_id: str,
) -> tuple[Path, int]:
    """Save raw download to parquet, deduplicating against existing data.
    
    Uses NATURAL_KEYS[source] to identify which rows are new.
    Returns (path, new_row_count).
    """
    if source not in NATURAL_KEYS:
        raise ValueError(f"Unknown source '{source}'. Known sources: {list(NATURAL_KEYS)}")

    keys = NATURAL_KEYS[source]
    path = staging_path(data_dir, source, station_id)

    if path.exists():
        df_existing = pd.read_parquet(path)
        # Filter to only genuinely new rows
        merged = df.merge(df_existing[keys], on=keys, how='left', indicator=True)
        df_new = merged[merged['_merge'] == 'left_only'].drop(columns='_merge')

        if df_new.empty:
            return path, 0

        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        new_count = len(df_new)
    else:
        df_combined = df
        new_count = len(df)

    df_combined.to_parquet(path, index=False)
    return path, new_count


def read_staging_glob(data_dir: Path, source: str) -> str:
    return (staging_dir(data_dir, source) / "*.parquet").as_posix()


def read_derived_glob(data_dir: Path, name: str) -> str:
    return (derived_dir(data_dir, name) / "*.parquet").as_posix()


def download_wiski_data(
    station_ids: List[str],
    start_year: int = 1996,
    end_year: int = 2030,
    data_dir: Path = DEFAULT_DATA_DIR,
    wplmn: bool = False
) -> None:
    """Download WISKI data for the given stations and save to staging, deduplicating against existing data."""
    
    keys = NATURAL_KEYS['wiski']
    for station_id in station_ids:
        df_new = wiski.download([station_id], start_year=start_year, end_year=end_year, wplmn=wplmn)

        if df_new.empty:
            print(f"No data for {station_id}")
            continue

        # Load what we already have for this station
        existing_path = staging_path(data_dir, 'wiski', station_id)

        if existing_path.exists():
            df_existing = pd.read_parquet(existing_path)

            merged = df_new.merge(
                df_existing[keys],
                on=keys,
                how='left',
                indicator=True
            )
            df_new = merged[merged['_merge'] == 'left_only'].drop(columns='_merge')

            if df_new.empty:
                print(f"{station_id}: no new data")
                continue

            # Append new rows to existing
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        else:
            df_combined = df_new

        df_combined.to_parquet(existing_path, index=False)
        print(f"{station_id}: added {len(df_new)} new rows ({len(df_combined)} total)")


        
def download_equis_data(
    station_ids: List[str],
    start_year: int = 1996,
    end_year: int = 2030,
    data_dir: Path = DEFAULT_DATA_DIR,
    wplmn: bool = False
) -> None:
    """Download EQUIS data for the given stations and save to staging, deduplicating against existing data."""
    
    keys = NATURAL_KEYS['equis']
    df_equis = equis.download(station_ids)

    if df_equis.empty:
        print("No data downloaded")

    else:
        for station_id in df_equis['SYS_LOC_CODE'].unique():
            df_new = df_equis[df_equis['SYS_LOC_CODE'] == station_id]
            
            # Load what we already have for this station
            existing_path = staging_path(data_dir, 'equis', station_id)

            if existing_path.exists():
                df_existing = pd.read_parquet(existing_path)

                merged = df_new.merge(
                    df_existing[keys],
                    on=keys,
                    how='left',
                    indicator=True
                )
                df_new = merged[merged['_merge'] == 'left_only'].drop(columns='_merge')

                if df_new.empty:
                    print(f"{station_id}: no new data")
                    continue

                # Append new rows to existing
                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            else:
                df_combined = df_new

            df_combined.to_parquet(existing_path, index=False)
            print(f"{station_id}: added {len(df_new)} new rows ({len(df_combined)} total)")




