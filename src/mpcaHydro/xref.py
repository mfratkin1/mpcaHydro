"""
xref
====

Cross-reference lookup between WISKI and EQuIS station identifiers.

Overview
--------
Monitoring stations are often represented in **both** WISKI (continuous
sensor data) and EQuIS (lab grab-sample data), but under different
identifiers.  The cross-reference table
(``data/WISKI_EQUIS_XREF.csv``) stores these associations along with
a Watershed Identifier (``WID``) column.

This module provides simple lookup functions so that calling code can
translate between the two naming systems:

* **WISKI → EQuIS**: Find the EQuIS station(s) that correspond to a
  WISKI station (and vice-versa).
* **Alias lookups**: Find the single "alias" identifier that directly
  links WISKI and EQuIS records (``WISKI_EQUIS_ID`` column).
* **WID-based lookups**: Retrieve stations from either system using a
  Watershed Identifier.

When to use this module
-----------------------
Use ``xref`` whenever you need to merge or compare data from WISKI and
EQuIS for the same physical location, or when building outlet mappings
that include stations from both systems.

Key data structures
-------------------
``WISKI_EQUIS_XREF``
    A :class:`pandas.DataFrame` loaded from the bundled CSV file.
    Columns include ``WISKI_STATION_NO``, ``EQUIS_STATION_ID``,
    ``WISKI_EQUIS_ID``, and ``WID``.
"""

import pandas as pd
from pathlib import Path

WISKI_EQUIS_XREF = pd.read_csv(Path(__file__).parent/'data/WISKI_EQUIS_XREF.csv')
#WISKI_EQUIS_XREF = pd.read_csv('C:/Users/mfratki/Documents/GitHub/hspf_tools/WISKI_EQUIS_XREF.csv')


def are_lists_identical(nested_list):
    """Check whether all sub-lists in *nested_list* contain the same elements.

    Each sub-list is sorted before comparison so that element order does
    not matter.

    Parameters
    ----------
    nested_list : list of list
        A list of sub-lists to compare.

    Returns
    -------
    bool
        ``True`` if every sub-list is identical (after sorting).
    """
    # Sort each sublist
    sorted_sublists = [sorted(sublist) for sublist in nested_list]
    # Compare all sublists to the first one
    return all(sublist == sorted_sublists[0] for sublist in sorted_sublists)                                                                                               

def get_wiski_stations():
    """Return all unique WISKI station numbers in the cross-reference table.

    Returns
    -------
    list of str
    """
    return list(WISKI_EQUIS_XREF['WISKI_STATION_NO'].unique())

def get_equis_stations():
    """Return all unique EQuIS station IDs in the cross-reference table.

    Returns
    -------
    list of str
    """
    return list(WISKI_EQUIS_XREF['EQUIS_STATION_ID'].unique())

def wiski_equis_alias(wiski_station_id):
    """Return the single EQuIS alias (``WISKI_EQUIS_ID``) for a WISKI station.

    An alias represents a direct 1-to-1 link between a WISKI and an
    EQuIS record in the cross-reference table.  If no alias exists an
    empty list is returned; if multiple aliases exist an error is raised
    because the mapping is expected to be unique.

    Parameters
    ----------
    wiski_station_id : str
        WISKI station number.

    Returns
    -------
    str or list
        The EQuIS alias string, or ``[]`` if none is found.

    Raises
    ------
    Exception
        If more than one alias is found (ambiguous mapping).
    """
    equis_ids =  list(set(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['WISKI_STATION_NO'] == wiski_station_id,'WISKI_EQUIS_ID'].to_list()))
    equis_ids = [equis_id for equis_id in equis_ids if not pd.isna(equis_id)]
    if len(equis_ids) == 0:
        return []
    elif len(equis_ids) > 1:
        print(f'Too Many Equis Stations for {wiski_station_id}')
        raise 
    else:
        return equis_ids[0]

def wiski_equis_associations(wiski_station_id):
    """Return all EQuIS station IDs associated with a WISKI station.

    Unlike :func:`wiski_equis_alias`, this returns all associations
    (not just the direct alias), which may include multiple EQuIS
    stations.

    Parameters
    ----------
    wiski_station_id : str
        WISKI station number.

    Returns
    -------
    list of str
        EQuIS station IDs, or ``[]`` if none found.
    """
    equis_ids =  list(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['WISKI_STATION_NO'] == wiski_station_id,'EQUIS_STATION_ID'].unique())
    equis_ids =  [equis_id for equis_id in equis_ids if not pd.isna(equis_id)]
    if len(equis_ids) == 0:
        return []
    else:
        return equis_ids
    
def equis_wiski_associations(equis_station_id):
    """Return all WISKI station numbers associated with an EQuIS station.

    Parameters
    ----------
    equis_station_id : str
        EQuIS ``SYS_LOC_CODE``.

    Returns
    -------
    list of str
        WISKI station numbers, or ``[]`` if none found.
    """
    wiski_ids = list(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['EQUIS_STATION_ID'] == equis_station_id,'WISKI_STATION_NO'].unique())
    wiski_ids = [wiski_id for wiski_id in wiski_ids if not pd.isna(wiski_id)]
    if len(wiski_ids) == 0:
        return []
    else:
        return wiski_ids
    
def equis_wiski_alias(equis_station_id):
    """Return the single WISKI station number for an EQuIS alias.

    Parameters
    ----------
    equis_station_id : str
        The ``WISKI_EQUIS_ID`` alias value.

    Returns
    -------
    str or list
        The WISKI station number, or ``[]`` if none found.

    Raises
    ------
    ValueError
        If more than one WISKI station matches (ambiguous mapping).
    """
    wiski_ids =  list(set(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['WISKI_EQUIS_ID'] == equis_station_id,'WISKI_STATION_NO'].to_list()))
    wiski_ids = [wiski_id for wiski_id in wiski_ids if not pd.isna(wiski_id)]
    if len(wiski_ids) == 0:
        return []
    elif len(wiski_ids) > 1:
        print(f'Too Many WISKI Stations for {equis_station_id}')
        raise ValueError(f'Too Many WISKI Stations for {equis_station_id}')
    else:
        return wiski_ids[0]

def _equis_wiski_associations(equis_station_ids):
    """Return WISKI associations shared by all given EQuIS stations.

    If all EQuIS station IDs map to the same set of WISKI stations, that
    set is returned.  Otherwise an empty list is returned.

    Parameters
    ----------
    equis_station_ids : list of str
        One or more EQuIS station IDs.

    Returns
    -------
    list of str
        Common WISKI station numbers, or ``[]``.
    """
    wiski_stations = [equis_wiski_associations(equis_station_id) for equis_station_id in equis_station_ids]
    if are_lists_identical(wiski_stations):
        return wiski_stations[0]
    else:
        return []
        
def _stations_by_wid(wid_no, station_origin):
    """Return station IDs associated with a Watershed Identifier (WID).

    Parameters
    ----------
    wid_no : str or int
        Watershed Identifier value.
    station_origin : str
        One of ``'wiski'``, ``'wplmn'`` (mapped to WISKI column) or
        ``'equis'``, ``'swd'`` (mapped to EQuIS column).

    Returns
    -------
    list of str
        Matching station identifiers.

    Raises
    ------
    Exception
        If *station_origin* is not a recognised value.
    """
    if station_origin in ['wiski','wplmn']:
        station_col = 'WISKI_STATION_NO'
    elif station_origin in ['equis','swd']:
        station_col = 'EQUIS_STATION_ID'
    else:
        raise
        
    return list(WISKI_EQUIS_XREF.loc[WISKI_EQUIS_XREF['WID'] == wid_no,station_col].unique())

