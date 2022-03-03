from opensky_api import OpenSkyApi
import pandas as pd
from typing import List, Tuple
from functools import lru_cache
import time

api = OpenSkyApi()
plane_key = "callsign"


ttl = 60
_cached_data: pd.DataFrame = None
_cached_data_time = 0

def _fetch_data() -> pd.DataFrame:

    states = api.get_states()
    if not states:
        return pd.DataFrame()

    # transform into dataframe
    df = pd.DataFrame([s.__dict__ for s in states.states])

    # remove empty callsign

    #df = df[not plane_key]

    # removing leading and trailling whitespace in callsign
    df[plane_key] = df[plane_key].apply(lambda x: x.strip())

    # by default pandas set  callsign column type to object
    df[plane_key] = df[plane_key].astype("string")

    return df


def fetch_data_with_cache() -> pd.DataFrame:
    '''not usable in multithread because cache (should use a lock)
    '''
    now = time.time()

    global _cached_data, _cached_data_time

    if _cached_data is None or now - _cached_data_time > ttl:

        _cached_data=_fetch_data()
        _cached_data_time = time.time()

    return _cached_data


def read_planes():
    filename='./planes.txt'
    with open(filename) as file:
        planes = [line.rstrip() for line in file]

    return planes


def search_planes(planes_callsign: List[str], df: pd.DataFrame) ->  pd.DataFrame:  
    return df.loc[df['callsign'].isin(planes_callsign)]


def get_positions(df: pd.DataFrame) -> List[Tuple[float, float]]:
    return list(zip(df['latitude'], df['longitude']))


