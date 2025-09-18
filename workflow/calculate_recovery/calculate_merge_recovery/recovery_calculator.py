import xarray as xr
import pandas as pd

from typing import List, Union

def calculate_ndvi_thresholds(
    combined_ndvi: xr.DataArray,
    config: dict) -> List[Union[xr.DataArray, pd.DataFrame]]:

    # returns ndvi_thresholds_da, summary_df
    pass

def calculate_recovery_time(
    ndvi_thresholds_da: xr.DataArray, 
    min_seasons: int):
    pass

def single_fire_recoverytime_summary(
    recovery_da, 
    config,
    fire_metadata,
    file_paths):
    pass