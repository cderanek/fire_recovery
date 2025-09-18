import xarray as xr
import numpy as np
import pandas as pd

def temporal_coverage_check(
    ndvi_thresholds_da: xr.DataArray, 
    config: dict, 
    fire_metadata: dict):
    '''Takes in ndvi_thresholds_da xr.DataArray and flags any pixels with too many missing dates, 
        as defined by:
        1. having fewer than <config['MIN_TEMPORAL_COVERAGE_RATIO'] coverage over the past
        config['YRS_PREFIRE_MATCHED'] years and 10 years post-fire. 
        2. any matched pixel group which, after doing a per-date matched_pixel_coverage_check, 
        has fewer than <config['MIN_TEMPORAL_COVERAGE_RATIO'] coverage over the past 
        config['YRS_PREFIRE_MATCHED'] years and 10 years post-fire. 
        
        Adds the new QA layer as a coordinate to ndvi_thresholds_da.
        The new layers are called 
        1. temporal_coverage_qa
        2. matched_group_temporal_coverage_qa
        , and are set to 1 for values to mask, and 0 for values to keep.
    '''
    # Filter NDVI time series to only have dates for YRS_PREFIRE_MATCHED
    fire_date = fire_metadata['FIRE_DATE']
    start_matching = fire_date - pd.Timedelta(weeks=52*config['RECOVERY_PARAMS']['YRS_PREFIRE_MATCHED'])
    end_matching = fire_date + pd.Timedelta(weeks=52*10)
    
    filtered_ndvi =  (
        ndvi_thresholds_da
        .sel(time=slice(start_matching, end_matching))
        .copy()
        )
    filtered_thresholds = (
        ndvi_thresholds_da.threshold
        .sel(time=slice(start_matching, end_matching))
        .copy()
        )
    
    # Count the number of nodata NDVI values during the time series (pixel-wise count)
    filtered_ndvi.data = np.where(
        (filtered_ndvi.data<=config['RECOVERY_PARAMS']['NDVI_LOWER_BOUND']) | (filtered_ndvi.data>config['RECOVERY_PARAMS']['NDVI_UPPER_BOUND']), 
        np.nan, 
        filtered_ndvi.data)
    count_ndvi_data = filtered_ndvi.count(dim='time')
        
    # Count the number of nodata threshold values during the time series (pixel-wise count) - reflects too few matched pixels, missing NDVI
    filtered_thresholds.data = np.where(
        (filtered_thresholds.data<0), 
        np.nan, 
        filtered_thresholds.data)
    count_thresholds_data = filtered_thresholds.count(dim='time')
    
    # Set the threshold for min number of dates with data
    # config['MIN_TEMPORAL_COVERAGE_RATIO'] * total number of possible date values
    # (total number of possible date values = shape of time dim)
    total_date_vals = len(filtered_ndvi.time)
    print(f'total_date_vals: {total_date_vals}')
    min_allowable_count = config['RECOVERY_PARAMS']['MIN_TEMPORAL_COVERAGE_RATIO'] * total_date_vals
    
    # Add mask layer to ndvi_thresholds_da
    # add mask for NDVI temporal coverage
    ndvi_thresholds_da['temporal_coverage_qa'] = (
        ndvi_thresholds_da['dist_mask'].dims,
        np.where(count_ndvi_data.data > min_allowable_count, 0, 1).squeeze().astype('int8')
    )
    ndvi_thresholds_da['matched_group_temporal_coverage_qa'] = (
        ndvi_thresholds_da['dist_mask'].dims,
        np.where(count_thresholds_data.data > min_allowable_count, 0, 1).squeeze().astype('int8')
    )
    
    return ndvi_thresholds_da