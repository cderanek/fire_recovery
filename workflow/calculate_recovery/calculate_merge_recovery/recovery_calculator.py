import xarray as xr
import pandas as pd

from typing import Callable, Tuple, Dict, Union
import warnings
from functools import reduce


def calculate_ndvi_thresholds(
    combined_ndvi: xr.DataArray,
    config: dict) -> List[Union[xr.DataArray, pd.DataFrame]]:
    ''' returns ndvi_thresholds_da, summary_df 
    '''
    # Sort by time, organize the dimensions
    ndvi_da = ndvi_da.sortby('time')
    ndvi_da = ndvi_da.transpose('time', 'y', 'x')
    
    # Calculate the summary DF with lower/upper limits by matched groups
    grouping_band_format = config['RECOVERY_PARAMS']['GROUPING_BAND_FORMAT']
    nlcd_vegcode_df = pd.read_csv(config['BASELAYERS']['GROUPINGS']['summary_csv'])
    summary_df = create_summary_csv(ndvi_da=ndvi_da,
                                    nlcd_vegcode_df=nlcd_vegcode_df,
                                    grouping_band_format=grouping_band_format
                                    )
    
    # Ensure the threshold DataFrame is indexed by time and groups
    summary_df.set_index(['time', 'groups'], inplace=True)

    ## Create the empty xarray dataarray to hold the threshold layers for each data
    thresholds_da = ndvi_da.copy(deep=True)
    thresholds_da.data = np.full(thresholds_da.data.shape, dtype='float32', fill_value=np.nan)
    thresholds_da = thresholds_da.rename('thresholds')
    nodata = config['LANDSAT']['DEFAULT_NODATA']

    ## Use the time and UID from the DataArray to fill corresponding threshold values in the thresholds data array
    for (t, uid), row in summary_df[summary_df['Masked']=='UNDISTURBED'].iterrows():
        curr_threshold = row['lower']
        
        if curr_threshold>0:
            # Case where we are above the current threshold -> Set to 1
            thresholds_da.sel(time=t).data[:] = np.where(
                (thresholds_da.groups==uid) & (ndvi_da.sel(time=t)>=curr_threshold), 
                1, 
                thresholds_da.sel(time=t))
            
            # Case where we are below the current threshold -> Set to 0
            thresholds_da.sel(time=t).data[:] = np.where(
                (thresholds_da.groups==uid) & (ndvi_da.sel(time=t)<curr_threshold), 
                0, 
                thresholds_da.sel(time=t))
            
            # Case where we are missing NDVI data -> Set to nodata
            thresholds_da.sel(time=t).data[:] = np.where(
                (thresholds_da.groups==uid) & (np.isnan(ndvi_da.sel(time=t)) | (ndvi_da.sel(time=t)==nodata)), 
                nodata, 
                thresholds_da.sel(time=t)
            )
            
            # Case where we have too few undisturbed pixels -> Set to nodata
            if row['Count'] < config['RECOVERY_PARAMS']['MIN_NUM_MATCHED_PIXELS']:
                thresholds_da.sel(time=t).data[:] = np.where(
                    (thresholds_da.groups==uid), 
                    nodata, 
                    thresholds_da.sel(time=t)
                )

            
    # Update data array to have a threshold variable
    ndvi_da['threshold'] = thresholds_da
    
    # Sort by time, organize the dimensions∂
    ndvi_da = ndvi_da.sortby('time')
    ndvi_da = ndvi_da.transpose('time', 'y', 'x')
    
    return ndvi_da, summary_df


def calculate_recovery_time(
    ndvi_thresholds_da: xr.DataArray, 
    config: dict,
    verbose: bool=True):

    nodata = config['LANDSAT']['DEFAULT_NODATA']
    min_seasons = config['RECOVERY_PARAMS']['MIN_SEASONS']

    if verbose: 
        print('Calculating recovery')
        print(ndvi_threshold_da)
    
    # Start at the time of the fire
    fire_date = pd.to_datetime([ndvi_threshold_da.attrs['fire_date']], format=ndvi_threshold_da.attrs['fire_date_format'])[0]
    threshold_data_postfire = ndvi_threshold_da.threshold.sel(time=slice(fire_date, pd.Timestamp.now()))
    
    if verbose: 
        print('threshold data postfire')
        print(threshold_data_postfire)
    
    # Calculate the rolling nanmeans.
    # Replace all nodata vals with np.nan for the rolling nanmeans
    threshold_data_postfire.data = np.where(threshold_data_postfire.data==nodata, np.nan, threshold_data_postfire.data)
    
    # Where recovered, the nanmean of the previous N seasons will be 1, with no fewer than N-1 datapoints existing
    rolling_nanmeans = (
        threshold_data_postfire
        .rolling(time=min_seasons, min_periods=min_seasons-1)
        .mean()
    )
    recovered = (rolling_nanmeans == 1) # recovered when the mean of the previous 4 time periods is exactly 1
    recovery_num_seasons = recovered.argmax(dim='time') # get the first date where rolling_nanmeans==1 is True
    
    # replace all 0 values (never reached recovery) with nans
    recovery_num_seasons_data = np.where(recovery_num_seasons==0, np.nan, recovery_num_seasons) 

    if verbose:
        print('recovery time np array')
        print(recovery_num_seasons_data)

    ndvi_threshold_da.coords['fire_recovery_time'] = (recovery_num_seasons.dims, recovery_num_seasons_data)

    if verbose: 
        print('Calculated recovery. Final fire biocube:')
        print(ndvi_threshold_da)
        
    return ndvi_threshold_da


def single_fire_recoverytime_summary(
    recovery_da, 
    config,
    fire_metadata,
    file_paths):
    '''
    FIRE RECOVERY CSV FORMAT
    Fire Name | Fire Date | Recovery time (# seasons) | Burn Severity | Vegetation Type | Elevation | ndvi_grouping | pixel_count | Colors
    ----------------------------------------------------------------------------------------------------------------------------------------
    
    each row is 1 recovery time/burn severity/groups grouping with the associated # of pixels in that grouping, 
    where np.inf is used to denote pixels that never recover
    (after masking all pixels with future disturbances,  ag/dev, poor temporal coverage or not enoguh matched pixels)
    '''                
    colors_dict = {
    "Low": "#fdf20a",
    "Medium": "#fcc704",
    "High": "#c60e02",
    }

    # Organize fire & file data
    fire_name = fire_metadata['FIRE_NAME']
    fire_date = fire_metadata['FIRE_DATE']
    out_csv = file_paths['RECOVERY_COUNTS_SUMMARY_CSV']
    nlcd_vegcode_df = pd.read_csv(config['BASELAYERS']['GROUPINGS']['summary_csv'])
                    
    # Open recovery_rxr obj and mask all ag/dev and future disturbances
    # Currently, all unrecovered pixels are nans, set them to np.inf, and then mask bad pixels as nans
    recovery_data = np.where(
        np.isnan(recovery_da['fire_recovery_time'].data), 
        np.inf,
        recovery_da['fire_recovery_time'].data)
    
    recovery_data = np.where(
        (recovery_da['future_dist_agdev_mask'].data > 0) | (recovery_da['temporal_coverage_qa'].data > 0) | (recovery_da['matched_group_temporal_coverage_qa'].data > 0), 
        np.nan, 
        recovery_da['fire_recovery_time'].data)
    
    data = pd.DataFrame(
        {"fire": fire_name,
        "fire_date": fire_date,
        'fire_acr': fire_metadata['FIRE_ACR'],
        "recovery_num_seasons": recovery_data.flatten(),
        "severity": recovery_da['severity'].data.flatten(), 
        "groups": recovery_da['groups'].data.flatten()
        }
    ).astype({
        "fire": 'str',
        "fire_date": 'datetime64[ns]',
        "recovery_num_seasons": 'float64',
        "severity": 'float64', 
        "groups": 'int'
        }
    ).dropna(subset=['groups', 'recovery_num_seasons'])
    
    # Format data
    ## Extract elevation, vegetation type, ndvi grouping from grouping code
    data = extract_group_vals(data, grouping_band_format, nlcd_vegcode_df)
    
    ## Update severity to have more readable names 
    data["severity"] = (
        data["severity"]
            .replace(0, np.nan)
            .replace(2, "Low")
            .replace(3, "Medium")
            .replace(4, "High")
            .astype("category")
            .cat.set_categories(['Low', 'Medium', 'High'], ordered=True)
    )
    
    ## Add colors based on severity
    data['colors'] = data['severity'].apply(lambda x: colors_dict[x])
    
    ## Group by all columns and count pixels for each group, recovery time
    data = (data.value_counts()
            .reset_index(name='count')
            .dropna()
            )
    
    # Write summary CSV
    data.to_csv(out_csv, mode='w', index=False, header=True)
        
    return None


def create_summary_csv(
    ndvi_da: xr.DataArray,
    nlcd_vegcode_df: pd.DataFrame,
    grouping_band_format: dict
    ) -> pd.DataFrame:

    """
    Return NDVI summary statistics using the NDVI data array with a groups coordinate.
    
    Params:
    ndvi_da : xr.DataArray
        NDVI DataArray with coordinates including groups
    nlcd_vegcode_df : pd dataframe mapping NLCD codes to vegetation names
    
    Returns:
    pd.DataFrame
        DataFrame with format:
        time | Month | Year | veg_elev_id | Elevation | Vegetation_Name | Masked | 
        10pctl | 50pctl | 90pctl | Std | Count | lower | upper
    
    Notes:
    - The groups are the group codes for each matched pixel group
    - The Masked options are: 'ALL', 'LOW_SEV', 'MED_SEV', 'HIGH_SEV', 'UNDISTURBED'
    - The lower and upper columns are the confidence interval bounds from undisturbed pixels
    """
    all_dfs = []
    print(f'Calculating summary thresholds for:\n{ndvi_da}')
    
    # Calcualte the percentiles
    for pctl in [10, 50, 90]:
        pctl_str = f"{pctl}pctl"
        out_df = single_reduct_summary(ndvi_da, np.nanpercentile, pctl_str, q=pctl)
        all_dfs.append(out_df)

    # Calculate the std
    all_dfs.append(single_reduct_summary(ndvi_da, np.nanstd, 'Std'))

    # Calculate the counts (replace NaNs with zeros first)
    nans_replaced = ndvi_da.copy()
    nans_replaced.data = np.where(np.isnan(nans_replaced.data), 0, nans_replaced.data)
    all_dfs.append(single_reduct_summary(nans_replaced, np.count_nonzero, 'Count'))

    # Merge into single summary CSV
    merge_on = ['time', 'groups', 'Masked']
    summary_df = reduce(lambda  left,right: pd.merge(left,right,on=merge_on),all_dfs)
    
    # Extract elevation, vegetation type/name, ndvi from the 'groups' code
    summary_df = extract_group_vals(summary_df, grouping_band_format, nlcd_vegcode_df)
    
    # Calculate month, year from the time column
    summary_df['Month'] = summary_df['time'].apply(lambda t: t.month)
    summary_df['Year'] = summary_df['time'].apply(lambda t: t.year)
    
    # Calculate lower and upper bounds as median +/- std for each group; 
    # in calculate_threshold, we'll only use the values from undisturbed pixels
    summary_df['lower'] = summary_df['50pctl'] - summary_df['Std']
    summary_df['upper'] = summary_df['50pctl'] + summary_df['Std']
    
    return summary_df


def extract_group_vals(summary_df: pd.DataFrame,
                       grouping_band_format: dict,
                       nlcd_vegcode_df: pd.DataFrame
                       ) -> pd.DataFrame:
                       
    # Get the pattern and associated column names
    pattern = grouping_band_format['pattern']
    col_names = grouping_band_format['groups']
    expected_len = grouping_band_format['digits']
    
    # Extract group vals and create new columns with individual group val values
    extracted_group_vals = (summary_df['groups'].astype(str)
                            .apply(lambda s: s.zfill(expected_len))
                            .str.extract(pattern))
    summary_df[col_names] = extracted_group_vals
        
    # Ensure that the output cols are integer types
    for col in col_names:
        summary_df[col] = summary_df[col].astype('Int64') # int64 can handle nans
        
    # Add vegetation names and elevation bands
    lookup_dict = nlcd_vegcode_df.set_index('id')[['NLCD_NAME', 'ELEV_LOWER_BOUND']].to_dict('index')
    summary_df[['Vegetation_Name', 'Elevation']] = summary_df['veg_elev_id'].map(
        lambda id: pd.Series([lookup_dict[id]['NLCD_NAME'], lookup_dict[id]['ELEV_LOWER_BOUND']])
        )   
    return summary_df


def single_reduct_summary(
    ndvi_da:xr.DataArray, 
    reducer: Callable, 
    reducer_name:str,
    **kwargs
    ) -> pd.DataFrame:
    """
    Calculate per-pixel and per-date summary statistics on NDVI DataArray.
    
    Params:
    ndvi_da : xr.DataArray
        NDVI DataArray with coordinates: time, y, x, groups, severity, dist_mask
        and variable NDVI
    reducer : Callable
        Function to reduce the data (e.g., np.nanpercentile, np.nanstd)
    reducer_name : str
        Name for the column containing reduced values
    **kwargs
        Additional arguments to pass to the reducer function
    
    Returns:
    pd.DataFrame
        DataFrame with format:
        time | Month | Year | groups | Elevation | Vegetation_Name | Masked | reducer_name
    """
    sev_class_dict = {2: 'LOW_SEV', 3: 'MED_SEV', 4: 'HIGH_SEV'}
    
    # Calculate the specified reducer over different groupings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=RuntimeWarning)
         # all pixels by group [both disturbned and undisturbed pixels]
        reduct_all = (
            ndvi_da.groupby('groups')
            .reduce(
                reducer, dim=('stacked_y_x'), **kwargs)
            )
         # all burned pixels by group and burn severity
        reduct_by_sev = (
            ndvi_da.groupby(['groups', 'severity'])
            .reduce(
                reducer, dim=('stacked_y_x'), **kwargs
                )
            )
        # all undisturbed pixels by group [this is what's used for the confidence interval]
        reduct_by_dist = (
            ndvi_da.groupby(['groups', 'dist_mask'])
            .reduce(
                reducer, dim=('stacked_y_x'), **kwargs
                )
            )

    # Format reduced values into single pd df
    reduct_all_df = reformat_reduct_da(reduct_all, reducer_name)
    reduct_by_sev_df = reformat_reduct_da(reduct_by_sev, reducer_name)
    reduct_by_dist_df = reformat_reduct_da(reduct_by_dist, reducer_name)
    
    # Define masked values for each group (ALL, by severity, UNDISTURBED)
    reduct_all_df['Masked'] = 'ALL'                    
    
    reduct_by_sev_df['Masked'] = (
        reduct_by_sev_df['severity']
        .map(lambda x: sev_class_dict.get(x, np.nan))
    )
    reduct_by_sev_df = reduct_by_sev_df.dropna(subset=['Masked'])
    reduct_by_sev_df = reduct_by_sev_df.drop(columns='severity')              
    
    reduct_by_dist_df['Masked'] = reduct_by_dist_df['dist_mask'].map(
        lambda x: 'UNDISTURBED' if x == 0 else np.nan
    )
    reduct_by_dist_df = reduct_by_dist_df.dropna(subset=['Masked'])
    reduct_by_dist_df = reduct_by_dist_df.drop(columns='dist_mask')
    
    # Concatenate summary for all 3 groups    
    return pd.concat([reduct_all_df, reduct_by_sev_df, reduct_by_dist_df])


def reformat_reduct_da(reduct_df: xr.DataArray, 
                       reducer_name: str
                       ) -> pd.DataFrame:
    """Convert a reduced DataArray to a DataFrame with column name based on the reducer used."""
    return (
        reduct_df
        .to_dataframe(name=reducer_name)
        .drop(columns='spatial_ref')
        .reset_index()
    )