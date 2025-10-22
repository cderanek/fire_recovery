import gc, glob, os, sys
import numpy as np
import pandas as pd
import xarray as xr
import rioxarray as rxr
import geopandas as gpd
from shapely.geometry import box

from typing import List, Tuple, Union

sys.path.append("workflow/utils/")
from geo_utils import reproj_align_rasters
sys.path.append("workflow/calculate_recovery/make_plots/")
from recovery_plots import create_density_plot


def open_align_fire_rasters(
        target_raster: xr.DataArray,
        config: dict,
        fire_metadata: dict,
        file_paths: dict
    ) -> List[xr.DataArray]:
    '''
    Output list of aligned, reprojected rioxarray DataArrays:
        EVT, elevation, any_dist, burn severity over the fire+buffer area.
    '''
    # Get the bounding box around the LS image for clipping the base layers to a more manageable size with lazy loading below
    xmin, ymin, xmax, ymax = target_raster.rio.bounds()
    template_box = gpd.GeoDataFrame(
        geometry=[box(xmin, ymin, xmax, ymax)], 
        crs=target_raster.rio.crs
    )

    # Open fire severity
    sev_rxr = rxr.open_rasterio(file_paths['BASELAYERS']['severity'], chunks='auto').isel(band=0)

    # Ag/dev mask: open and clip to bbox
    agdev_mask_rxr = rxr.open_rasterio(file_paths['BASELAYERS']['agdev_mask'], chunks='auto').isel(band=0)
    curr_crs = agdev_mask_rxr.rio.crs
    xmin, ymin, xmax, ymax = template_box.to_crs(curr_crs).geometry.iloc[0].bounds # get the bbox
    agdev_mask_rxr = agdev_mask_rxr.rio.clip_box(xmin, ymin, xmax, ymax)

    # Annual disturbance: open and clip to bbox
    dist_rxr =  xr.open_dataset(file_paths['BASELAYERS']['annual_dist'], format="NETCDF4", engine="netcdf4", chunks='auto')
    curr_crs = dist_rxr.spatial_ref.crs_wkt
    xmin, ymin, xmax, ymax = template_box.to_crs(curr_crs).geometry.iloc[0].bounds # get the bbox
    dist_rxr = (
        dist_rxr
        .rio.write_crs(curr_crs)
        .rio.clip_box(xmin, ymin, xmax, ymax)
    )
    dist_rxr, agdev_mask_rxr = reproj_align_rasters('reproj_match', dist_rxr, agdev_mask_rxr)

    # All-time disturbance: select latest year + add agdev mask
    cuml_dist_rxr = dist_rxr.cumulative_annual_dist.isel(time=-1).rio.write_crs(curr_crs)
    cuml_dist_data = np.append([cuml_dist_rxr.data], [agdev_mask_rxr.data], axis=0) # append ag/dev mask so that anything that is ag/dev (set to 1 on ag/dev mask) leads to that pixel having nanmax >0
    cuml_dist_data = np.nanmax(cuml_dist_data, axis=0)
    cuml_dist_rxr = cuml_dist_rxr.copy(data=cuml_dist_data)
    cuml_dist_rxr = cuml_dist_rxr.rio.set_nodata(0)

    # Pre and post-fire disturbance: cumulative over all dates pre or post-fire + add all-time agdev mask
    fire_year = int(fire_metadata['FIRE_DATE'].split('-')[0])
    last_pre_fire_date = np.datetime64(f'{fire_year - 1}-01-01')
    first_post_fire_date = np.datetime64(f'{fire_year + 1}-01-01') # need to add a year, because annual_disturbance date is set to Dec 31 of that year
    
    future_dist_rxr = dist_rxr.annual_dist.sel(
        time=slice(
            first_post_fire_date, 
            np.datetime64('today', 'D'))
        )

    future_dist_data = np.append(future_dist_rxr.data, [agdev_mask_rxr.data], axis=0) # append ag/dev mask so that anything that is ag/dev (set to 1 on ag/dev mask) leads to that pixel having nanmax >0
    future_dist_data = np.nanmax(future_dist_data, axis=0)
    future_dist_data = np.where(future_dist_data>0, 1, 0)
    future_dist_rxr = cuml_dist_rxr.copy(data=future_dist_data)
    future_dist_rxr = future_dist_rxr.rio.set_nodata(0)

    past_dist_rxr = dist_rxr.annual_dist.sel(
        time=slice(
            np.datetime64('1980-01-01'), 
            last_pre_fire_date)
        )
    past_dist_data = np.append(past_dist_rxr.data, [agdev_mask_rxr.data], axis=0) # append ag/dev mask so that anything that is ag/dev (set to 1 on ag/dev mask) leads to that pixel having nanmax >0
    past_dist_data = np.nanmax(past_dist_data, axis=0)
    past_dist_data = np.where(past_dist_data>0, 1, 0)
    past_dist_rxr = cuml_dist_rxr.copy(data=past_dist_data)
    past_dist_rxr = past_dist_rxr.rio.set_nodata(0)

    del future_dist_data, past_dist_data, dist_rxr
    gc.collect()

    # Matched pixel groupings: choose the latest pre-fire date of matched pixel groups and apply agriculture/development mask
    grouping_rxr = xr.open_dataset(file_paths['BASELAYERS']['groupings'], format="NETCDF4", engine="netcdf4", chunks='auto')
    grouping_rxr_crs = grouping_rxr.spatial_ref.crs_wkt
    grouping_rxr = grouping_rxr.__xarray_dataarray_variable__.rio.write_crs(grouping_rxr_crs)
    if fire_year == 1999: cutoff_date = np.datetime64('2000-01-01') # for fires in 1999, use the 1999 vegetation data
    else: cutoff_date = fire_metadata['FIRE_DATE']
    # Only select the latest date pre-fire, make sure nodata vals are 0
    grouping_rxr = grouping_rxr \
        .isel(band=0) \
        .sel(time=slice(np.datetime64('1984-01-01'), cutoff_date)) \
        .sortby("time").isel(time=-1) \
        .fillna(0).rio.set_nodata(0)
    
    # Reproject, align all to same grid
    _ , sev_rxr, cuml_dist_rxr, past_dist_rxr, future_dist_rxr, agdev_mask_rxr, grouping_rxr = reproj_align_rasters('reproj_match', target_raster, sev_rxr, cuml_dist_rxr, past_dist_rxr, future_dist_rxr, agdev_mask_rxr, grouping_rxr)
    
    del target_raster, _
    gc.collect()
    
    print(f'Severity:\n{sev_rxr}')
    print(f'Agdev mask:\n{agdev_mask_rxr}')
    print(f'Any dist:\n{cuml_dist_rxr}')
    print(f'Future dist:\n{future_dist_rxr}')
    print(f'Past dist:\n{past_dist_rxr}')
    print(f'Groupings:\n{grouping_rxr}')
    
    return sev_rxr, cuml_dist_rxr, past_dist_rxr, future_dist_rxr, agdev_mask_rxr, grouping_rxr


def create_ndvi_timeseries_rxr(
        config: dict,
        fire_metadata: dict,
        file_paths: dict
    ) -> xr.DataArray:
    ''' 
    Combines all NDVI seasonal tifs along the time dimension of an output xr.DataArray
    Output rxr DataArray with format:
        Dims: 
            time, x, y
        Coords: 
            t=time, x=lon, y=lat
        Variable: 
            NDVI
    '''
    # List to hold all NDVI arrrays and associated dates
    ndvi_search_arg = config['RECOVERY_PARAMS']['NDVI_SEARCH_ARG']
    ndvi_dir = file_paths['INPUT_LANDSAT_SEASONAL_DIR']
    invalid_lower_val, invalid_upper_val = float(config['RECOVERY_PARAMS']['NDVI_LOWER_BOUND']), float(config['RECOVERY_PARAMS']['NDVI_UPPER_BOUND'])
    seasonal_ndvi_paths = glob.glob(os.path.join(ndvi_dir, ndvi_search_arg))

    seasonal_ndvi = []  # set up lists to hold NDVI rxr data + associated dates
    ndvi_dates = []
    template_rxr = None

    # Iterate over season of LS NDVI data
    print(f'Will iterate over: {seasonal_ndvi_paths}')
    for f in seasonal_ndvi_paths:
        print(f)
        # Get the yr, month from the file name
        f_name = f.split('/')[-1].split('_')[0]
        yr, month = int(f_name[:4]), int(config['RECOVERY_PARAMS']['MONTH_SEASON_DICT'][str(int(f_name[4:]))])
        curr_date = f"{yr}{month:02d}"
        ndvi_dates.append(curr_date)
        print(f.split('/')[-1], month, yr, curr_date)

        # Open the NDVI_rxr file and mask invalid values
        ndvi_rxr = rxr.open_rasterio(f)
        ndvi_rxr.data = np.where(ndvi_rxr.data<=invalid_lower_val, np.nan, ndvi_rxr.data)
        ndvi_rxr.data = np.where(ndvi_rxr.data>invalid_upper_val, np.nan, ndvi_rxr.data)

        # Align all the NDVI rxr to the same grid
        if template_rxr is not None:
            _, ndvi_rxr = reproj_align_rasters('reproj_match', template_rxr, ndvi_rxr)
        else: template_rxr = ndvi_rxr

        # Add ndvi_rxr to the list of data to concatenate
        ndvi_rxr = ndvi_rxr.squeeze(dim='band')
        seasonal_ndvi += [ndvi_rxr.astype('float32')]

    combined_ndvi_da = xr.concat(seasonal_ndvi, dim='time').rename('NDVI')
    combined_ndvi_da['time'] = pd.to_datetime(ndvi_dates, format='%Y%m').astype('datetime64[ns]')
    del seasonal_ndvi
    gc.collect()
    
    # Sort by time, organize the dimensions
    combined_ndvi_da = combined_ndvi_da.sortby('time')
    combined_ndvi_da = combined_ndvi_da.transpose('time', 'y', 'x')

    return combined_ndvi_da


def create_ndvi_match_layer(
    fire_datacube: xr.DataArray,
    config: dict,
    fire_metadata: dict,
    file_paths: dict
) -> xr.DataArray:
    '''Update the groupings rxr data to have the ndvi group appended to the group ID. 
    
    Returns the same format as the original fire_datacube, but with the data updated to include ndvi matching.
    '''
    # input params
    _, out_dtype, nodata = file_paths['OUT_TIFS_D']['groups']
    groupings_df = pd.read_csv(file_paths['BASELAYERS']['groupings_summary_csv'])
    invalid_lower_val, invalid_upper_val = float(config['RECOVERY_PARAMS']['NDVI_LOWER_BOUND']), float(config['RECOVERY_PARAMS']['NDVI_UPPER_BOUND'])
    yrs_prefire_matched, num_ndvi_groups = config['RECOVERY_PARAMS']['YRS_PREFIRE_MATCHED'], config['RECOVERY_PARAMS']['NUM_NDVI_GROUPS']
    pre_fire_end_date = np.datetime64(fire_metadata['FIRE_DATE'])
    pre_fire_start_date = pre_fire_end_date - pd.Timedelta(weeks=52*yrs_prefire_matched)

    # get list of unique groupings
    groupings_rxr = fire_datacube['groups']
    base_groupings = np.unique(groupings_rxr.data)

    # Create rxr.dataarray of median pre-fire NDVI value
    ndvi_vals_prefire = fire_datacube.sel(
        time=slice(pre_fire_start_date, pre_fire_end_date)
    )

    ndvi_vals_prefire.data[:] = np.where(
        (ndvi_vals_prefire.data<invalid_lower_val) | (ndvi_vals_prefire.data>invalid_upper_val), 
        np.nan, 
        ndvi_vals_prefire.data
    )
    med_ndvi_prefire = ndvi_vals_prefire.median(dim=['time'], skipna=True)
    
    # Loop through unique groups, creating NDVI groupings for each
    updated_groups = np.full(groupings_rxr.data.shape, nodata, dtype=out_dtype)   # initialize new groupings with nodata
    for base_group in base_groupings:
        print(f'base group: {base_group}')
        # Create 1D array of all non-nan median pre-fire NDVI values for just this base group
        group_med_ndvi_arr = med_ndvi_prefire.values[(groupings_rxr.values == base_group) & ~np.isnan(med_ndvi_prefire.values)]

        if len(group_med_ndvi_arr)>0: # if no prefire ndvi available for this group, skip
            # Bin median NDVI values into num_ndvi_groups bins
            quantiles = np.linspace(0, 1, num_ndvi_groups+1)
            bin_edges = np.nanquantile(group_med_ndvi_arr, quantiles)
            print(bin_edges)
            bin_edges[0] = invalid_lower_val
            bin_edges[-1] = invalid_upper_val # Ensure the upper bound includes the maximum possible value, and lower bound includes minimum possible value
            print(f'bin edges: {bin_edges}')
            
            # Optionally create plot of NDVI values
            if config['RECOVERY_PARAMS']['MAKE_PLOTS'] and base_group!=0: 
                plots_dir = os.path.join(file_paths['PLOTS_DIR'], 'quantile_plots/')
                veg_name = groupings_df.loc[groupings_df['id']==base_group, 'NLCD_NAME'].values[0].replace('/', '_').replace(' ', '_')
                elevation = groupings_df.loc[groupings_df['id']==base_group, 'ELEV_LOWER_BOUND'].values[0]
                print(veg_name)
                print(elevation)
                create_density_plot(group_med_ndvi_arr, bin_edges, quantiles, plots_dir, f'{veg_name}_{elevation}_prefire_median_ndvi_density_plot.png')
            
            # Classify non-nan values into quantile groups (index is upper bound of NDVI in group, rounded to nearest 0.01)
            for i in range(num_ndvi_groups):
                if i == 0:
                    # For the first group, include the lower bound
                    mask = ((groupings_rxr.data == base_group) & ~np.isnan(med_ndvi_prefire.data)) & (med_ndvi_prefire.data >= bin_edges[i]) & (med_ndvi_prefire.data <= bin_edges[i+1])
                else:
                    # For subsequent groups, exclude the lower bound
                    mask = ((groupings_rxr.data == base_group) & ~np.isnan(med_ndvi_prefire.data)) & (med_ndvi_prefire.data > bin_edges[i]) & (med_ndvi_prefire.data <= bin_edges[i+1])
                
                group_id = base_group*10**3 + int(bin_edges[i+1]*10**2) # upper bound of NDVI for grouping, appended as last 3 digits to groupings data
                print(f'base_group: {base_group}, base_group*10**3: {base_group*10**3}, int(bin_edges[i+1]*10**2): {int(bin_edges[i+1]*10**2)}, group_id: {group_id}')
                updated_groups = np.where(mask==True, group_id, updated_groups)

    # Update groups data
    fire_datacube['groups'].data[:] = updated_groups
    fire_datacube['groups'].rio.set_nodata(nodata, inplace=True)

    del groupings_rxr, group_med_ndvi_arr
    gc.collect()
        
    return fire_datacube



def create_fire_datacube(
        config: dict,
        fire_metadata: dict,
        file_paths: dict
    ) -> xr.DataArray:
    '''

    '''
    # Create NDVI rxr data array
    fire_datacube = create_ndvi_timeseries_rxr(
        config,
        fire_metadata,
        file_paths
    )

    target_raster = fire_datacube.isel(time=0) # get first date of data as template for CRS, bbox

    # Clip, align all baselayers to just our fire ROI
    sev_rxr, cuml_dist_rxr, past_dist_rxr, future_dist_rxr, agdev_mask, grouping_rxr = open_align_fire_rasters(
        target_raster,
        config,
        fire_metadata,
        file_paths
    )

    # Add coordinates to fire_datacube
    sev_rxr.data = np.where((sev_rxr.data>4) | (sev_rxr.data<2) | (np.isnan(sev_rxr.data)), 0, sev_rxr.data)

    for rxr, layer_name in [(grouping_rxr, 'groups'), (cuml_dist_rxr, 'dist_mask'), (future_dist_rxr, 'future_dist_agdev_mask'), (past_dist_rxr, 'past_dist_agdev_mask'), (sev_rxr, 'severity')]:
        fire_datacube[layer_name] = (
            rxr.dims, 
            rxr.data.squeeze().astype(file_paths['OUT_TIFS_D'][layer_name][1])
        )

    # Add attributes
    fire_datacube.attrs['ndvi_mask_lower'] = config['RECOVERY_PARAMS']['NDVI_LOWER_BOUND']
    fire_datacube.attrs['ndvi_mask_upper'] = config['RECOVERY_PARAMS']['NDVI_UPPER_BOUND']
    fire_datacube.attrs['fire_name'] = fire_metadata['FIRE_NAME']
    fire_datacube.attrs['fire_date'] = fire_metadata['FIRE_DATE']
    fire_datacube.attrs['fire_date_format'] =  '%Y-%m-%d'
    
    # Add spatial information
    try:
        crs_info = target_raster['crs'].attrs
        target_crs = pyproj.CRS.from_cf(crs_info)
    except:
        crs_info = target_raster.spatial_ref.crs_wkt
        target_crs = crs_info
    fire_datacube.rio.write_crs(target_crs, inplace=True)
    
    fire_datacube = fire_datacube.transpose('time', 'y', 'x')

    # Update groupings layer to have NDVI groupings
    print(fire_datacube)
    fire_datacube = create_ndvi_match_layer(
        fire_datacube,
        config,
        fire_metadata,
        file_paths
    )

    return fire_datacube

