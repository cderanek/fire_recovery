'''
This script downloads, clips, and summarizes the GRIDMET climate data.
'''

import subprocess, glob
import xarray as xr
import rioxarray as rxr
import numpy as np
import geopandas as gpd
import pandas as pd
import pyproj 
import os, gc

def gridmet_download_clip(WGET_FILE, DATA_DIR, ROI_PATH):
    ROI = gpd.read_file(ROI_PATH)

    # Change to data download directory
    os.chdir(DATA_DIR)
    
    # Get wget commands -> organize by product
    wget_commands = open(WGET_FILE, 'r').readlines()[1:]
    all_products = np.unique([c.split('/')[-1].split('_')[0] for c in wget_commands if 'pdsi' not in c])
    wget_commands_byprod = {
        prod: [c for c in wget_commands if prod in c]
        for prod in all_products
    }
    
    # For each product, output a .nc file with the monthly values for all years, clipped to our ROI
    for prod in all_products:
        print(f'Starting download, clipping for {prod}.', flush=True)
        
        # Get command and output file name
        wget_commands = wget_commands_byprod[prod]
        f_new = wget_commands[0].split('/')[-1].split('_')[0] + '_clipped'
        all_yrs_rasters = {
            'sum': [],
            'mean': []
        }
        
        # Download, clip each year for this product
        for wget_line in wget_commands:
            wget_line = wget_line.strip(' \n')
            f = wget_line.split('/')[-1]
            
            print(f'\tDownloading: {f}', flush=True)
            subprocess.run(wget_line, shell=True)
                
            # Open the NetCDF file
            dataset = xr.open_dataset(f, decode_coords="all")

            # Get riodataset for all vars and write crs
            crs_info = dataset.variables['crs'].attrs
            orig_crs = pyproj.CRS.from_cf(crs_info)
            rds = dataset[dataset.rio.vars]
            rds.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
            rds.rio.write_crs(orig_crs, inplace=True)
            
            # Clip to ROI
            print(f'\tClipping: {f}', flush=True)
            ROI_reproj = ROI.to_crs(orig_crs)
            minx, miny, maxx, maxy = tuple(*list(ROI_reproj.bounds.to_records(index=False)))
            rds_clipped = rds.rio.clip_box(minx=minx, miny=miny, maxx=maxx, maxy=maxy, crs=orig_crs)

            # Aggregate monthly
            print(f'\tAggregating to monthly.', flush=True)
            data_var = list(rds_clipped.data_vars)[0]
            
            
            all_yrs_rasters['sum'].append(rds_clipped[data_var].resample(day='ME', skipna=True).sum().rename({'day':'time'}))
            all_yrs_rasters['mean'].append(rds_clipped[data_var].resample(day='ME', skipna=True).mean().rename({'day':'time'}))

            # Delete full extent, daily .nc file
            print(f'\tDeleting {f}', flush=True)
            os.remove(f)
            
            # Memory management
            del rds, rds_clipped, ROI_reproj
            gc.collect()
        
        # Concatenate, save all months/years of data for this product
        print(f'\n\nConcatenating all years rasters for {prod}', flush=True)
        out_sum = xr.concat(all_yrs_rasters['sum'], dim='time').transpose('time', 'lat', 'lon')
        out_sum.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
        out_sum.rio.write_crs(orig_crs, inplace=True)
        out_sum.name = data_var+'_sum'
        
        out_mean = xr.concat(all_yrs_rasters['mean'], dim='time').transpose('time', 'lat', 'lon')
        out_mean.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
        out_mean.rio.write_crs(orig_crs, inplace=True)
        out_mean.name = data_var+'_mean'
        
        print(f'Saving merged, clipped .nc for {prod}', flush=True)
        out_sum.to_netcdf(f_new+'_sum.nc')
        out_mean.to_netcdf(f_new+'_mean.nc')
        print(f'Saved {f_new}.', flush=True)
        
        # Memory management
        del all_yrs_rasters, out_sum, out_mean
        gc.collect()
    
    pass

def calculate_anomaly(nc_path, reference_yrs_range=None, out_path=None):
    '''Calculates monthly anomaly. 
    If reference_yrs_range is None, uses all years.
    If out_path is None, saves to nc_path.replace('.nc', '_anomaly.nc')
    '''
    print(f'Opening {nc_path}', flush=True)
    monthly_da = xr.open_dataset(nc_path, decode_coords="all")
    data_var = list(monthly_da.data_vars)[0]
    
    print(f'Calculating monthly anomalies', flush=True)
    # Assign year, month values for slicing and aggregating
    temp_da = monthly_da.assign_coords(month=monthly_da.time.dt.month)
    temp_da = temp_da.assign_coords(year=temp_da.time.dt.year)
    
    # Filter to reference range years, if applicable
    if reference_yrs_range: temp_da.sel(year=slice(*reference_yrs_range))
    
    # Calculate monthly mean, std
    monthly_mean = temp_da.copy().groupby('month').mean(dim='time', skipna=True)
    monthly_std = temp_da.groupby('month').std(dim='time', skipna=True)
    monthly_std = xr.where(monthly_std == 0, np.nan, monthly_std) # avoiding divide by 0 errors
    
    del temp_da
    gc.collect()
    
    # Build a new anomaly xarray, using the shape, properties of the original xarray
    fill_val = np.iinfo(np.int16).min
    monthly_anomalies_da = monthly_da.copy()
    monthly_anomalies_da[data_var].data = np.full_like(monthly_da[data_var].data, fill_value=fill_val)
    # print(monthly_anomalies_da)
    
    # Update each date with the anomaly for that date
    for date in monthly_da.time.values:
        # Select the relevant date of original monthly data, and the corresponding anomaly month
        curr_month_orig = monthly_da.sel(time=date)
        month_int = pd.to_datetime(date).month
        curr_month_anom = (curr_month_orig - monthly_mean.sel(month=month_int)) / monthly_std.sel(month=month_int)
        # Update the relevant date in monthly_anomalies_da
        monthly_anomalies_da.sel(time=date)[data_var].data[:] = curr_month_anom[data_var].data    
    
    # Apply scaling, set nan value, store as int16 for lower memory usage
    print(f'Applying scaling factor', flush=True)
    scaling_factor = 10**3
    monthly_anomalies_da[data_var].data = np.round(np.nan_to_num(monthly_anomalies_da[data_var].data * scaling_factor, nan=fill_val)).astype('int')
    print(np.nanmin(monthly_anomalies_da[data_var].data), np.nanmax(monthly_anomalies_da[data_var].data))
    if np.nanmin(monthly_anomalies_da[data_var].data) < np.iinfo(np.int16).min:
        print(f'ERROR WITH {data_var}: actual min {np.nanmin(monthly_anomalies_da[data_var].data)} < {np.iinfo(np.int16).min}')
    if np.nanmax(monthly_anomalies_da[data_var].data) > np.iinfo(np.int16).max:
        print(f'ERROR WITH {data_var}: actual max {np.nanmax(monthly_anomalies_da[data_var].data)} > {np.iinfo(np.int16).max}')
    
    monthly_anomalies_da = (
        monthly_anomalies_da
        .rename({data_var: data_var+'_anomaly'})
        .fillna(fill_val)
        .astype(np.int16)
    )
    
    # Save monthly anomalies
    if not out_path: out_path=nc_path.replace('.nc', '_anomaly.nc')
    crs_info = monthly_anomalies_da.variables['crs'].attrs
    orig_crs = pyproj.CRS.from_cf(crs_info)
    print(orig_crs)
    monthly_anomalies_da.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
    monthly_anomalies_da.rio.write_crs(orig_crs, inplace=True)
    monthly_anomalies_da.to_netcdf(out_path)
    print(monthly_anomalies_da)
    
    monthly_mean = monthly_mean.rename({data_var: data_var+'_monthly_mean'})
    monthly_mean.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
    monthly_mean.rio.write_crs(orig_crs, inplace=True)
    monthly_mean.to_netcdf(out_path.replace('_anomaly.nc', '_monthly_means.nc'))
    
    monthly_std = monthly_std.rename({data_var: data_var+'_monthly_std'})
    monthly_std.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
    monthly_std.rio.write_crs(orig_crs, inplace=True)
    monthly_std.to_netcdf(out_path.replace('_anomaly.nc', '_monthly_std.nc'))
    print(f'Saved {out_path}', flush=True)
    
    pass

def calculate_water_yr_avgs(nc_path, annual_agg_type='sum'):
    print(f'Opening {nc_path}', flush=True)
    monthly_da = xr.open_dataset(nc_path, decode_coords="all")
    data_var = list(monthly_da.data_vars)[0]

    crs_info = monthly_da.variables['crs'].attrs
    orig_crs = pyproj.CRS.from_cf(crs_info)
    print(orig_crs)
    monthly_da[data_var].rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)
    monthly_da[data_var].rio.write_crs(orig_crs, inplace=True)
        
    print(f'Calculating water year averages', flush=True)
    # Assign year, month values for slicing and aggregating
    monthly_da = monthly_da.assign_coords(month=monthly_da.time.dt.month)
    monthly_da = monthly_da.assign_coords(year=monthly_da.time.dt.year)
    monthly_da = monthly_da.assign_coords(water_yr=monthly_da.year - (((12 - monthly_da.month) // 2) > 0).astype(int))

    if annual_agg_type == 'sum':
        # calculate water year total sum
        annual_da = monthly_da.groupby('water_yr').sum()

    elif annual_agg_type == 'average':
        # calculate water year average monthly val
        annual_da = monthly_da.groupby('water_yr').mean()

    # calculate avg, std over all water years
    all_time_da_mean = annual_da.mean(dim='water_yr', skipna=True)
    all_time_da_std = annual_da.std(dim='water_yr', skipna=True)
    # all_time_da_std = xr.where(all_time_da_std == 0, np.nan, all_time_da_std) # avoiding divide by 0 errors

    # calculate water yr anomaly
    annual_da_anom = (annual_da - all_time_da_mean) / all_time_da_std

    print(f'Saving out_nc file', flush=True)
    out_path = nc_path.replace('.nc', f'_wateryr_{annual_agg_type}.nc')
    annual_da = annual_da.rename({data_var: data_var+'_wateryr_'+annual_agg_type})
    annual_da.rio.write_crs(orig_crs, inplace=True).to_netcdf(out_path)
    print(f'Saved {out_path}', flush=True)

    out_path = nc_path.replace('.nc', f'_alltimeavg_wateryr_{annual_agg_type}.nc')
    all_time_da_mean = all_time_da_mean.rename({data_var: data_var+'_alltimeavg_wateryr_'+annual_agg_type})
    all_time_da_mean.rio.write_crs(orig_crs, inplace=True).to_netcdf(out_path)
    print(f'Saved {out_path}', flush=True)
    
    out_path = nc_path.replace('.nc', f'_wateryr_{annual_agg_type}_anom.nc')
    annual_da_anom = annual_da_anom.rename({data_var: data_var+'_wateryr_'+annual_agg_type+'_anom'})
    annual_da_anom.rio.write_crs(orig_crs, inplace=True).to_netcdf(out_path)
    print(f'Saved {out_path}', flush=True)
    