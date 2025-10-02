from merged_band_info import band_info, band_names, encoding
from merge_allfire_recovery import *
import time

def aggregate_recovery_summaries(
    recovery_dir: str,
    out_summary_path: str,
    full_ca_template: str,
    merged_recovery_path: str,
    uid_start: int,
    uid_end: int
    ) -> None:
    '''Finds all the clipped recovery rasters in recovery_dir, merges into int8 CA-wide layers with bands described in band_info.
    
    In out_summary_path creates a pd df with UID: fire_incidID and creates tif with recovery time and UID layers
    '''
    # Order fires by date --> assign UID to each fire
    uid_fireID_list = get_ordered_fireUIDs(recovery_dir, out_summary_path)
    num_recovery_tifs = len(uid_fireID_list)
    
    # Create template with no data to update with recovery times and UIDs
    # dtype = int8 for all  layers
    out_raster = create_template_raster(full_ca_template)
    
    # For each fire (in chronological order, oldest to newest), update full CA-wide template with recovery time, UID, severity, 1-5yr precip, 1-5yr VPD
    for uid, fire_dir in uid_fireID_list[uid_start: uid_end]:
        out_raster = add_fire_out_raster(uid, recovery_dir, fire_dir, group_type, num_recovery_tifs, out_raster)
        
    # Add attributes for each variable
    for band_name in band_names:
        out_raster.attrs[f'{band_name}_description'] = band_info[band_name]['description']
        out_raster.attrs[f'{band_name}_units'] = band_info[band_name]['units']

    # Add global attributes
    num_bands = len(band_names)
    out_raster.attrs.update({
        'title': 'Merged Fire Recovery',
        'variable_count': str(num_bands),
        'creation_date': str(pd.Timestamp.now()),
        'nodata_value': str(nodata_value)
    })
        
    # Save output raster
    out_merged_path = f'{merged_recovery_path.strip('.nc')}_merged_{uid_start}_{uid_end}.nc'
    out_raster.to_netcdf(out_merged_path, format='NETCDF4', engine='netcdf4')
    print(f'Successfully exported {out_merged_path} as:\n{out_raster}', flush=True)
    
    # Save per-variable output tif
    for band_name in band_names:
        out_merged_tif = out_merged_path.replace('_merged_', f'_merged_{band_name}_').replace('.nc', '.tif')
        out_raster[band_name].rio.write_nodata(-128).rio.to_raster(out_merged_tif, dtype=np.int8, PIXELTYPE='SIGNEDBYTE')
        print(f'Successfully exported {os.path.basename(out_merged_tif)}', flush=True)
    
    return None


def merge_all_recovery_rasters(out_summary_path):
    '''Once all subsets of fires have been merged by aggregate_recovery_summaries, merge all the aggregate recovery maps into 1 final map.
    '''
    all_rasters = glob.glob(f'{out_summary_path}/merged_recovery_time_*.tif')

    while len(all_rasters)<total_aggregate_maps:
        # wait until all jobs finish
        print(f'Only have {len(all_rasters)}, but expecting {total_aggregate_maps}.\nCurrent list is: {all_rasters}.\nWaiting 5 minutes and checking again.', flush=True)
        time.sleep(60*5) # wait 5 minutes and check again
        all_rasters = glob.glob(f'{out_summary_path}/merged_recovery_time_*.tif')

    sorted_rasters = sorted(all_rasters, key=extract_first_number)

    # EXPORT SINGLE BAND TIFS OF EACH BAND AFTER MERGING
    for band in band_names:
        f_to_merge = [f.replace('recovery_time', band).replace('.nc', '.tif') for f in sorted_rasters]
        rasters_to_merge = [rxr.open_rasterio(f.replace('recovery_time', band).replace('.nc', '.tif')) for f in sorted_rasters]
        print(f'About to merge: {f_to_merge}')
        
        merged_raster = merge_arrays(rasters_to_merge, nodata=-128, method='last')
        merged_raster.rio.to_raster(f'{out_summary_path}/merged_{band}.tif', PIXELTYPE='SIGNEDBYTE', **encoding)
        print(f'Successfully exported merged_{band}.tif', flush=True)
        
        del rasters_to_merge, merged_raster
        gc.collect()
        
    cmd = [
        'gdal_merge.py',
        '-o', f'{out_summary_path}/merged_recovery_full.nc',
        '-n', '-128',
        '-a_nodata', '-128',
        '-ot', 'Int8',
        '-co', 'FORMAT=NC4',
        '-co', 'COMPRESS=DEFLATE'
    ]
    
    # Add all input files
    cmd.extend(sorted_rasters)
    
    print(f'Running command: {' '.join(cmd)}')
    
    try:
        result = subprocess.run(
            cmd,
            check=True,  # Raises exception if command fails
            capture_output=True,
            text=True
        )
        
        print(f'Merged all recovery rasters: {sorted_rasters}')
        if result.stdout:
            print('STDOUT:', result.stdout)
        
        return True
        
    except subprocess.CalledProcessError as e:
        print('Error running gdal_merge.py: {e}')
        print(f'Return code: {e.returncode}')
        if e.stdout:
            print(f'STDOUT: {e.stdout}')
        if e.stderr:
            print(f'STDERR: {e.stderr}')
        return False


if __name__ == '__main__':
    config_path = sys.argv[1]
    uid_start = int(sys.argv[2])
    uid_end = uint(sys.argv[3])
    merge_all = str(sys.argv[4])=='True'
    total_aggregate_maps = sys.argv[5]
    print(f'Merge all: {merge_all}', flush=True)

    aggregate_recovery_summaries(
        recovery_dir='/u/project/eordway/shared/surp_cd/timeseries_data/data/fullCArecovery_reconfig_temporalmasking',
        out_summary_path='/u/project/eordway/shared/surp_cd/timeseries_data/data/fullCArecovery_reconfig_temporalmasking',
        full_ca_template='/u/project/eordway/shared/surp_cd/timeseries_data/data/CA_wide_readonly/cawide_testing/asp_slope_anydist_fullCA_agrdevmask.nc',
        uid_start=uid_start,
        uid_end=uid_end)

    if merge_all:
        merge_all_recovery_rasters(out_summary_path='')