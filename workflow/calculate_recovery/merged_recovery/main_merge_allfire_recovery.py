import rioxarray as rxr
import xarray as xr
import rasterio as rio
import pandas as pd
import numpy as np
import subprocess, glob
from merged_band_info import band_info, band_names, encoding
from merge_allfire_recovery import *
import time

def aggregate_recovery_summaries(
    perfire_config: dict,
    processing_progress_csv_path: str,
    template_baselayer: str,
    merged_recovery_path: str,
    uid_start: int,
    uid_end: int
    ) -> None:
    '''Finds all the clipped recovery rasters in recovery_dir, merges into int8 CA-wide layers with bands described in band_info.
    
    In out_summary_path, creates a pd df with UID: fire_incidID and creates tif with recovery time and UID layers
    '''
    # Order fires by date --> assign UID to each fire
    uid_fireID_list = get_ordered_fireUIDs(processing_progress_csv_path)
    num_recovery_tifs = len(uid_fireID_list)
    
    # Create template with no data to update with recovery times and UIDs
    # dtype = int8 for all  layers
    out_raster = create_template_raster(template_baselayer, band_names)
    
    # For each fire (in chronological order, oldest to newest), update full CA-wide template with recovery time, UID, severity
    for uid, fireid in uid_fireID_list[uid_start: uid_end]:
        recovery_matched_path = perfire_config[fireid]['FILE_PATHS']['OUT_TIFS_D']['fire_recovery_time'][0]
        recovery_baseline_path = perfire_config[fireid]['FILE_PATHS']['OUT_TIFS_D']['prefire_baseline_recovery_time'][0]
        out_raster = add_fire_out_raster(
            perfire_config,
            uid, 
            fireid, 
            num_recovery_tifs, 
            out_raster)
        
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


def merge_all_recovery_rasters(merged_recovery_path, total_aggregate_maps):
    '''Once all subsets of fires have been merged by aggregate_recovery_summaries, merge all the aggregate recovery maps into 1 final map.
    '''
    all_rasters = glob.glob(os.path.join(os.dirname(merged_recovery_path), 'merged_recovery_time_*.tif'))

    while len(all_rasters)<total_aggregate_maps:
        # wait until all jobs finish
        print(f'Only have {len(all_rasters)}, but expecting {total_aggregate_maps}.\nCurrent list is: {all_rasters}.\nWaiting 5 minutes and checking again.', flush=True)
        time.sleep(60*5) # wait 5 minutes and check again
        all_rasters = glob.glob(os.path.join(os.dirname(merged_recovery_path), 'merged_recovery_time_*.tif'))

    sorted_rasters = sorted(all_rasters, key=extract_first_number)

    # EXPORT SINGLE BAND TIFS OF EACH BAND AFTER MERGING
    for band in band_names:
        f_to_merge = [f.replace('recovery_time', band).replace('.nc', '.tif') for f in sorted_rasters]
        rasters_to_merge = [rxr.open_rasterio(f.replace('recovery_time', band).replace('.nc', '.tif')) for f in sorted_rasters]
        print(f'About to merge: {f_to_merge}')
        
        merged_raster = merge_arrays(rasters_to_merge, nodata=-128, method='last')
        out_path = os.path.join(os.dirname(merged_recovery_path), f'merged_{band}.tif')
        merged_raster.rio.to_raster(out_path, PIXELTYPE='SIGNEDBYTE', **encoding)
        print(f'Successfully exported merged_{band}.tif', flush=True)
        
        del rasters_to_merge, merged_raster
        gc.collect()
        
    cmd = [
        'gdal_merge.py',
        '-o', merged_recovery_path,
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
    print(datetime.now())
    print(f'Running main_merge_allfire_recovery.py with arguments {'\n'.join(sys.argv)}\n', flush=True)
    config_path = sys.argv[1]
    perfire_config_path = sys.argv[2]
    uid_start = int(sys.argv[3])
    uid_end = int(sys.argv[4])
    merge_all = str(sys.argv[5])=='True'
    done_flag = sys.argv[6]

    # read in jsons
    with open(config_path, 'r') as f:
        config = json.load(f)
    with open(perfire_config_path, 'r') as f:
        perfire_config = json.load(f)

    # get args from config
    recovery_dir = config['RECOVERY_PARAMS']['RECOVERY_MAPS_DIR']
    merged_recovery_path = os.path.join(recovery_dir, 'merged_recovery_full.nc')
    processing_progress_csv_path = config['RECOVERY_PARAMS']['LOGGING_PROCESS_CSV']
    template_baselayer = config['BASELAYERS']['topo']['fname']

    # aggregate from uid_start to uid_end
    aggregate_recovery_summaries(
        perfire_config,
        processing_progress_csv_path,
        template_baselayer,
        merged_recovery_path,
        uid_start,
        uid_end)

    # merge all fires (if this is the last job)
    if merge_all:
        total_aggregate_maps = uid_end // 100 # 1 merged file per 100 fires
        merge_all_recovery_rasters(merged_recovery_path, total_aggregate_maps)

    subprocess.run(['touch', done_flag])