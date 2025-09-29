import sys, filelock, glob, json, os, re
import copy
import pandas as pd
import numpy as np
 
from data_merger import create_fire_datacube
from qa_checks import temporal_coverage_check
from recovery_calculator import calculate_ndvi_thresholds, calculate_recovery_time, single_fire_recoverytime_summary

sys.path.append("workflow/utils")
from geo_utils import clip_raster_to_poly, export_to_tiff

sys.path.append("workflow/calculate_recovery/make_plots")
from recovery_plots import plot_time_series, plot_random_sampled_pt


if __name__ == '__main__':
    print(f'Running main_fire_recovery.py with arguments {'\n'.join(sys.argv)}\n')
    main_config_path=sys.argv[1]
    perfire_config_path=sys.argv[2]
    fireid=sys.argv[3]

    # read in jsons
    with open(main_config_path, 'r') as f:
        config = json.load(f)
    with open(perfire_config_path, 'r') as f:
        perfire_json = json.load(f)
    fire_metadata = perfire_json[fireid]['FIRE_METADATA']
    file_paths = perfire_json[fireid]['FILE_PATHS']

    #### CHECK LOG FILE ####
    # open csv
    csv = pd.read_csv(config['RECOVERY_PARAMS']['LOGGING_PROCESS_CSV'])
    # update row associated with just completed downloads
    mask = csv['fireid'] == fireid
    curr_row_index = csv[mask].index[0]
    
    print(f'Download status: {csv.loc[curr_row_index, 'download_status']}\nRecovery status: {csv.loc[curr_row_index, 'recovery_complete']}')
    if csv.loc[curr_row_index, 'download_status'].lower() != 'complete': 
        print(f'All years data not successfully downloaded. Quitting program.') 
        sys.exit(1)
        
    if csv.loc[curr_row_index, 'recovery_complete'] == True: 
        print(f'Recovery already calculated. Quitting program.') 
        sys.exit(1)
    

    #### GENERATE ALL PARAMS FOR SENSITIVITY ANALYSIS ####
    all_param_combos = [] # create a list of dictionaries with all param values combos + the assocated file suffix

    if fire_metadata['SENSITIVITY_ANALYSIS']:  # update all param combos to include all param combos, if this fire is being used for sensitivity tests
        for curr_param in config['SENSITIVITY']['PARAMS'].keys():
            for curr_value in config['SENSITIVITY']['PARAMS'][curr_param]['Range']:
                if curr_value != config['SENSITIVITY']['PARAMS'][curr_param]['Default']:
                    param_d = {param: config['SENSITIVITY']['PARAMS'][param]['Default'] for param in config['SENSITIVITY']['PARAMS'].keys()}
                    param_d[curr_param] = curr_value
                    suffix = f'{curr_param}_{curr_value}'
                    param_d['suffix'] = suffix
                    all_param_combos.append(param_d)
        # add default configuration
        param_d = {param: config['SENSITIVITY']['PARAMS'][param]['Default'] for param in config['SENSITIVITY']['PARAMS'].keys()}
        param_d['suffix'] = '' # default values, suffix is empty string
        all_param_combos.append(param_d)
    
    else: # just use current configuration if not in sensitivity analysis fire
        param_d = {param: config['RECOVERY_PARAMS'][param] for param in config['SENSITIVITY']['PARAMS'].keys()}
        param_d['suffix'] = '' # default values, suffix is empty string
        all_param_combos.append(param_d)
    
    #### CALCULATE RECOVERY FOR ALL PARAMS IN SENSITIVITY ANALYSIS ####  
    # Update config and file_paths dicts to match the current set of params
    orig_file_paths = copy.deepcopy(file_paths)

    for param_d in all_param_combos:
        print(f'Currently calculating recovery for the following params:\n{param_d}')
        
        # Update config to match the current set of params
        for param_name, param_val in param_d.items():
            if param_name != 'suffix': config['RECOVERY_PARAMS'][param_name] = param_val
        
        # Update file_paths config to match the current set of params
        suffix = param_d['suffix']

        for out_tifs_name in orig_file_paths['OUT_TIFS_D'].keys():
            old_dir = os.path.dirname(orig_file_paths['OUT_TIFS_D'][out_tifs_name][0])
            old_fname = os.path.basename(orig_file_paths['OUT_TIFS_D'][out_tifs_name][0])
            new_path = os.path.join(old_dir, f'{suffix}', old_fname)
            os.makedirs(os.path.dirname(new_path), exist_ok=True)
            file_paths['OUT_TIFS_D'][out_tifs_name][0] = new_path

        for f in ['OUT_MAPS_DATA_DIR_PATH', 'RECOVERY_COUNTS_SUMMARY_CSV', 'PLOTS_DIR', 'OUT_MERGED_NDVI_NC', 'OUT_SUMMARY_CSV', 'OUT_MERGED_THRESHOLD_NC']:
            old_dir = os.path.dirname(orig_file_paths[f])
            old_fname = os.path.basename(orig_file_paths[f])
            new_path = os.path.join(old_dir, f'{suffix}', old_fname)
            os.makedirs(os.path.dirname(new_path), exist_ok=True)
            file_paths[f] = new_path

        # for key in orig_file_paths.keys():
        #     if key != 'BASELAYERS':
        #         print(key)
        #         print(f'ORIGINAL:\t{orig_file_paths[key]}')
        #         try:
        #             print(f'UPDATE:\t\t{file_paths[key]}')
        #         except:
        #             print()
        #         print()

        # Save params to text file in the new OUT_MAPS_DATA_DIR_PATH
        with open(os.path.join(file_paths['OUT_MAPS_DATA_DIR_PATH'], 'params.txt'), 'w') as f:
            print(param_d, file=f)

        #### LOAD + MERGE DATA ####
        # Load rasters and create merged NDVI dataset + get associated groupings dict
        combined_ndvi = create_fire_datacube(
            config=config,
            fire_metadata=fire_metadata,
            file_paths=file_paths
        )
        if config['RECOVERY_PARAMS']['CREATE_INTERMEDIATE_TIFS']: combined_ndvi.to_netcdf(file_paths['OUT_MERGED_NDVI_NC'], format='NETCDF4') # Optional: output intermediate .nc

        # Process NDVI thresholds and create summary of thresholds over time for each group
        ndvi_thresholds_da, summary_df = calculate_ndvi_thresholds(
            combined_ndvi,
            config        
        )
        summary_df.to_csv(file_paths['OUT_SUMMARY_CSV'])


        #### CALCULATE RECOVERY ####
        # Update ndvi_thresholds dataarray to have layers to flag QA issues
        ndvi_thresholds_da =  temporal_coverage_check(
            ndvi_thresholds_da, 
            config, 
            fire_metadata
        )
        
        # Calculate recovery time using NDVI and thresholds timeseries
        recovery_da = calculate_recovery_time(
            ndvi_thresholds_da, 
            config
        )

        # save printout to summary txt file
        with open(file_paths['OUT_MERGED_THRESHOLD_NC'].replace('.nc', '_summary.txt'), 'w') as f:
            print(f'{recovery_da}\n\n{recovery_da.coords}', file=f)

        # Export outputs to nc, tif
        if config['RECOVERY_PARAMS']['CREATE_INTERMEDIATE_TIFS']: 
            recovery_da.to_netcdf(file_paths['OUT_MERGED_THRESHOLD_NC'])    # export full biocube with the time series, coords, and resulting recovery 
        
        for coord, (fname, dtype, nodata) in file_paths['OUT_TIFS_D'].items():
            # skip outputting the following layers if we're not in the default settings
            # these layers are independent of the params, so don't need to be saved for every param combination
            if suffix!='' and coord in ['severity', 'dist_mask', 'future_dist_agdev_mask', 'past_dist_agdev_mask', 'elevation', 'evt']:
                pass 

            else:
                try:
                    out_data = recovery_da.coords[coord]
                    if 'recovery.tif' in fname or 'resilience' in fname:
                        out_data_clipped = clip_raster_to_poly(out_data, fire_metadata['FIRE_BOUNDARY_PATH'])
                        
                        export_to_tiff(
                            out_data_clipped, 
                            fname.replace('.tif', '_clipped.tif'), 
                            dtype_out=dtype, 
                            nodata=nodata
                        ) 
                    
                    export_to_tiff(
                        out_data, 
                        fname, 
                        dtype_out=dtype, 
                        nodata=nodata
                    )                                   # export just recovery layer to tif
                
                except Exception as e:
                    print(coord, fname, dtype)
                    print(f'Skipping tif output for {coord} due to error: {e}')
        
        # Create a summary of the recovery time across all pixels without future disturbances
        single_fire_recoverytime_summary(
            recovery_da, 
            config,
            fire_metadata,
            file_paths
        )


        #### VISUALIZATIONS ####
        if config['RECOVERY_PARAMS']['MAKE_PLOTS'] or fire_metadata['SENSITIVITY_ANALYSIS']:
            # Plot median time series for each group
            plot_time_series(
                summary_df,
                np.datetime64(fire_metadata['FIRE_DATE']),
                os.path.join(file_paths['PLOTS_DIR'], 'time_series_aggregate/'),
                config['RECOVERY_PARAMS']['MIN_NUM_MATCHED_PIXELS']
            )

            # Plot time series for 30 randomly selected pixels
            for i in range(30):
                try:
                    plot_random_sampled_pt(
                        recovery_da, 
                        summary_df,
                        os.path.join(file_paths['PLOTS_DIR'], 'time_series_random/'))
                except:
                    pass
        

    #### UPDATE LOG FILE ####
    lock_file = config['RECOVERY_PARAMS']['LOGGING_PROCESS_CSV'] + '.lock'
    lock = filelock.FileLock(lock_file, timeout=60)  # Wait up to 60 seconds for lock
    try:
        with lock:
            csv = pd.read_csv(config['RECOVERY_PARAMS']['LOGGING_PROCESS_CSV'])
            
            # update row associated with just completed downloads
            mask = csv['fireid'] == fireid
            curr_row_index = csv[mask].index[0]
            csv.loc[curr_row_index, 'recovery_complete'] = True

        # Save the updated csv
        csv.to_csv(config['RECOVERY_PARAMS']['LOGGING_PROCESS_CSV'], index=False)
        
    except filelock.Timeout:
        print("Could not acquire lock on file after waiting")
    

    #### DELETE ORIGINAL DATA ####
    pattern = r".*/temp/seasonal/.*_\d+/"
    ndvi_dir = file_paths['INPUT_LANDSAT_SEASONAL_DIR']
    if re.match(pattern, ndvi_dir) and config['RECOVERY_PARAMS']['DELETE_NDVI_SEASONAL_TIFS']:
        print('will delete:', f'rm -r {ndvi_dir}*.tif')
        print(os.system(f'ls {ndvi_dir}*.tif'))
        os.system(f'rm -r {ndvi_dir}*.tif')
        
    
    landsat_dir = file_paths['INPUT_LANDSAT_DATA_DIR']
    if os.path.exists(landsat_dir) and config['RECOVERY_PARAMS']['DELETE_NDVI_SEASONAL_TIFS'] and not fire_metadata['SENSITIVITY_ANALYSIS']:
        for folder in glob.glob(f'{landsat_dir}LS_01-01-*'):
            if os.path.isdir(folder):
                print('will delete:', f'rm -r {folder}')
                os.system(f'rm -r {folder}')