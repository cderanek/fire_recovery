import sys, os, glob, json
import pandas as pd
import filelock
from functools import partial
from datetime import datetime, timedelta

from download_log_helpers import *
sys.path.append("workflow/utils/") 
from earthaccess_downloads import *
from merge_process_scenes import mosaic_ndvi_timeseries
from geo_utils import buffer_firepoly


### Helper functions to process individual jobs, organize all years downloads, report results ##
def process_all_years(args: dict):
    # cleanup file names for task submission
    roi_path = args['bufferedfireShpPath']
    cleaned_roi_path = re.sub(r'[^a-zA-Z0-9_-]', '', os.path.basename(roi_path))

    # create log to hold headers, task_ids, download status for all jobs
    download_log = create_download_log(args)
    unsubmitted_years_df = download_log[['start_date', 'end_date']][(download_log['task_submitted_time'].isna()) & (download_log['ndvi_mosaic_complete']==False)]
    
    # submit a task for all years
    for index, (start_date, end_date) in unsubmitted_years_df.iterrows():
        start_date_str=f'{start_date.month}-{start_date.day}-{start_date.year}'
        end_date_str=f'{end_date.month}-{end_date.day}-{end_date.year}'
        task_name = f'LS_{start_date_str}_{end_date_str}_{cleaned_roi_path}'

        # for the task spanning the bad date, need to output 2 tasks to the same data dir to avoid splitting up data from the same year
        if (end_date.month != 12) or (end_date.day != 31): # bad year start of split
            next_end_date = download_log.loc[index+1, 'end_date']
            next_end_date_str=f'{next_end_date.month}-{next_end_date.day}-{next_end_date.year}'
            dest_dir = os.path.join(args['ls_data_dir'], f'LS_{start_date_str}_{next_end_date_str}_{cleaned_roi_path}')
        if (start_date.month != 1) or (start_date.day != 1): # bad year end of split
            dest_dir = download_log.loc[index-1, 'dest_dir']
        else: # regular case
            dest_dir = os.path.join(args['ls_data_dir'], task_name)
        print(f'Dest dir {dest_dir}')

        task_json = create_product_request_json(
            task_name=task_name,
            start_date=start_date_str,
            end_date=end_date_str,
            shp_file_path=roi_path,
            product_layers=args['product_layers'],
            file_type='geotiff'
        )

        # Log in to earth access
        head = login_earthaccess()
        # Submit task
        task_id = post_request(task_json, head, max_retries=10)
        print(f'task id: {task_id}')

        # Update download log with dest_dir, head, task_id, task_submitted_time
        download_log.loc[index, 'dest_dir'] = dest_dir
        download_log.loc[index, 'head'] = head['Authorization']
        download_log.loc[index, 'task_id'] = task_id
        download_log.loc[index, 'task_submitted_time'] = datetime.now()
        download_log.to_csv(args['download_log_csv'], index=False)
        
    print(f'Download log after submitting all tasks: {download_log}', flush=True)

    # keep working on download until all years complete
    unsuccessful_years_w_retries = download_log['start_date'][(download_log['ndvi_mosaic_complete']==False) & (download_log['get_bundle_tries_left']>0) & (download_log['download_bundle_tries_left']>0) & (download_log['mosaic_tries_left']>0)]
    while len(unsuccessful_years_w_retries) > 0:
        print(f'Starting another round of checks: {unsuccessful_years_w_retries}\n{download_log}', flush=True)
        download_log.to_csv(args['download_log_csv'], index=False)
        # for each task with no bundle, ping appeears, and if ready, get bundle
        nodbundle_years_df = download_log[['start_date', 'task_id']][(download_log['ndvi_mosaic_complete']==False) & (download_log['bundle'].isna()) & (download_log['get_bundle_tries_left']>0)]
        for index, (start_date, task_id) in nodbundle_years_df.iterrows():
            head = login_earthaccess()
            print(f'Pinging appears for start date: {start_date}; task_id: {task_id}')
            task_complete = ping_appears_once(task_id, head)
            print(f'ping response: {task_complete}', flush=True)
            time.sleep(SLEEP_TIME) # to enforce sleep time between requests
            if task_complete:
                # try to download bundles 
                print('task complete')
                bundle = try_get_bundle_once(task_id, head)
                print('received bundle')
                time.sleep(SLEEP_TIME) # to enforce sleep time between requests

                # Update download log
                if bundle:
                    download_log.loc[index, 'bundle'] = json.dumps(bundle)
                    download_log.loc[index, 'bundle_received_time'] = datetime.now()
                else:
                    download_log.loc[index, 'get_bundle_tries_left'] = download_log.loc[index, 'get_bundle_tries_left'] - 1

        # for each task with a bundle, but incomplete download, try to download bundle
        incompletedownload_years_df = download_log[['start_date', 'task_id', 'bundle', 'dest_dir']][(download_log['ndvi_mosaic_complete']==False) & (download_log['download_complete']==False) & (download_log['bundle'].notna()) & (download_log['download_bundle_tries_left']>0)]
        for index, (start_date, task_id, bundle, dest_dir) in incompletedownload_years_df.iterrows():
            print(f'Downloading bundle with start date: {start_date}; task_id: {task_id}')
            head = login_earthaccess()
            # try to download bundle
            dest_dir_complete = None
            dest_dir_complete = download_landsat_bundle(bundle, task_id, head, dest_dir)

            # Update download log
            if dest_dir_complete:
                download_log.loc[index, 'download_complete'] = True
            else:
                download_log.loc[index, 'download_bundle_tries_left'] = download_log.loc[index, 'download_bundle_tries_left'] - 1
        
        # for each task with a complete download, but incomplete ndvi_mosaic, try to create mosaic
        incomplete_mosaic_df = download_log[['start_date', 'end_date', 'dest_dir']][(download_log['download_complete']==True) & (download_log['ndvi_mosaic_complete']==False) & (download_log['mosaic_tries_left']>0)]
        for index, (start_date, end_date, dest_dir) in incomplete_mosaic_df.iterrows():
            print(f'Starting seasonal mosaic for: {start_date}; dest_dir: {dest_dir}')
            try:
                mosaic_ndvi_timeseries(
                    dest_dir, args['valid_layers'], args['ls_seasonal_dir'], NODATA=args['default_nodata'], 
                    NDVI_BANDS_DICT=args['ndvi_bands_dict'], RGB_BANDS_DICT=args['rgb_bands_dict'],
                    MAKE_RGB=args['make_daily_rgb'], MAKE_DAILY_NDVI=args['make_daily_ndvi']
                )
                # Update download log
                download_log.loc[index, 'ndvi_mosaic_complete'] = True

            except Exception as e:
                # Update mosaic_tries_left on download log
                print(f'Failed to mosaic from {dest_dir}.')
                print(e)
                download_log.loc[index, 'mosaic_tries_left'] = download_log.loc[index, 'mosaic_tries_left'] - 1
        
        # re-check the list of unsuccessful years with retries left to determine if we should keep looping
        unsuccessful_years_w_retries = download_log['start_date'][(download_log['ndvi_mosaic_complete']==False) & (download_log['get_bundle_tries_left']>0) & (download_log['download_bundle_tries_left']>0) & (download_log['mosaic_tries_left']>0)]
        
        # Update final results report
        report_results(args['ls_seasonal_dir'], args['years_range'], args['progress_log_csv'], args['fireid'])
        
if __name__ == "__main__":
    print(f'Running main_landsat_download.py with arguments {'\n'.join(sys.argv)}\n')
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

    # get relevant params
    args = {
        'fireid': fireid,
        'fire_shp': glob.glob(f'{fire_metadata['FIRE_BOUNDARY_PATH']}*wumi_mtbs_poly.shp')[0],
        'ls_data_dir': file_paths['INPUT_LANDSAT_DATA_DIR'],
        'ls_seasonal_dir': file_paths['INPUT_LANDSAT_SEASONAL_DIR'],
        'progress_log_csv': config['RECOVERY_PARAMS']['LOGGING_PROCESS_CSV'],
        'download_log_csv': os.path.join(file_paths['INPUT_LANDSAT_DATA_DIR'], 'download_log.csv'),
        'valid_layers': config['LANDSAT']['VALID_LAYERS'],
        'default_nodata': config['LANDSAT']['DEFAULT_NODATA'],
        'product_layers': config['LANDSAT']['PRODUCT_LAYERS'],
        'ndvi_bands_dict': config['LANDSAT']['NDVI_BANDS_DICT'],
        'rgb_bands_dict': config['LANDSAT']['RGB_BANDS_DICT'],
        'num_yrs_per_request': config['LANDSAT']['NUM_YRS_PER_REQUEST'],
        'fire_yr': fire_metadata['FIRE_YEAR'],
        'years_range': range(fire_metadata['FIRE_YEAR'] - int(config['RECOVERY_PARAMS']['YRS_PREFIRE_MATCHED']), datetime.now().year+1),
        'make_daily_rgb': True,
        'make_daily_ndvi': False
    }
    
    # get buffered fire polygon for requesting Landsat data in a fire + 10km buffer
    bufferedfire_gdf, bufferedfireShpPath = buffer_firepoly(args['fire_shp'])
    args['bufferedfire_gdf'] = bufferedfire_gdf
    args['bufferedfireShpPath'] = bufferedfireShpPath

    # print args to log
    for (key, val) in args.items():
        print(key, val, flush=True)
    
    # process all years with parallel workers (will skip any years that were successfully downloaded prior)
    process_all_years(args)