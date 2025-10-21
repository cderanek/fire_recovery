import sys, os, glob, json
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from download_log_helpers import *
sys.path.append("workflow/utils/") 
from earthaccess_downloads import *
from merge_process_scenes import mosaic_ndvi_timeseries


lock = Lock()

### Helper functions to process individual jobs, organize all years downloads, report results ##
def download_task(submission_order, start_date, task_id, bundle, dest_dir, download_log, args, fireid):
    print(f'Current time: {datetime.now()}')
    print(f'Downloading bundle with start date: {start_date}; task_id: {task_id}', flush=True)
    head = login_earthaccess()
    
    # Try to download bundle
    dest_dir_complete = download_landsat_bundle(bundle, task_id, head, dest_dir)
    print('DEST DIR COMPLETE FLAG')
    print(f'{dest_dir_complete}, {type(dest_dir_complete)}', flush=True)
    
    # Return results 
    return {
        'submission_order': submission_order,
        'success': isinstance(dest_dir_complete, str)
    }

def update_download_log(result, download_log, args, fireid):
    # Update download log
    if result['success']:
        print('download complete')
        download_log.loc[download_log['submit_order']==result['submission_order'], 'download_complete'] = True
    else:
        download_log.loc[download_log['submit_order']==submission_order, 'download_bundle_tries_left'] = download_log.loc[download_log['submit_order']==submission_order, 'download_bundle_tries_left'] - 1
        
    # Update download log csv
    update_csv_wlock(args['download_log_csv'], download_log, fireid)
    
    return download_log


def process_all_years(args: dict):
    # open download log
    download_log = read_csv_wait_for_content(args['download_log_csv'])

    # keep working on download until all years complete
    unsuccessful_years_w_retries = download_log['start_date'][
        (download_log['fireid']==args['fireid']) & 
        (download_log['ndvi_mosaic_complete']==False) &
        ((download_log['download_bundle_tries_left']>0) | (download_log['download_complete']==True)) & 
        (download_log['mosaic_tries_left']>0)
    ]
    print(f'YEARS TO DOWNLOAD/MOSAIC: {unsuccessful_years_w_retries}', flush=True)
    print(download_log[['start_date', 'download_bundle_tries_left']][download_log['fireid']==args['fireid']], flush=True)
    while len(unsuccessful_years_w_retries) > 0:
        # DOWNLOAD
        # for each task with a bundle, but incomplete download, try to download bundle
        incompletedownload_years_df = download_log[['submit_order','start_date', 'task_id', 'bundle', 'dest_dir']][
            (download_log['fireid']==args['fireid']) &
            (download_log['ndvi_mosaic_complete']==False) & 
            (download_log['download_complete']==False) & 
            # (download_log['bundle'].notna()) & 
            (download_log['download_bundle_tries_left']>0)
        ]

        print(f'YEARS TO DOWNLOAD: {incompletedownload_years_df['start_date']}', flush=True)

        with ThreadPoolExecutor(max_workers=4) as executor:
            # Submit all tasks
            futures = []
            for _, (submission_order, start_date, task_id, bundle, dest_dir) in incompletedownload_years_df.iterrows():
                future = executor.submit(download_task, submission_order, start_date, task_id, bundle, dest_dir, download_log, args, fireid)
                futures.append(future)

            for future in as_completed(futures):
                result = future.result()
                with lock:
                    download_log = update_download_log(result, download_log, args, fireid)

        # MOSAIC
        # for each task with a complete download, but incomplete ndvi_mosaic, try to create mosaic
        incomplete_mosaic_df = download_log[['start_date', 'end_date', 'dest_dir']][
            (download_log['fireid']==args['fireid']) &
            (download_log['download_complete']==True) & 
            (download_log['ndvi_mosaic_complete']==False) & 
            (download_log['mosaic_tries_left']>0)
        ]
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

            # Update download log csv
            update_csv_wlock(args['download_log_csv'], download_log, fireid)
        
        # ASSESS PROGRESS + UPDATE LOG
        # re-check the list of unsuccessful years with retries left to determine if we should keep looping
        unsuccessful_years_w_retries = download_log['start_date'][
            (download_log['fireid']==args['fireid']) &
            (download_log['ndvi_mosaic_complete']==False) & 
            (download_log['get_bundle_tries_left']>0) & 
            (download_log['download_bundle_tries_left']>0) & 
            (download_log['mosaic_tries_left']>0)
        ]
        
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
    download_log_path = os.path.join(
        os.path.dirname(config['RECOVERY_PARAMS']['LOGGING_PROCESS_CSV']),
        'allfires_download_log.csv'
    )

    # get relevant params
    args = {
        'fireid': fireid,
        'ls_seasonal_dir': file_paths['INPUT_LANDSAT_SEASONAL_DIR'],
        'progress_log_csv': config['RECOVERY_PARAMS']['LOGGING_PROCESS_CSV'],
        'download_log_csv': download_log_path,
        'valid_layers': config['LANDSAT']['VALID_LAYERS'],
        'default_nodata': config['LANDSAT']['DEFAULT_NODATA'],
        'ndvi_bands_dict': config['LANDSAT']['NDVI_BANDS_DICT'],
        'rgb_bands_dict': config['LANDSAT']['RGB_BANDS_DICT'],
        'years_range': range(fire_metadata['FIRE_YEAR'] - int(config['RECOVERY_PARAMS']['YRS_PREFIRE_MATCHED']), datetime.now().year+1),
        'make_daily_rgb': False,
        'make_daily_ndvi': False
    }

    # print args to log
    for (key, val) in args.items():
        print(key, val, flush=True)
    
    # process all years (will skip any years that were successfully downloaded prior)
    process_all_years(args)