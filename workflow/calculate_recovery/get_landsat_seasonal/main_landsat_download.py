import sys, os, glob, json
import pandas as pd
import filelock
from functools import partial
from datetime import datetime, timedelta

from typing import Tuple, Union
NumericType = Union[int, float]
RangeType = Tuple[NumericType, NumericType]    

sys.path.append("workflow/utils/") 
from earthaccess_downloads import *
from merge_process_scenes import mosaic_ndvi_timeseries
from geo_utils import buffer_firepoly
SLEEP_TIME = 60*1 # 2min pause between pings

### Helper functions to process individual jobs, organize all years downloads, report results ##
def create_download_log(args):
    # if download log doesn't exist, create it
    if not os.path.exists(args['download_log_csv']):
        print(f'Making new {args['download_log_csv']}')
        years_range = list(args['years_range'])
        start_years = list(range(years_range[0], years_range[-1], args['num_yrs_per_request']))
        end_years = [yr-1 for yr in start_years[1:]] + [years_range[-1]]
        df = pd.DataFrame({
            'start_year': start_years, 
            'end_year': end_years,
            'dest_dir': '',
            'head': np.nan, 
            'task_id': np.nan, 
            'bundle': np.nan,
            'task_submitted_time': np.nan, 
            'bundle_received_time': np.nan, 
            'download_complete': False, 
            'ndvi_mosaic_complete': False,
            'get_bundle_tries_left': 20,
            'download_bundle_tries_left': 20,
            'mosaic_tries_left': 5
        })

    # if download log exists, reset any rows that were created >24hrs ago, but download not completed
    else:
        df = pd.read_csv(args['download_log_csv'])
        df['task_submitted_time'] = pd.to_datetime(df['task_submitted_time'], errors='coerce')
        df['bundle_received_time'] = pd.to_datetime(df['task_submitted_time'], errors='coerce')

        now = datetime.now()
        cutoff = now - timedelta(hours=24)

        reset_download_rows = (
            (df['download_complete'] == False) &
            (df['task_submitted_time'].notna()) &
            (df['task_submitted_time']< cutoff)
        )
        print(f'reset download rows: {reset_download_rows}')
        cols_to_reset = ['head', 'task_id', 'bundle', 'task_submitted_time', 'bundle_received_time']
        df.loc[reset_download_rows, cols_to_reset] = np.nan

    df['start_year'] = df['start_year'].astype('int')
    df['end_year'] = df['end_year'].astype('int')
    os.makedirs(os.path.dirname(args['download_log_csv']), exist_ok=True)
    df.to_csv(args['download_log_csv'], index=False)
    return df


def process_all_years(args: dict) -> dict:
    # cleanup file names for task submission
    roi_path = args['bufferedfireShpPath']
    cleaned_roi_path = re.sub(r'[^a-zA-Z0-9_-]', '', os.path.basename(roi_path))

    # create log to hold headers, task_ids, download status for all jobs
    download_log = create_download_log(args)
    unsubmitted_years_df = download_log[['start_year', 'end_year']][download_log['task_submitted_time'].isna()]
    
    # submit a task for all years
    for index, (start_year, end_year) in unsubmitted_years_df.iterrows():
        start_date=f'01-01-{start_year}'
        end_date=f'12-31-{end_year}'
        task_name = f'LS_{start_date}_{end_date}_{cleaned_roi_path}'
        dest_dir = os.path.join(args['ls_data_dir'], task_name)
        print(f'Dest dir {dest_dir}')

        task_json = create_product_request_json(
            task_name=task_name,
            start_date=start_date,
            end_date=end_date,
            shp_file_path=roi_path,
            product_layers=args['product_layers'],
            file_type='geotiff'
        )

        # Log in to earth access
        head = login_earthaccess()
        print(f'head: {head}')
        # Submit task
        task_id = post_request(task_json, head, max_retries=10)
        print(f'task id: {task_id}')

        # Update download log with dest_dir, head, task_id, task_submitted_time
        download_log.loc[index, 'dest_dir'] = dest_dir
        download_log.loc[index, 'head'] = head['Authorization']
        download_log.loc[index, 'task_id'] = task_id
        download_log.loc[index, 'task_submitted_time'] = datetime.now()
        download_log.to_csv(args['download_log_csv'], index=False)
        
    print(f'Download log after submitting all tasks: {download_log}')
    time.sleep(SLEEP_TIME) # to enforce sleep time between requests

    # keep working on download until all years complete
    successful_years = download_log['start_year'][download_log['ndvi_mosaic_complete']==True]
    unsuccessful_years_w_retries = download_log['start_year'][(download_log['ndvi_mosaic_complete']==False) & (download_log['get_bundle_tries_left']>0) & (download_log['download_bundle_tries_left']>0) & (download_log['mosaic_tries_left']>0)]
    while len(unsuccessful_years_w_retries) >= 1:
        print(f'Starting another round of checks: {unsuccessful_years_w_retries}\n{download_log}')
        
        # for each task with no bundle, ping appeears, and if ready, get bundle
        nodbundle_years_df = download_log[['start_year', 'task_id', 'head']][(download_log['bundle'].isna()) & (download_log['get_bundle_tries_left']>0)]
        for index, (start_year, task_id, head) in nodbundle_years_df.iterrows():
            print(f'Pinging appears for year: {start_year}; task_id: {task_id}')
            head = {'Authorization': head}
            task_complete = ping_appears_once(task_id, head)
            print(f'ping response: {task_complete}')
            time.sleep(SLEEP_TIME) # to enforce sleep time between requests
            if task_complete:
                # try to download bundles 
                print('task complete')
                bundle = try_get_bundle_once(task_id, head)
                time.sleep(SLEEP_TIME) # to enforce sleep time between requests
                print(bundle)

                # Update download log
                if bundle:
                    download_log.loc[index, 'bundle'] = bundle
                    download_log.loc[index, 'bundle_received_time'] = datetime.now()
                else:
                    download_log.loc[index, 'get_bundle_tries_left'] = download_log.loc[index, 'get_bundle_tries_left'] - 1

        # for each task with a bundle, but incomplete download, try to download bundle
        incompletedownload_years_df = download_log[['start_year', 'task_id', 'head', 'bundle', 'dest_dir']][(download_log['bundle'].notna()) & (download_log['download_bundle_tries_left']>0)]
        for index, (start_year, task_id, head, bundle, dest_dir) in incompletedownload_years_df.iterrows():
            print(f'Attempting download for year: {start_year}; task_id: {task_id}')
            head = {'Authorization': head}
            # try to download bundle
            dest_dir = download_landsat_bundle(bundle, task_id, head, dest_dir)

            # Update download log
            if dest_dir:
                download_log.loc[index, 'download_complete'] = True
            else:
                download_log.loc[index, 'download_bundle_tries_left'] = download_log.loc[index, 'download_bundle_tries_left'] - 1
        
        # for each task with a complete download, but incomplete ndvi_mosaic, try to create mosaic
        incomplete_mosaic_df = download_log[['start_year', 'end_year', 'dest_dir']][(download_log['download_complete']==True) & (download_log['ndvi_mosaic_complete']==False) & (download_log['mosaic_tries_left']>0)]
        for index, (start_year, end_year, dest_dir) in incomplete_mosaic_df.iterrows():
            print(f'Attempting mosaic for: {start_year}; dest_dir: {dest_dir}')
            try:
                mosaic_ndvi_timeseries(
                    dest_dir, args['valid_layers'], args['ls_seasonal_dir'], NODATA=args['default_nodata'], 
                    NDVI_BANDS_DICT=args['ndvi_bands_dict'], RGB_BANDS_DICT=args['rgb_bands_dict'],
                    MAKE_RGB=False, MAKE_DAILY_NDVI=False, 
                )
                # Update download log
                download_log.loc[index, 'ndvi_mosaic_complete'] = True

            except Exception as e:
                # Update mosaic_tries_left on download log
                download_log.loc[index, 'mosaic_tries_left'] = download_log.loc[index, 'mosaic_tries_left'] - 1
                print(f'Failed to mosaic from {dest_dir}.')

        # Update count of successful, unsuccessful years
        successful_years = download_log['start_year'][download_log['ndvi_mosaic_complete']==True]
        unsuccessful_years_w_retries = download_log['start_year'][(download_log['ndvi_mosaic_complete']==False) & (download_log['get_bundle_tries_left']>0) & (download_log['download_bundle_tries_left']>0) & (download_log['mosaic_tries_left']>0)]

    failed_years = download_log['start_year'][(download_log['ndvi_mosaic_complete']==False)]
    return successful_years, failed_years


def report_results(successful_years, failed_years, args):
    # list successful/unsuccessful years downloads
    if len(failed_years) == 0: 
        download_status='Complete'
    else: 
        download_status='Failed'
    
    # update submissions organizer csv
    lock_file = args['progress_log_csv'] + '.lock'
    lock = filelock.FileLock(lock_file, timeout=60)  # wait for lock if necessary (other batch jobs for other fires may also be waiting to update csv)
    try:
        with lock:
            csv = pd.read_csv(args['progress_log_csv'])
               
            # update row associated with just completed downloads
            mask = csv['fireid'] == args['fireid']
            csv.loc[mask, 'download_status'] = download_status
            csv.loc[mask, 'successful_years'] = str(successful_years)
            csv.loc[mask, 'failed_years'] = str(failed_years)
            
        # Save the updated csv
        csv.to_csv(args['progress_log_csv'], index=False)
        
    except filelock.Timeout:
        print("Could not acquire lock on file after waiting", flush=True)



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
        'ls_seasonal': file_paths['INPUT_LANDSAT_SEASONAL_DIR'],
        'progress_log_csv': config['RECOVERY_PARAMS']['LOGGING_PROCESS_CSV'],
        'download_log_csv': os.path.join(file_paths['INPUT_LANDSAT_DATA_DIR'], 'download_log.csv'),
        'valid_layers': config['LANDSAT']['VALID_LAYERS'],
        'default_nodata': config['LANDSAT']['DEFAULT_NODATA'],
        'product_layers': config['LANDSAT']['PRODUCT_LAYERS'],
        'ndvi_bands_dict': config['LANDSAT']['NDVI_BANDS_DICT'],
        'rgb_bands_dict': config['LANDSAT']['RGB_BANDS_DICT'],
        'num_yrs_per_request': config['LANDSAT']['NUM_YRS_PER_REQUEST'],
        'fire_yr': fire_metadata['FIRE_YEAR'],
        'years_range': range(fire_metadata['FIRE_YEAR'] - int(config['RECOVERY_PARAMS']['YRS_PREFIRE_MATCHED']), 2025)
    }
    
    for (key, val) in args.items():
        print(key, val, flush=True)
    
    # get buffered fire polygon for requesting Landsat data in a fire + 10km buffer
    bufferedfire_gdf, bufferedfireShpPath = buffer_firepoly(args['fire_shp'])
    args['bufferedfire_gdf'] = bufferedfire_gdf
    args['bufferedfireShpPath'] = bufferedfireShpPath
    
    # process all years with parallel workers (will skip any years that were successfully downloaded prior)
    successful_years, failed_years = process_all_years(args)

    # Print, store results summary
    report_results(successful_years, failed_years, args)