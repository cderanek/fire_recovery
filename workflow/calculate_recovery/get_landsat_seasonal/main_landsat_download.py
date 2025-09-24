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
SLEEP_TIME = 60*2 # 2min pause between pings


### Helper functions to process individual jobs, organize all years downloads, report results ##
def create_download_log(args):
    # if download log doesn't exist, create it
    if not os.path.exists(args['download_log_csv']):
        print(f'Making new {args['download_log_csv']}')
        years_range = list(args['years_range'])
        current_year = datetime.now().year
        if years_range[-1] != current_year:
            start_years = list(range(years_range[0], years_range[-1], args['num_yrs_per_request']))
            end_years = [yr-1 for yr in start_years[1:]] + [years_range[-1]]
        else: # make separate requests for more recent years -- more likely to fail
            start_years = list(range(years_range[0], current_year-2, args['num_yrs_per_request'])) + [current_year-1, current_year]
            end_years = [yr-1 for yr in start_years[1:]] + [current_year]
        print(start_years, flush=True)
        print(end_years, flush=True)
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

    # if download log exists already
    else:
        # FOR YEAR GROUPS WHERE DOWNLOAD DIDN'T COMPLETE
        # reset any rows that were created >28days ago, but download not completed
        df = pd.read_csv(args['download_log_csv'])
        df['task_submitted_time'] = pd.to_datetime(df['task_submitted_time'], errors='coerce')
        df['bundle_received_time'] = pd.to_datetime(df['task_submitted_time'], errors='coerce')
        df['mosaic_tries_left'] = 5 # reset mosaic tries left
        df['download_bundle_tries_left'] = 5 # reset download bundle tries left
        now = datetime.now()
        cutoff = now - timedelta(days=28)

        reset_download_rows = (
            (df['download_complete'] == False) &
            (df['task_submitted_time'].notna()) &
            (df['task_submitted_time']< cutoff)
        )
        print(f'reset download rows: {reset_download_rows}')
        cols_to_reset = ['head', 'task_id', 'bundle', 'task_submitted_time', 'bundle_received_time']
        df.loc[reset_download_rows, cols_to_reset] = np.nan
        
    df = df.astype({
        'start_year': 'int', 
        'end_year': 'int',
        'dest_dir': 'str',
        'head': 'object', 
        'task_id': 'object', 
        'bundle': 'object',
        'task_submitted_time': 'object', 
        'bundle_received_time': 'object', 
        'download_complete': 'bool', 
        'ndvi_mosaic_complete': 'bool',
        'get_bundle_tries_left': 'int',
        'download_bundle_tries_left': 'int',
        'mosaic_tries_left': 'int'
    })

    # make sure failed/successful year results are up-to-date
    successful_years, unsuccessful_years = report_results(args)

    # if all years between start and end are successfully mosaiced, update download_complete and ndvi_mosaic_complete to be True
    for index, (start_year, end_year) in df[['start_year', 'end_year']].iterrows():
        allyrs = list(range(start_year, end_year+1))
        if sum([yr in unsuccessful_years for yr in allyrs]) == 0:
            df.loc[index, 'download_complete'] = True
            df.loc[index, 'ndvi_mosaic_complete'] = True

    os.makedirs(os.path.dirname(args['download_log_csv']), exist_ok=True)
    df.to_csv(args['download_log_csv'], index=False)
    return df


def get_successful_failed_years(seasonal_dir, desired_years_range):
    successful_years, failed_years = [], []
    for year in desired_years_range:
        mosaiced = []
        for season in ['01', '02', '03', '04']:
            mosaiced.extend(glob.glob(os.path.join(seasonal_dir, f'{year}{season}_season_mosaiced.tif')))
        if len(mosaiced)==4: successful_years.append(year)
        else: failed_years.append(year)

    return successful_years, failed_years


def report_results(args):
    # list successful/unsuccessful years downloads
    successful_years, failed_years = get_successful_failed_years(args['ls_seasonal_dir'], args['years_range'])
    if len(failed_years) == 0: 
        download_status='Complete'
    else: 
        download_status='Failed'
    
    # update submissions organizer csv
    lock_file = args['progress_log_csv'] + '.lock'
    lock = filelock.FileLock(lock_file, timeout=60)  # wait for lock, if necessary (other batch jobs for other fires may also be waiting to update csv)
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

    return successful_years, failed_years

def process_all_years(args: dict):
    # cleanup file names for task submission
    roi_path = args['bufferedfireShpPath']
    cleaned_roi_path = re.sub(r'[^a-zA-Z0-9_-]', '', os.path.basename(roi_path))

    # create log to hold headers, task_ids, download status for all jobs
    download_log = create_download_log(args)
    unsubmitted_years_df = download_log[['start_year', 'end_year']][(download_log['task_submitted_time'].isna()) & (download_log['ndvi_mosaic_complete']==False)]
    
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
        
    print(f'Download log after submitting all tasks: {download_log}', flush=True)

    # keep working on download until all years complete
    unsuccessful_years_w_retries = download_log['start_year'][(download_log['ndvi_mosaic_complete']==False) & (download_log['get_bundle_tries_left']>0) & (download_log['download_bundle_tries_left']>0) & (download_log['mosaic_tries_left']>0)]
    while len(unsuccessful_years_w_retries) >= 1:
        print(f'Starting another round of checks: {unsuccessful_years_w_retries}\n{download_log}', flush=True)
        download_log.to_csv(args['download_log_csv'], index=False)
        # for each task with no bundle, ping appeears, and if ready, get bundle
        nodbundle_years_df = download_log[['start_year', 'task_id', 'head']][(download_log['bundle'].isna()) & (download_log['get_bundle_tries_left']>0)]
        for index, (start_year, task_id, head) in nodbundle_years_df.iterrows():
            print(f'Pinging appears for year: {start_year}; task_id: {task_id}')
            head = {'Authorization': head}
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
        incompletedownload_years_df = download_log[['start_year', 'task_id', 'head', 'bundle', 'dest_dir']][(download_log['download_complete']==False) & (download_log['bundle'].notna()) & (download_log['download_bundle_tries_left']>0)]
        for index, (start_year, task_id, head, bundle, dest_dir) in incompletedownload_years_df.iterrows():
            print(f'Downloading bundle with start year: {start_year}; task_id: {task_id}')
            head = {'Authorization': head}
            # try to download bundle
            dest_dir_complete = None
            dest_dir_complete = download_landsat_bundle(bundle, task_id, head, dest_dir)

            # Update download log
            if dest_dir_complete:
                download_log.loc[index, 'download_complete'] = True
            else:
                download_log.loc[index, 'download_bundle_tries_left'] = download_log.loc[index, 'download_bundle_tries_left'] - 1
        
        # for each task with a complete download, but incomplete ndvi_mosaic, try to create mosaic
        incomplete_mosaic_df = download_log[['start_year', 'end_year', 'dest_dir']][(download_log['download_complete']==True) & (download_log['ndvi_mosaic_complete']==False) & (download_log['mosaic_tries_left']>0)]
        for index, (start_year, end_year, dest_dir) in incomplete_mosaic_df.iterrows():
            print(f'Starting seasonal mosaic for: {start_year}; dest_dir: {dest_dir}')
            try:
                mosaic_ndvi_timeseries(
                    dest_dir, args['valid_layers'], args['ls_seasonal_dir'], NODATA=args['default_nodata'], 
                    NDVI_BANDS_DICT=args['ndvi_bands_dict'], RGB_BANDS_DICT=args['rgb_bands_dict'],
                    MAKE_RGB=args['make_daily_rgb'], MAKE_DAILY_NDVI=args['make_daily_ndvi']
                )
                # Update download log
                download_log.loc[index, 'ndvi_mosaic_complete'] = True

                # Update final results report
                report_results(args)

            except Exception as e:
                # Update mosaic_tries_left on download log
                print(f'Failed to mosaic from {dest_dir}.')
                print(e)
                download_log.loc[index, 'mosaic_tries_left'] = download_log.loc[index, 'mosaic_tries_left'] - 1



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