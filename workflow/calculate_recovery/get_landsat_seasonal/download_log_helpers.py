import sys, os, glob, json
import pandas as pd
import numpy as np
import filelock
import time
from datetime import datetime, timedelta
from earthaccess_downloads import *
from typing import List

sys.path.append("workflow/utils/") 
from geo_utils import export_to_tiff, reproj_align_rasters, buffer_firepoly

LANDSAT_BAD_DATE = datetime(2024, 6, 8) # this date causes jobs to fail -- temp DAAC issue
SLEEP_TIME = 120 # 2min pause between requests


### Helper functions to process individual jobs, organize all years downloads, report results ##
def skip_bad_dates(
    start_dates: List[datetime], 
    end_dates: List[datetime], 
    bad_date: datetime = LANDSAT_BAD_DATE
    ):
    '''Given a range of start/end dates, for any download request that would span the bad date,
    split that request into 2 requests -- skipping over the bad date.
    '''
    new_start_dates, new_end_dates = [], []

    for start_date, end_date in zip(start_dates, end_dates):
        if start_date < bad_date < end_date:
            # split the date range to end the day before the bad date,
            # and then start back up 1 day after the bad date
            new_start_dates.append(start_date)
            new_end_dates.append(bad_date - timedelta(days=1))
            new_start_dates.append(bad_date + timedelta(days=1))
            new_end_dates.append(end_date)

        else:
            # keep the original date range
            new_start_dates.append(start_date)
            new_end_dates.append(end_date)

    return new_start_dates, new_end_dates


def get_successful_failed_years(seasonal_dir, desired_years_range):
    successful_years, failed_years = [], []
    curr_yr_expected_seasons = 2 #np.floor((4*datetime.now().month)/12.0) ## having appeears access errors downloading after mid-year 2025, truncating for now
    for year in desired_years_range:
        mosaiced = []
        for season in ['01', '02', '03', '04']:
            mosaiced.extend(glob.glob(os.path.join(seasonal_dir, f'{year}{season}_season_mosaiced.tif')))

        if len(mosaiced)==4: 
            successful_years.append(year)
        elif (year == int(datetime.now().year)) and (len(mosaiced) >= curr_yr_expected_seasons): 
            successful_years.append(year)
        else: failed_years.append(year)

    return successful_years, failed_years


def report_results(ls_seasonal_dir, years_range, progress_log_path, fireid):
    # list successful/unsuccessful years downloads
    successful_years, failed_years = get_successful_failed_years(ls_seasonal_dir, years_range)
    if len(failed_years) == 0: 
        download_status='Complete'
    else: 
        download_status='Failed'
    
    # update submissions organizer csv
    lock_file = progress_log_path + '.lock'
    lock = filelock.FileLock(lock_file, timeout=60)  # wait for lock, if necessary (other batch jobs for other fires may also be waiting to update csv)
    try:
        with lock:
            csv = pd.read_csv(progress_log_path)
               
            # update row associated with just completed downloads
            mask = csv['fireid'] == fireid
            csv.loc[mask, 'download_status'] = download_status
            csv.loc[mask, 'successful_years'] = str(successful_years)
            csv.loc[mask, 'failed_years'] = str(failed_years)
            
            # Save the updated csv
            csv.to_csv(progress_log_path, index=False)
        
    except filelock.Timeout:
        print("Could not acquire lock on file after waiting", flush=True)

    return successful_years, failed_years


def generate_default_perfire_download_log(fireid, file_paths, fire_metadata, config):
    # Generate job task dates
    num_yrs_per_request = config['LANDSAT']['NUM_YRS_PER_REQUEST']
    years_range = list(range(
        fire_metadata['FIRE_YEAR'] - int(config['RECOVERY_PARAMS']['YRS_PREFIRE_MATCHED']), 
        datetime.now().year+1
        ))
    start_years = range(years_range[0], years_range[-1], num_yrs_per_request)
    start_dates = list(f'01-01-{year}' for year in start_years)
    end_dates = [f'12-31-{year-1}' for year in start_years[1:]] + [f'06-30-{years_range[-1]}']
    start_dates, end_dates = skip_bad_dates(pd.to_datetime(start_dates), pd.to_datetime(end_dates))

    # Create buffered fire shp path
    # get buffered fire polygon for requesting Landsat data in a fire + 10km buffer
    fire_poly_orig = glob.glob(f'{fire_metadata['FIRE_BOUNDARY_PATH']}*wumi_mtbs_poly.shp')[0]
    _, bufferedfireShpPath = buffer_firepoly(fire_poly_orig)

    # Create perfire df with default values
    perfire_df = pd.DataFrame({
        'fire_name': fire_metadata['FIRE_NAME'],
        'fireid': fireid,
        'sensitivity': fire_metadata['SENSITIVITY_ANALYSIS'],
        'fire_year': fire_metadata['FIRE_YEAR'],
        'start_date': start_dates,
        'end_date': end_dates,
        'dest_dir': '',
        'task_id': np.nan,
        'bundle': np.nan,
        'task_status': 'unsubmitted', # one of: unsubmitted, submitted, ready_to_download 
        'task_submitted_time': np.nan,
        'bundle_received_time': np.nan,
        'download_complete': False,
        'ndvi_mosaic_complete': False,
        'get_bundle_tries_left': 20,
        'download_bundle_tries_left': 20,
        'mosaic_tries_left': 5,
        'bufferedfire_shp_path': bufferedfireShpPath
    })

    return perfire_df, years_range


def update_perfire_df_from_oldlogs(perfire_df, fireid, years_range, file_paths, config):
    """
    check for unfinished tasks, existing tasks, and mark which years are completed
    """
    # If all years between start and end are successfully mosaiced, set download_complete and ndvi_mosaic_complete to True
    downloaded_years, undownloaded_years = report_results(
        file_paths['INPUT_LANDSAT_DATA_DIR'], 
        years_range, 
        config['RECOVERY_PARAMS']['LOGGING_PROCESS_CSV'], 
        fireid)

    for index, (start_date, end_date) in perfire_df[['start_date', 'end_date']].iterrows():
        start_year, end_year = start_date.year, end_date.year
        allyrs = list(range(start_year, end_year+1))
        if sum([yr in undownloaded_years for yr in allyrs]) == 0:
            perfire_df.loc[index, 'download_complete'] = True
            perfire_df.loc[index, 'ndvi_mosaic_complete'] = True

    # If any individual fire download logs exist, updates download log with information from them,
    # then delete the individual fire (vestige of old download log system)
    perfire_download_log_path = os.path.join(file_paths['INPUT_LANDSAT_DATA_DIR'], 'download_log.csv')
    if os.path.exists(perfire_download_log_path):
        # find rows with tasks created <30 days ago where download not complete
        old_log = pd.read_csv(perfire_download_log_path)
        old_log['task_submitted_time'] = pd.to_datetime(old_log['task_submitted_time'], errors='coerce')
        old_log['bundle_received_time'] = pd.to_datetime(old_log['task_submitted_time'], errors='coerce')
        now = datetime.now()
        cutoff = now - timedelta(days=30)

        inprogress_jobs_rows = (
            (old_log['download_complete'] == False) &
            (old_log['task_submitted_time'].notna()) &
            (old_log['task_submitted_time'] > cutoff)
        )

        # update task_id, bundle, task_submitted_time, bundle_received_time
        update_cols = ['start_date', 'end_date', 'task_id', 'bundle', 'dest_dir']
        for _, row in old_log.loc[inprogress_jobs_rows, update_cols].iterrows():
            start_date = row['start_date']
            end_date = row['end_date']

            match_rows = (
                (perfire_df['start_date'] == start_date) &
                (perfire_df['end_date'] == end_date)
            )

            if match_rows.any():
                perfire_df.loc[match_rows, ['task_id', 'bundle', 'dest_dir']] = (
                    row[['task_id', 'bundle', 'dest_dir']].values
                )

    return perfire_df

def format_download_log(download_log):
    download_log['start_date'] = pd.to_datetime(download_log['start_date'], errors='coerce')
    download_log['end_date'] = pd.to_datetime(download_log['end_date'], errors='coerce')
    download_log = download_log.astype({
        'fire_name': str,
        'fireid': str,
        'sensitivity': bool,
        'fire_year': int,
        'start_date': object, 
        'end_date': object,
        'dest_dir': str,
        'task_id': object, 
        'bundle': object,
        'task_status': str,
        'task_submitted_time': object, 
        'bundle_received_time': object, 
        'download_complete': bool, 
        'ndvi_mosaic_complete': bool,
        'get_bundle_tries_left': int,
        'download_bundle_tries_left': int,
        'mosaic_tries_left': int,
        'bufferedfire_shp_path': str
    })

    return download_log



def create_download_log(
    config:dict, 
    perfire_config:dict
    )->pd.DataFrame:
    '''
    If it doesn't exist yet, creates a download log, ordered by task submit priority 
    (keep fires together in order)

    Download log cols:
        name: str
        fireid: str
        sensitivity: bool
        fire_year: int
        start_date: date 
        end_date: date 
        dest_dir: str 
        task_id: str
        bundle: str
        task_status: str (one of: unsubmitted, submitted, ready_to_download)
        task_submitted_time: date
        bundle_received_time: date 
        download_complete: bool
        ndvi_mosaic_complete: bool
        get_bundle_tries_left: int
        download_bundle_tries_left: int
        mosaic_tries_left: int,
        bufferedfire_shp_path: str (path to buffered fire polygon bbox for generating requests)
    '''
    download_log_path = os.path.join(
        os.path.dirname(config['RECOVERY_PARAMS']['LOGGING_PROCESS_CSV']),
        'allfires_download_log.csv'
    )

    # If download log exists, open and return it
    if os.path.exists(download_log_path):
        download_log = pd.read_csv(download_log_path)
        download_log = format_download_log(download_log)
        return download_log, download_log_path

    # Otherwise, create download log and save it
    # create list to hold perfire dfs
    download_log_dfs = []

    # iterate over all fires, adding them to download log
    for fireid in perfire_config.keys():
        file_paths = perfire_config[fireid]['FILE_PATHS']
        fire_metadata = perfire_config[fireid]['FIRE_METADATA']

        # create perfire download log df with default values
        perfire_df, years_range = generate_default_perfire_download_log(fireid, file_paths, fire_metadata, config)
        
        # update download_complete and ndvi_mosaic_complete for each potential task
        # If recovery has been made, mark all as complete
        if os.path.exists(file_paths['OUT_TIFS_D']['fire_recovery_time'][0]):
            perfire_df['download_complete'] = True
            perfire_df['ndvi_mosaic_complete'] = True

        # Otherwise, check for unfinished tasks, existing tasks, and mark which years are completed
        else:
            perfire_df = update_perfire_df_from_oldlogs(perfire_df, fireid, years_range, file_paths, config)

        download_log_dfs.append(perfire_df)
    
    # Concatenate all perfire dfs and save to csv
    download_log = pd.concat(download_log_dfs)                      # concat all perfire dfs
    download_log = format_download_log(download_log)                # format nicely
    download_log = download_log.sort_values(
        by=['sensitivity', 'fireid', 'start_date'],
        ascending=False)                                                           # sort by sensitivity, then fireid, then start date
    download_log['submit_order'] = range(len(download_log))
    os.makedirs(os.path.dirname(download_log_path), exist_ok=True)  # save csv
    download_log.to_csv(download_log_path, index=False)

    return download_log, download_log_path


def create_post_request(download_log, download_log_path, index, config, perfire_config):
    download_log_row = download_log.loc[download_log['submit_order']==index].iloc[0]
    print(download_log_row)

    if download_log_row['task_status']!='unsubmitted':
        print(f'Download row already submitted. Skipping row: \n{download_log_row}', flush=True)
        return None

    # Get LS download data
    fireid = download_log_row['fireid']
    file_paths = perfire_config[fireid]['FILE_PATHS']
    ls_data_dir = file_paths['INPUT_LANDSAT_DATA_DIR']
    product_layers = config['LANDSAT']['PRODUCT_LAYERS']

    # create and submit task json
    roi_path = download_log_row['bufferedfire_shp_path']
    cleaned_roi_path = re.sub(r'[^a-zA-Z0-9_-]', '', os.path.basename(roi_path))
    start_date, end_date = download_log_row['start_date'], download_log_row['end_date']
    start_date_str=f'{start_date.month}-{start_date.day}-{start_date.year}'
    end_date_str=f'{end_date.month}-{end_date.day}-{end_date.year}'
    task_name = f'LS_{start_date_str}_{end_date_str}_{cleaned_roi_path}'

    # Format dest dir
    # for the task spanning the bad date, need to output 2 tasks to the same data dir to avoid splitting up data from the same year
    if (end_date.month == LANDSAT_BAD_DATE.month) and (end_date.year == LANDSAT_BAD_DATE.year): # bad date start of split
        next_end_date = download_log.loc[download_log['submit_order']==index-1, 'end_date'].iloc[0]
        next_end_date_str=f'{next_end_date.month}-{next_end_date.day}-{next_end_date.year}'
        dest_dir = os.path.join(ls_data_dir, f'LS_{start_date_str}_{next_end_date_str}_{cleaned_roi_path}')
    elif (start_date.month == LANDSAT_BAD_DATE.month) and (start_date.year == LANDSAT_BAD_DATE.year): # bad date end of split
        # dest_dir = download_log.loc[download_log['submit_order']==index+1, 'dest_dir'].iloc[0]
        prev_start_date = download_log.loc[download_log['submit_order']==index+1, 'start_date'].iloc[0]
        prev_start_date_str=f'{prev_start_date.month}-{prev_start_date.day}-{prev_start_date.year}'
        dest_dir = os.path.join(ls_data_dir, f'LS_{prev_start_date_str}_{end_date_str}_{cleaned_roi_path}')
    else: # regular case
        dest_dir = os.path.join(ls_data_dir, task_name)

    print(f'TASK NAME: {task_name} DEST DIR: {dest_dir}')

    task_json = create_product_request_json(
        task_name=task_name,
        start_date=start_date_str,
        end_date=end_date_str,
        shp_file_path=roi_path,
        product_layers=product_layers,
        file_type='geotiff'
    )

    # Log in to earth access
    head = login_earthaccess()
    # Submit task
    task_id = post_request(task_json, head, max_retries=30)

    # Update download log row with task_status, task_submitted_time, dest_dir, task_id
    download_log.loc[download_log['submit_order']==index, 'task_status'] = 'submitted'
    download_log.loc[download_log['submit_order']==index, 'task_submitted_time'] = datetime.now()
    download_log.loc[download_log['submit_order']==index, 'dest_dir'] = dest_dir
    download_log.loc[download_log['submit_order']==index, 'task_id'] = task_id
    
    # Update csv
    download_log.to_csv(download_log_path, index=False)
    pass


def update_status_incomplete_tasks(download_log, download_log_path):
    # for each task with 'submitted' download_status, ping appeears, and if ready, change status to 'ready_to_download'
    active_tasks = (download_log['ndvi_mosaic_complete']==False) & (download_log['task_status']=='submitted') & (download_log['get_bundle_tries_left']>0)
    submitted_tasks = download_log[['start_date', 'task_id', 'submit_order', 'fire_name']][active_tasks]

    for _, (start_date, task_id, submit_order, fire_name) in submitted_tasks.iterrows():
        # ping appeears for this task
        head = login_earthaccess()
        task_complete = ping_appears_once(task_id, head)
        print(f'Pinging appeears for start date: {fire_name} {start_date}; task_id: {task_id}; ping response: \t {task_complete}', flush=True)
        time.sleep(SLEEP_TIME) # to enforce sleep time between requests

        if task_complete==True:
            # try to download bundles 
            print('task complete')
            bundle = try_get_bundle_once(task_id, head)
            print('received bundle')

            # Update download log
            if type(bundle)!=type(np.nan):
                download_log.loc[download_log['submit_order']==submit_order, 'bundle'] = json.dumps(bundle)
                download_log.loc[download_log['submit_order']==submit_order, 'bundle_received_time'] = datetime.now()
                download_log.loc[download_log['submit_order']==submit_order, 'task_status'] = 'ready_to_download'

            else:
                download_log.loc[download_log['submit_order']==submit_order, 'get_bundle_tries_left'] = download_log.loc[download_log['submit_order']==submit_order, 'get_bundle_tries_left'] - 1
            
        # Update csv
        download_log.to_csv(download_log_path, index=False)

    # Update list of ready fires
    fires_not_ready = set(download_log.loc[download_log['task_status'] != 'ready_to_download', 'fireid'])
    all_fires = set(download_log['fireid'])
    ready_fireids = list(all_fires - fires_not_ready)

    # return fireids for new fires ready
    return download_log, ready_fireids


def read_csv_wait_for_content(filepath, timeout=120, check_interval=0.5):
    """Wait until file has content and is readable."""
    start_time = time.time()
    last_error = None
    
    while time.time() - start_time < timeout:
        try:
            # Check if file exists and has content
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                # Try to read the CSV
                df = pd.read_csv(filepath)
                # Verify we got actual data
                if len(df) > 0 and len(df.columns) > 0:
                    return df
                else:
                    # File read but empty, wait and retry
                    time.sleep(check_interval)
                    continue
            else:
                # File doesn't exist or is empty
                time.sleep(check_interval)
                continue
                
        except pd.errors.EmptyDataError as e:
            # File is being written to or is temporarily empty
            last_error = e
            time.sleep(check_interval)
            continue
        except (PermissionError, IOError) as e:
            # File is locked
            last_error = e
            time.sleep(check_interval)
            continue
        except Exception as e:
            # Other pandas parsing errors (partial write)
            last_error = e
            time.sleep(check_interval)
            continue
    
    raise TimeoutError(f"Could not read valid data from {filepath} after {timeout} seconds. Last error: {last_error}")


def update_csv_wlock(f_to_update, download_log, fireid, timeout=60):
    # Update download log csv
    lock_file = f_to_update + '.lock'
    lock = filelock.FileLock(lock_file, timeout=timeout)  # wait for lock, if necessary (other batch jobs for other fires may also be waiting to update csv)
    try:
        with lock:
            csv = read_csv_wait_for_content(f_to_update)
               
            # update rows associated with this fireid
            mask = csv['fireid'] == fireid
            csv.loc[mask] = download_log[mask]
            
            # Save the updated csv
            csv.to_csv(f_to_update, index=False)

    except filelock.Timeout:
        print("Could not acquire lock on file after waiting", flush=True)