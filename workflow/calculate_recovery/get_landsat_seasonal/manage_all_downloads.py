# This script creates/manages the download log csv containing all the task jsons,
# task statuses. It pings appeears periodically to check on the status of submitted jobs.
# Once all the tasks for a fire are finished, it creates a done flag to trigger the 
# perfire_recovery rule.

# This script will run with ~10G for 2 weeks on the lab node, 
# quiety triggering larger 24hr jobs on the shared nodes in the background

import json
import pandas as pd

LANDSAT_BAD_DATE = datetime(2024, 6, 8) # this date causes jobs to fail -- temp DAAC issue
SLEEP_TIME = 60*2 # 2min pause between pings


def create_download_log(
    config:dict, 
    perfire_config:dict
    )->pd.DataFrame:
    '''
    Download log cols:
        name: str
        fireid: str
        fire_year: int
        start_date: date 
        end_date: date 
        dest_dir: str 
        task_id: str
        bundle: str
        task_status: str (one of: unsubmitted, submitted, complete)
        task_submitted_time: date
        bundle_received_time: date 
        download_complete: bool
        ndvi_mosaic_complete: bool
        get_bundle_tries_left: int
        download_bundle_tries_left: int
        mosaic_tries_left: int
    '''
    # If it doesn't exist yet, creates a download log, ordered by task submit priority (keep fires together in order)

    # If any individual fire download logs exist, updates download log with information from them,
    # then deletes the individual fire log
    
    pass

def create_post_request(download_log_row):
    # create and submit task json

    # update the row (task_status, task_submitted_time, task_id)

    return updated_log_row


if __name__ == '__main__':
    print(f'Running manage_all_downloads.py with arguments {'\n'.join(sys.argv)}\n')
    main_config_path=sys.argv[1]
    prioritize_sensitiity = bool(sys.argv[2]) # if true, submit/monitor sensitivity fires, then exit

    # read in jsons
    with open(main_config_path, 'r') as f:
        config = json.load(f)

    # create download log
    download_log = create_download_log(config, perfire_config)

    # while not all tasks ready, keep looping
    not_done = True
    while not_done:
        # if <45 jobs active, submit tasks until up to 45
        active_jobs_count = (download_log['task_status'] == 'submitted').sum()
        jobs_to_submit_count = max(45 - active_jobs_count, 0)
        next_job_row_index = download_log[download_log['task_status'] == 'unsubmitted'].iloc[0]
        for i in range(next_job_row_index, min(next_job_row_index+jobs_to_submit_count, len(download_log))):
            updated_row = create_post_request(download_log.loc[i])
            download_log.loc[i] = updated_row

        # update status of all submitted, incomplete jobs


        # for fires where all the tasks are complete, create done flag


        # check if all jobs are processed, update not_done bool
        if prioritize_sensitiity:
            # filter to just sensitivity fires, then check if all jobs are done
            pass
        else: pass
