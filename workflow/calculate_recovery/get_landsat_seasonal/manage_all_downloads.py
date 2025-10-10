# This script creates/manages the download log csv containing all the task jsons,
# task statuses. It pings appeears periodically to check on the status of submitted jobs.
# Once all the tasks for a fire are finished, it creates a done flag to trigger the 
# perfire_recovery rule.

# This script will run with ~10G for 2 weeks on the lab node, 
# quiety triggering larger 24hr jobs on the shared nodes in the background

import json, sys
import pandas as pd
import numpy as np
from datetime import datetime
import subprocess
from download_log_helpers import *


if __name__ == '__main__':
    print(datetime.now())
    print(f'Running manage_all_downloads.py with arguments {'\n'.join(sys.argv)}\n')
    main_config_path=sys.argv[1]
    perfire_config_path=sys.argv[2]
    fireid_done_template=sys.argv[3]


    # read in jsons
    with open(main_config_path, 'r') as f:
        config = json.load(f)
    with open(perfire_config_path, 'r') as f:
        perfire_config = json.load(f)


    # create download log (organized by sensitivity, fireid, date)
    download_log, download_log_path = create_download_log(config, perfire_config)
    print(f'Download log can be found at: {download_log_path}')

    # while not all tasks ready, keep looping
    not_done = True

    while not_done:
        # if <25 jobs active and there are unsubmitted tasks left to submit, submit tasks until up to 25
        active_jobs_count = (
            (download_log['ndvi_mosaic_complete']==False) &
            (download_log['task_status'] == 'submitted') &
            (download_log['get_bundle_tries_left']>0)
            ).sum()
        jobs_to_submit_count = max(25 - active_jobs_count, 0)
        unsubmitted_tasks = download_log[download_log['task_status'] == 'unsubmitted']
        if len(unsubmitted_tasks) > 0:
            next_job_row_index = unsubmitted_tasks['submit_order'].values[0]
            for i in range(next_job_row_index, min(next_job_row_index+jobs_to_submit_count, len(download_log))):
                create_post_request(download_log, download_log_path, i, config, perfire_config)

        # update status of all submitted, incomplete jobs
        download_log, new_fires_ready = update_status_incomplete_tasks(download_log, download_log_path)

        # for fires where all the tasks are complete, create done flag
        [subprocess.run(
            ['touch', fireid_done_template.replace('fireid',fireid)]
            ) for fireid in new_fires_ready]

        # check if all jobs are processed, update not_done bool
        jobs_not_ready_mask = (
            (download_log['ndvi_mosaic_complete']==False) &
            (download_log['task_status'] != 'ready_to_download') &
            (download_log['get_bundle_tries_left']>0)
            )
        jobs_not_ready_count = jobs_not_ready_mask.sum()
        unique_fires_left = np.unique(download_log.loc[jobs_not_ready_mask, 'fireid'])
        not_done = jobs_not_ready_count>0
        
        if not_done:
            print(f'Still have {jobs_not_ready_count} jobs over {len(unique_fires_left)} unique fires left to complete.', flush=True)
        else:
            print('All fires are ready for download!')