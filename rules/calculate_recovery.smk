import json, sys, os

configfile: 'configs/config.yml'
sys.path.append('rules/')
from common import *


### Set paths based on testing mode ###
TESTING = config['TESTING']
if TESTING: ROI_PATH = config['TEST_ROI']
else: ROI_PATH = config['ROI']

# The list of fireids will be generated after the mtbs bundles rule completes
# NOTE that this does not strictly enforce the job order priority. BUT, it allows you to taget only active jobs or sensitivity jobs when running the main snakefile by targetting that rule, rather than the full perfire recovery rule
# Without specifying a target, no specific fireid order will be enforced at all
FIREIDS_PRIORITY = None


### RECOVERY CONFIG SETUP ###
rule make_recovery_config:
    input:
       get_path("logs/baselayers/done/mtbs_bundles.done", ROI_PATH)

    output:
        main_config_out=get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}main_config.json', ROI_PATH),
        perfire_config_out=get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}perfire_config.json', ROI_PATH),
        done_flag=get_path('logs/calculate_recovery/done/makeconfigs.done', ROI_PATH)
        
    params:
        conda_env='RIO_GPD',
        config_path='configs/config.yml',
        email=config['NOTIFY_EMAIL']

    conda: 
        '../workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/calculate_recovery/makeconfigs.log', ROI_PATH),
        stderr=get_path('logs/calculate_recovery/makeconfigs.err', ROI_PATH)

    shell:
        """
        workflow/calculate_recovery/sh_scripts/generate_configs.sh \
             {params.conda_env} \
             {params.config_path} \
             {output.main_config_out} \
             {output.perfire_config_out} \
             {output.done_flag}  > {log.stdout} 2> {log.stderr}
        """


### MAKE ALL PER-FIRE RECOVERY MAPS ###

## HELPER FUNCTIONS (ORGANIZE DOWNLOAD PRIORITY)
checkpoint generate_fire_list:
    input:
        get_path("logs/baselayers/done/mtbs_bundles.done", ROI_PATH)
    
    output:
        get_path("logs/baselayers/done/ready_to_generate_fireids.done", ROI_PATH)

    params:
        email=config['NOTIFY_EMAIL']

    shell: "touch {output}"


def get_fireids():
    # wait for checkpoint
    checkpoints.generate_fire_list.get()

    global FIREIDS_PRIORITY

    if FIREIDS_PRIORITY == None:
        # Initialize fireids priority ict
        FIREIDS_PRIORITY = {
                'active_fireids': set(),
                'sensitivity_fireids': set(),
                'remaining_fireids': set()
            }
        # Get list of all fireids from wumi csv
        wumi_csv_path = f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}wumi_data.csv'
        wumi_data = pd.read_csv(get_path(wumi_csv_path, ROI_PATH))
        all_fireids = set(list(wumi_data['fireid'].values))

        # Get list of active jobs from progress log csv
        progress_log_csv_path = get_path(config['RECOVERY_PARAMS']['LOGGING_PROCESS_CSV'], ROI_PATH)
        if os.path.exists(progress_log_csv_path):
            progress_log_data = pd.read_csv(progress_log_csv_path)[['fireid', 'download_status']]
            failed_jobs = set(list(progress_log_data.loc[progress_log_data['download_status']=='Failed', 'fireid'].values))
            FIREIDS_PRIORITY['active_fireids'] = failed_jobs

        # Get list of sensitivity analyses from sensitivity csv
        sensitivity_csv_path = config['SENSITIVITY']['output_sensitivity_selected_csv']
        if os.path.exists(sensitivity_csv_path):
            sensitivity_data = pd.read_csv(sensitivity_csv_path)[['fireid', 'sensitivity_selected']]
            fireids_sensitivity = set(list(sensitivity_data.loc[sensitivity_data['sensitivity_selected']==True, 'fireid'].values))
            FIREIDS_PRIORITY['sensitivity_fireids'] = fireids_sensitivity

        # Update list of remaining fireids
        FIREIDS_PRIORITY['remaining_fireids'] = all_fireids - failed_jobs - fireids_sensitivity
    
    with open('priority_fireids_dict.txt', 'w') as f:
        print(FIREIDS_PRIORITY, file=f)
    return FIREIDS_PRIORITY


def get_active_fireids(wildcards):
    checkpoints.generate_fire_list.get()
    fireids = get_fireids()
    return expand(get_path('logs/calculate_recovery/done/perfire_recovery_{fireid}.done', ROI_PATH), 
                  fireid=list(fireids['active_fireids']))


def get_sensitivity_fireids(wildcards):
    checkpoints.generate_fire_list.get()
    fireids = get_fireids()
    return expand(get_path('logs/calculate_recovery/done/perfire_recovery_{fireid}.done', ROI_PATH), 
                  fireid=list(fireids['sensitivity_fireids']))


def get_remaining_fireids(wildcards):
    checkpoints.generate_fire_list.get()
    fireids = get_fireids()
    return expand(get_path('logs/calculate_recovery/done/perfire_recovery_{fireid}.done', ROI_PATH), 
                  fireid=list(fireids['remaining_fireids']))


## COORDINATOR FOR FIRE DOWNLOADS
rule coordinate_appeears_requests:
    """
    Main job that organizes steady flow submissions to appeear without overloading apppeears with requests
    runs at low-memory for long time on lab node
    """
    input:
        # need all baselayers in-place before we can submit the perfire recovery qsub task array
        get_path('logs/baselayers/done/all_baselayers_merged.done', ROI_PATH),
        # need config files with params for running download, calculate_recovery scripts
        main_config_json=get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}main_config.json', ROI_PATH),
        perfire_config_json=get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}perfire_config.json', ROI_PATH)
        
    params:
        conda_env='EARTHACCESS',
        email=config['NOTIFY_EMAIL']

    conda: 
        '../workflow/envs/earthaccess_env.yml'

    log: 
        stdout=get_path('logs/calculate_recovery/coordinate_appeears_requests.log', ROI_PATH),
        stderr=get_path('logs/calculate_recovery/coordinate_appeears_requests.err', ROI_PATH)

    output:
        done_flag=get_path('logs/calculate_recovery/done/coordinate_appeears_requests.done', ROI_PATH)

    resources:
        cpus=1,
        runtime=336,
        mem_gb=10,
        pe_flag=',highp'

    shell: 
        """
        workflow/calculate_recovery/sh_scripts/coordinate_appeears_requests.sh \
             {params.conda_env} \
             {input.main_config_json} \
             {input.perfire_config_json} \
             {output.done_flag} > {log.stdout} 2> {log.stderr}
        """

rule potential_ready_flags:
    """
    dummy rule to reassure snakemake that there will be ready_to_download_{fireid}.done flags created.
    these flags will be created by coordinate_appears_requests, but can't be explicit outputs because
    we only want it to run 1 time, and want the perfire rule jobs to release over time, not all at once
    when the coordinator jobs ends
    """
    output:
        get_path('logs/calculate_recovery/done/ready_to_download_{fireid}.done', ROI_PATH)
    run:
        pass



## TRIGGER DOWNLOADS/RECOVERY CALCS
# Batch 1: Run fires with active AppEEARS jobs first
rule active_appeears_fires_done:
    input: get_active_fireids
    
    log: 
        stdout=get_path('logs/calculate_recovery/active_perfire_recovery.log', ROI_PATH),
        stderr=get_path('logs/calculate_recovery/active_perfire_recovery.err', ROI_PATH)

    params:
        email=config['NOTIFY_EMAIL']

    output:
        done_flag=get_path('logs/calculate_recovery/done/active_perfire_recovery.done', ROI_PATH)

    shell: "touch {output.done_flag}  > {log.stdout} 2> {log.stderr}"


# Batch 2: Run sensitivity fires next
rule sensitivity_fires_done:
    input: 
        get_path('logs/calculate_recovery/done/active_perfire_recovery.done', ROI_PATH),  # wait for batch 1
        get_sensitivity_fireids
    
    log: 
        stdout=get_path('logs/calculate_recovery/sensitivity_perfire_recovery.log', ROI_PATH),
        stderr=get_path('logs/calculate_recovery/sensitivity_perfire_recovery.err', ROI_PATH)

    params:
        email=config['NOTIFY_EMAIL']

    output:
        done_flag=get_path('logs/calculate_recovery/done/sensitivity_perfire_recovery.done', ROI_PATH)

    shell: "touch {output.done_flag}  > {log.stdout} 2> {log.stderr}"

# Batch 3: Run all other fires last
rule allfire_recovery:
    input:
        get_path('logs/calculate_recovery/done/sensitivity_perfire_recovery.done', ROI_PATH), # wait for batch 2 
        get_remaining_fireids

    log: 
        stdout=get_path('logs/calculate_recovery/all_perfire_recovery.log', ROI_PATH),
        stderr=get_path('logs/calculate_recovery/all_perfire_recovery.err', ROI_PATH)

    params:
        email=config['NOTIFY_EMAIL']

    output:
        done_flag=get_path('logs/calculate_recovery/done/all_perfire_recovery.done', ROI_PATH)

    shell: "touch {output.done_flag}  > {log.stdout} 2> {log.stderr}"


rule perfire_recovery:
    """
    separate job for each fire. each fire job includes: 
    download LS time series -> process LS time series to seasonal NDVI -> use NDVI and merged baselayers to calculate per-fire recovery time
    """
    input:
        # need all baselayers in-place before we can submit the perfire recovery qsub task array
        get_path('logs/baselayers/done/all_baselayers_merged.done', ROI_PATH),
        # need to know job is ready to download before submitting
        get_path('logs/calculate_recovery/done/ready_to_download_{fireid}.done', ROI_PATH),
        # need config files with params for running download, calculate_recovery scripts
        main_config_json=get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}main_config.json', ROI_PATH),
        perfire_config_json=get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}perfire_config.json', ROI_PATH)
        
    params:
        conda_env_download='EARTHACCESS',
        conda_env_recovery='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    conda: 
        '../workflow/envs/earthaccess_env.yml'

    log: 
        stdout=get_path('logs/calculate_recovery/perfire_recovery_{fireid}.log', ROI_PATH),
        stderr=get_path('logs/calculate_recovery/perfire_recovery_{fireid}.err', ROI_PATH)

    output:
        done_flag=get_path('logs/calculate_recovery/done/perfire_recovery_{fireid}.done', ROI_PATH)

    resources:
        cpus=1,#4,
        runtime=24,
        mem_gb=35

    shell: 
        """
        workflow/calculate_recovery/sh_scripts/calculate_recovery.sh \
             {params.conda_env_download} \
             {params.conda_env_recovery} \
             {input.main_config_json} \
             {input.perfire_config_json} \
             {wildcards.fireid} \
             {output.done_flag}  > {log.stdout} 2> {log.stderr}
        """

# ### MERGE ALL PER-FIRE RECOVERY MAPS ###
# """
# task array: 1 job per 100 fires. last job waits for jobs 1 to n-1 to complete, then does final merge.
# """