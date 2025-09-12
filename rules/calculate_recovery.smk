import json, sys

configfile: 'configs/config.yml'
sys.path.append('rules/')
from common import *


### Set paths based on testing mode ###
TESTING = config['TESTING']
if TESTING: ROI_PATH = config['TEST_ROI']
else: ROI_PATH = config['ROI']

### Get list of fireids for our ROI ###
FIREIDS = get_fireids(get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}wumi_data.csv', ROI_PATH))


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
        fireslist_txt=get_path(f'{config["RECOVERY_PARAMS"]["RECOVERY_CONFIGS"]}allfilteredfires.txt', ROI_PATH),
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
rule allfire_recovery:
    input:
        expand(get_path('logs/calculate_recovery/done/perfire_recovery_{fireid}.done', ROI_PATH), fireid=FIREIDS)

    log: 
        stdout=get_path('logs/calculate_recovery/all_perfire_recovery.log', ROI_PATH),
        stderr=get_path('logs/calculate_recovery/all_perfire_recovery.err', ROI_PATH)

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
        # need config files with params for running download, calculate_recovery scripts
        main_config_json=get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}main_config.json', ROI_PATH),
        perfire_config_json=get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}perfire_config.json', ROI_PATH)
        
    params:
        conda_env_download='EARTHACCESS',
        conda_env_recovery='RIO_GPD',
        email=config['NOTIFY_EMAIL']

    conda: 
        'workflow/envs/earthaccess_env.yml'

    log: 
        stdout=get_path('logs/calculate_recovery/perfire_recovery_{fireid}.log', ROI_PATH),
        stderr=get_path('logs/calculate_recovery/perfire_recovery_{fireid}.err', ROI_PATH)

    output:
        done_flag=get_path('logs/calculate_recovery/done/perfire_recovery_{fireid}.done', ROI_PATH)

    resources:
        cpus=4,
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