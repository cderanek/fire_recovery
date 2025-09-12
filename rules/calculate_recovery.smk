import json, sys

configfile: 'configs/config.yml'
sys.path.append('rules/')
from common import get_path


rule make_recovery_config:
# before running perfire recovery, make json to hold all the recovery settings from the config.yml
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


# rule perfire_recovery:
#     """
#     separate job for each fire. each fire job includes: 
#     download LS time series -> process LS time series to seasonal NDVI -> use NDVI and merged baselayers to calculate per-fire recovery time
#     """
#     input:
#         fireid=expand({fireid}, fireid=FIREIDS),
        # main_config_json=get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}main_config.json', ROI_PATH),
        # perfire_config_json=get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}perfire_config.json', ROI_PATH),
#         # need all baselayers in-place before we can submit the perfire recovery qsub task array
#         get_path('logs/baselayers/done/all_baselayers_merged.done', ROI_PATH)

#     params:
#         email=config['NOTIFY_EMAIL']

#     conda: 
#         'workflow/envs/earthaccess_env.yml'

#     log: 
#         stdout=get_path('logs/get_baselayers/perfire_recovery.log', ROI_PATH),
#         stderr=get_path('logs/get_baselayers/perfire_recovery.err', ROI_PATH)

#     output:
#         filepaths_json=temp(f'{get_path(config['RECOVERY_PARAMS']['RECOVERY_CONFIGS'])}_{fireid}_filepaths.json'),
#         fire_metadata_json=temp(f'{get_path(config['RECOVERY_PARAMS']['RECOVERY_CONFIGS'])}_{fireid}_fire_metadata.json'),
#         done_flag=get_path(f'logs/baselayers/done/perfire_recovery_{firename}_{fireid}.done')

#     shell: 
#         """
#         workflow/calculate_recovery/sh_scripts/calculate_recovery.sh \
#              {params.conda_env_download} \
#              {params.conda_env_recovery} \
#              {params.config_json} \
#              {params.perfire_config_json} \
#              {wildcards.fireid} \
#              {output.done_flag}  > {log.stdout} 2> {log.stderr}
#         """

# ### Merge all recovery maps ###
# """
# task array: 1 job per 100 fires. last job waits for jobs 1 to n-1 to complete, then does final merge.
# """