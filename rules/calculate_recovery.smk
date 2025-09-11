import json

configfile: 'configs/config.yml'
include: 'common.smk'


rule make_recovery_config:
# before running perfire recovery, make json to hold all the recovery settings from the config.yml
    input:
        fireslist_txt: f'{get_path(config['RECOVERY_PARAMS']['RECOVERY_CONFIGS'])}allfilteredfires.txt'

    output:

    params:
        conda_env='RIO_GPD'
        
    conda:

    shell:
        """
        workflow/calculate_recovery/sh_scripts/generate_configs.sh \
             {params.conda_env} \
             '{json.dumps(config)}' \
             {input.fireslist_txt} \
             {output.done_flag}  > {log.stdout} 2> {log.stderr}
        """


rule perfire_recovery:
    """
    separate job for each fire. each fire job includes: 
    download LS time series -> process LS time series to seasonal NDVI -> use NDVI and merged baselayers to calculate per-fire recovery time
    """
    input:
        fireid=expand({fireid}, fireid=FIREIDS),
        # need all baselayers in-place before we can submit the perfire recovery qsub task array
        get_path('logs/baselayers/done/all_baselayers_merged.done')

    params:
        email=config['NOTIFY_EMAIL']

    conda: 
        'workflow/envs/earthaccess_env.yml'

    log: 
        stdout=get_path('logs/get_baselayers/perfire_recovery.log'),
        stderr=get_path('logs/get_baselayers/perfire_recovery.err')

    output:
        filepaths_json=temp(f'{get_path(config['RECOVERY_PARAMS']['RECOVERY_CONFIGS'])}_{fireid}_filepaths.json'),
        fire_metadata_json=temp(f'{get_path(config['RECOVERY_PARAMS']['RECOVERY_CONFIGS'])}_{fireid}_fire_metadata.json'),
        done_flag=get_path(f'logs/baselayers/done/perfire_recovery_{firename}_{fireid}.done')

    shell: 
        """
        workflow/calculate_recovery/sh_scripts/calculate_recovery.sh \
             {params.conda_env_download} \
             {params.conda_env_recovery} \
             {params.config_json} \
             {wildcards.fireid} \
             {output.done_flag}  > {log.stdout} 2> {log.stderr}
        """

### Merge all recovery maps ###
"""
task array: 1 job per 100 fires. last job waits for jobs 1 to n-1 to complete, then does final merge.
"""