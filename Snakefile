import os, sys

configfile: 'configs/config.yml'
include: 'rules/get_baselayers.smk'
include: 'rules/calculate_recovery.smk'
sys.path.append('rules/')
from common import get_path

sys.path.append('rules/')
from common import get_path

## STILL TODO: how to use the Snakefile conda: yml
## STILL TODO: Once all final baselayers are confirmed to be completed, set intermediated output files as temp() in the relevant rules
# TODO: Add in more recovery metrics (compare to simple pre-fire baseline, also calculate EVI-based)
# TODO: Make earthaccess downloads env.yam


### Set paths based on testing mode ###
TESTING = config['TESTING']
if TESTING: ROI_PATH = config['TEST_ROI']
else: ROI_PATH = config['ROI']

### Define targets for the full workflow ###
rule all:
    input:
        get_path('logs/baselayers/done/all_baselayers_merged.done', ROI_PATH), # get baselayers
        get_path('logs/calculate_recovery/done/all_perfire_recovery.done', ROI_PATH)    # calculate recovery for all fireids


### Download baselayers for our ROI ###
BASELAYER_FILES = list(config['BASELAYERS'].keys())
rule get_baselayers:
    """
    Triggers product downloads for all baselayers for full ROI.
    Creates ROI-wide baselayers as specified in config['BASELAYERS']:
        agriculture, development mask for all available years
        topography (elevation, aspect, slope)
        annual + cumulative disturbance for all years to process
        annual vegetation+elevation groupings for all years to process, based on specified elevation bands (in m)

    Each output file also has summary txt file with overview of data structure when opened with xarray
    """
    input:
        [get_path(config['BASELAYERS'][prod]['fname'], ROI_PATH) for prod in BASELAYER_FILES],
        get_path("logs/baselayers/done/mtbs_bundles.done", ROI_PATH)

    params:
        email=config['NOTIFY_EMAIL']

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/baselayers/get_baselayers.log', ROI_PATH),
        stderr=get_path('logs/baselayers/get_baselayers.err', ROI_PATH)

    output:
        done_flag=get_path('logs/baselayers/done/all_baselayers_merged.done', ROI_PATH)

    shell: "touch {output.done_flag}  > {log.stdout} 2> {log.stderr}"