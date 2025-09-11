import os

configfile: 'configs/config.yml'
include: 'rules/common.smk'
include: 'rules/get_baselayers.smk'

## STILL TODO: how to use the Snakefile conda: yml
## STILL TODO: Once all final baselayers are confirmed to be completed, set intermediated output files as temp() in the relevant rules
# TODO: Add in more recovery metrics (compare to simple pre-fire baseline, also calculate EVI-based)
# TODO: Make earthaccess downloads env.yam


### Set paths based on testing mode ###
TESTING = config['TESTING']
if TESTING:
    ROI_PATH = config['TEST_ROI']
else:
    ROI_PATH = config['ROI']
DATA_PREFIX = os.path.splitext(os.path.basename(ROI_PATH))[0]

### Define targets for the full workflow ###
rule all:
    input:
        get_path('logs/baselayers/done/all_baselayers_merged.done') # get baselayers
        # get_path(ls_done_flag TBD) # get Landsat data


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
        [get_path(config['BASELAYERS'][prod]['fname']) for prod in BASELAYER_FILES],
        get_path("logs/baselayers/done/mtbs_bundles.done")

    params:
        email=config['NOTIFY_EMAIL']

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/get_baselayers/get_baselayers.log'),
        stderr=get_path('logs/get_baselayers/get_baselayers.err')

    output:
        done_flag=get_path('logs/baselayers/done/all_baselayers_merged.done')

    shell: "touch {output.done_flag}  > {log.stdout} 2> {log.stderr}"


# ### Make recovery maps ###
# rule calculate_recovery:
# looks for .done flag from the merged_recovery rule