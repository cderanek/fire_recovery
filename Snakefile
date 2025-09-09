import os

configfile: 'configs/config.yml'
include: 'rules/common.smk'
include: 'rules/get_baselayers.smk'

# Set paths based on testing mode
TESTING = config['TESTING']
if TESTING:
    ROI_PATH = config['TEST_ROI']
    DATA_PREFIX = os.path.join('data/test_data/output', os.path.splitext(os.path.basename(ROI_PATH))[0])
else:
    ROI_PATH = config['ROI']

BASELAYER_FILES = list(config['BASELAYERS'].keys())

### Define targets for the full workflow (coming soon) ###
# rule all:
#     input:
#         # tbd


### Download baselayers for full ROI ###
# Trigger product downloads for all baselayers

## TODO: Once all final baselayers are confirmed to be completed, set intermediated output files as temp() in the relevant rules

rule get_baselayers:
    input:
        [get_path(config['BASELAYERS'][prod]['fname']) for prod in BASELAYER_FILES],
        get_path("data/baselayers/done/mtbs_bundles.done")

    params:
        email=config['NOTIFY_EMAIL']

    conda: 
        'workflow/envs/get_baselayers_env.yml'

    log: 
        stdout=get_path('logs/get_baselayers.log'),
        stderr=get_path('logs/get_baselayers.err')

    output:
        done_flag=get_path('data/baselayers/done/all_baselayers_merged.done')

    shell: "touch {output.done_flag}  > {log.stdout} 2> {log.stderr}"