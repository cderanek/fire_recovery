#!/bin/bash

## TODO: How to update profiles/age/config.yaml to imitate this section of original .sh script? 
## this is handled with the wildcards in the snakefile
"""
#$ -t 1-923                       # task IDs
#$ -tc 5                       # maximum concurrent jobs
# error = Merged with joblog
#$ -o joblog.$JOB_ID.$TASK_ID
"""

# INPUTS
CONDA_ENV_DOWNLOAD=$1
CONDA_ENV_RECOVERY=$2
CONFIG_JSON=$3
FIRE_ID=$4 


# Activate venv for download
. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV_DOWNLOAD

# Download landsat data for this fire
python workflow/calculate_recovery/get_landsat_seasonal/main_landsat_download.py \
    $CONFIG_JSON \
    $FIRE_ID

# Activate venv for recovery calculation
conda deactivate
conda activate $CONDA_ENV_RECOVERY

# Create recovery data for this fire
python workflow/calculate_recovery/calculate_merge_recovery/fire_recovery_main.py \
    $CONFIG_JSON \
    $FIRE_ID