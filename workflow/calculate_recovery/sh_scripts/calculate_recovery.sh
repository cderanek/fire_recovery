#!/bin/bash

## TODO: How to update profiles/age/config.yaml to imitate this section of original .sh script?
"""
#$ -t 1-923                       # task IDs
#$ -tc 5                       # maximum concurrent jobs
# error = Merged with joblog
#$ -o joblog.$JOB_ID.$TASK_ID
"""

# INPUTS
CONDA_ENV_DOWNLOAD=$1
CONDA_ENV_RECOVERY=$2
PARAMS_FILE=$3

# Read in params for each fire
TEMP_FILE="temp_params_${SGE_TASK_ID}.txt"
sed -n "${SGE_TASK_ID}p" "$PARAMS_FILE" > $TEMP_FILE
IFS=$' ' read -r _SH_SCRIPT FIRE_SHP_PATH LS_DATA_DIR LS_MONTHLY_DIR LS_SEASONAL_DIR LOG_FILE START_YEAR END_YEAR < $TEMP_FILE
rm $TEMP_FILE

# Activate venv for download
. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV_DOWNLOAD

# Download landsat data for this fire
python workflow/calculate_recovery/get_landsat_seasonal/main_landsat_download.py \
    $FIRE_SHP_PATH \
    $LS_DATA_DIR \
    $LS_MONTHLY_DIR \
    $LS_SEASONAL_DIR \
    $LOG_FILE \
    $START_YEAR \
    $END_YEAR

# Activate venv for recovery calculation
conda deactivate
conda activate $CONDA_ENV_RECOVERY

# Create recovery data for this fire
python workflow/calculate_recovery/calculate_merge_recovery/fire_recovery_main.py \
    $LS_SEASONAL_DIR \
    $FIRE_SHP_PATH \
    $LOG_FILE