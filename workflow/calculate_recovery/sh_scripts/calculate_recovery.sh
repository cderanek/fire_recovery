#!/bin/bash

# INPUTS
CONDA_ENV_DOWNLOAD=$1
CONDA_ENV_RECOVERY=$2
CONFIG_JSON=$3
PERFIRE_CONFIG_JSON=$4
FIRE_ID=$5
DONE_FLAG=$6


# Activate venv for download
. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV_DOWNLOAD

# Download landsat data for this fire
python workflow/calculate_recovery/get_landsat_seasonal/main_landsat_download.py \
    $CONFIG_JSON \
    $PERFIRE_CONFIG_JSON \
    $FIRE_ID


# Activate venv for recovery calculation
conda deactivate
conda activate $CONDA_ENV_RECOVERY

# Create recovery data for this fire
python workflow/calculate_recovery/single_fire_recovery/main_fire_recovery.py \
    $CONFIG_JSON \
    $PERFIRE_CONFIG_JSON \
    $FIRE_ID \
    $DONE_FLAG