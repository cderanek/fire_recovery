#!/bin/bash

# INPUTS
CONDA_ENV=$1
CONFIG_JSON=$2
UID_START=$3            # UID of first fire to merge
UID_END=$4              # UID of last fire to merge
MERGE_ALL=$5            # bool
TOTAL_FIRES_COUNT = $6  # count of total fires to aggregate
DONE_FLAG=$7 

# Activate venv for download
. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV

# Download landsat data for this fire
python workflow/calculate_recovery/get_landsat_seasonal/main_landsat_download.py \
    $CONFIG_JSON \
    $PERFIRE_CONFIG_JSON \
    $FIRE_ID