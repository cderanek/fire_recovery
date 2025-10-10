#!/bin/bash

# INPUTS
CONDA_ENV=$1
CONFIG_JSON=$2
PERFIRE_CONFIG_JSON=$3
UID_START=$4            # UID of first fire to merge
UID_END=$5              # UID of last fire to merge
MERGE_ALL=$6            # bool
DONE_FLAG=$7 

# Activate venv for download
. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV

# Download landsat data for this fire
python workflow/calculate_recovery/get_landsat_seasonal/main_merge_allfire_recovery.py \
    $CONFIG_JSON \
    $PERFIRE_CONFIG_JSON \
    $UID_START \
    $UID_END \
    $MERGE_ALL \
    $DONE_FLAG