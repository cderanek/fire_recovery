#!/bin/bash

# INPUTS
CONDA_ENV=$1
CONFIG_JSON=$2
PERFIRE_CONFIG_JSON=$3
DONE_FLAG=$4


# Activate venv for download
. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV

# Download landsat data for this fire
python workflow/calculate_recovery/get_landsat_seasonal/manage_all_downloads.py \
    $CONFIG_JSON \
    $PERFIRE_CONFIG_JSON

touch $DONE_FLAG