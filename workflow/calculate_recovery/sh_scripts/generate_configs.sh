#!/bin/bash

# INPUTS
CONDA_ENV=$1
CONFIG_JSON_IN=$2
MAIN_CONFIG_OUT=$3
PERFIRE_CONFIG_OUT=$4
DONE_FLAG=$5

# Activate venv
. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV

echo $CONDA_ENV

# Generate the file paths json and fire metadata json unique to this fire
python workflow/calculate_recovery/single_fire_recovery/generate_recovery_configs.py \
    $CONFIG_JSON_IN \
    $MAIN_CONFIG_OUT \
    $PERFIRE_CONFIG_OUT \
    $DONE_FLAG