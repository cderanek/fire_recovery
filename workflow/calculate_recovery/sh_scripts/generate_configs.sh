#!/bin/bash# INPUTS


CONDA_ENV=$1
CONFIG_JSON_IN=$2
FIRESLIST_TXT=$3
DONE_FLAG=$4

# Activate venv
. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV

# Generate the file paths json and fire metadata json unique to this fire
python workflow/calculate_recovery/calculate_merge_recovery/generate_recovery_configs.py \
    $CONFIG_JSON_IN \
    $FIRESLIST_TXT \
    $DONE_FLAG