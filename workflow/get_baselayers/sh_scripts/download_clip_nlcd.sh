#!/bin/bash

# INPUTS
CONDA_ENV=$1
DOWNLOAD_LINK=$2
OUT_DIR=$3
METADATA_DIR=$4
START_YEAR=$5
END_YEAR=$6
ROI_FILE=$7
DONE_FLAG=$8

. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV
python workflow/get_baselayers/download_clip_nlcd.py \
    "$DOWNLOAD_LINK" \
    "$OUT_DIR" \
    "$METADATA_DIR" \
    "$START_YEAR" \
    "$END_YEAR" \
    "$ROI_FILE" \
    "$DONE_FLAG"