#!/bin/bash

# INPUTS
CONDA_ENV=$1
DOWNLOAD_LINK=$2
CHECKSUM_FILENAME_REF=$3
CURRENT_YEAR=$4
ROI_FILE=$5
OUT_DIR=$6
DONE_FLAG=$7

. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV

python workflow/get_baselayers/download_rap.py \
    "$DOWNLOAD_LINK" \
    "$CHECKSUM_FILENAME_REF" \
    "$CURRENT_YEAR" \
    "$ROI_FILE" \
    "$OUT_DIR"