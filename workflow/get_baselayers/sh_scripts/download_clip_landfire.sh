#!/bin/bash

# INPUTS
CONDA_ENV=$1
PRODUCT_NAME=$2
DOWNLOAD_LINK=$3
CHECKSUM=$4
DIR_NAME=$5
METADATA_DIR=$6
ROI_FILE=$7
DONE_FLAG=$8

. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV
python workflow/get_baselayers/download_clip_landfire.py \
    "$PRODUCT_NAME" \
    "$DOWNLOAD_LINK" \
    "$CHECKSUM" \
    "$DIR_NAME" \
    "$METADATA_DIR" \
    "$ROI_FILE"

touch "$DONE_FLAG"