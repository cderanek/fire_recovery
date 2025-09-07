#!/bin/bash

# INPUTS
CONDA_ENV=$1
NLCD_DIR=$2
VEGCODES_CSV=$3
MERGED_OUT_PATH=$4
DTYPE_OUT=$5
DONE_FLAG=$6

. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV

python workflow/get_baselayers/make_agdev_mask.py \
    "$NLCD_DIR" \
    "$VEGCODES_CSV" \
    "$MERGED_OUT_PATH" \
    "$DTYPE_OUT" \
    "$DONE_FLAG"

chmod 555 $MERGED_OUT_PATH # make read only