#!/bin/bash

# INPUTS
CONDA_ENV=$1
EVT_DIR=$2
MERGED_OUT_PATH=$3
DTYPE_OUT=$4
DONE_FLAG=$5

. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV

python workflow/get_baselayers/make_agdev_mask.py \
    "$EVT_DIR" \
    "$MERGED_OUT_PATH" \
    "$DTYPE_OUT" \
    "$DONE_FLAG"

chmod 555 $MERGED_OUT_PATH # make read only