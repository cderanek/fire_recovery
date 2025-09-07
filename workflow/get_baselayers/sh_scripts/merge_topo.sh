#!/bin/bash

# INPUTS
CONDA_ENV=$1
ELEV_DIR=$2
ASP_DIR=$3
SLOPE_DIR=$4
OUT_F=$5
N_PROCESSES=$6


. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV

python workflow/get_baselayers/merge_topo.py \
    "$ELEV_DIR" \
    "$ASP_DIR" \
    "$SLOPE_DIR" \
    "$MTBS_SEV_DIR" \
    "$OUT_F" \
    "$DONE_FLAG"