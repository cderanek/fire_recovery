#!/bin/bash

# INPUTS
CONDA_ENV=$1
SUBFIRES_CSV=$2
WUMI_PROJ=$3
WUMI_DIR=$4
MTBS_SEV_DIR=$5
START_YR=$6
END_YR=$7
OUT_DIR=$8
DONE_FLAG=$9
N_PROCESSES=${10}

. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV

python workflow/get_baselayers/make_hdist.py \
    "$SUBFIRES_CSV" \
    "$WUMI_PROJ" \
    "$WUMI_DIR" \
    "$MTBS_SEV_DIR" \
    "$START_YR" \
    "$END_YR" \
    "$OUT_DIR" \
    "$DONE_FLAG" \
    "$N_PROCESSES"