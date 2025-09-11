#!/bin/bash

# INPUTS
CONDA_ENV=$1
ROI=$2
SUBFIRES_CSV=$3
WUMI_PROJ=$4
WUMI_DIR=$5
MTBS_SEV_DIR=$6
START_YR=$7
END_YR=$8
OUT_DIR=$9
WUMI_SUMMARY_DIR=${10}
ALLFIRES_TXT=${11}
DONE_FLAG=${12}
N_PROCESSES=${13}

. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV

python workflow/get_baselayers/make_mtbs_bundles.py \
    "$ROI" \
    "$SUBFIRES_CSV" \
    "$WUMI_PROJ" \
    "$WUMI_DIR" \
    "$MTBS_SEV_DIR" \
    "$START_YR" \
    "$END_YR" \
    "$OUT_DIR" \
    "$WUMI_SUMMARY_DIR" \
    "$ALLFIRES_TXT" \
    "$DONE_FLAG" \
    "$N_PROCESSES"