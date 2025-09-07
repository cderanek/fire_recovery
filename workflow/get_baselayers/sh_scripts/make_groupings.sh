#!/bin/bash

# INPUTS
CONDA_ENV=$1
ELEV_BAND_M=$2
NLCD_DIR=$3
NLCD_VEGCODES_CSV=$4
MERGED_TOPO=$5
OUT_F=$6
DONE_FLAG=$7

. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate $CONDA_ENV

python workflow/get_baselayers/make_groupings.py \
    "$ELEV_BAND_M" \
    "$NLCD_DIR" \
    "$NLCD_VEGCODES_CSV" \
    "$MERGED_TOPO" \
    "$OUT_F" \
    "$DONE_FLAG"