# fire_recovery

All LANDFIRE download links/checksum were accessed from: https://landfire.gov/data/FullExtentDownloads?field_version_target_id=All&field_theme_target_id=All&field_region_id_target_id=4 


# Notes to self for convenience
. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate RIO_GPD
cd /u/project/eordway/shared/surp_cd/fire_recovery
snakemake --lint


snakemake --dry-run
snakemake -c 1


# TODO:
TESTING:
Implement test to confirm that clipped copy of data has smaller dimensions than unclipped 

LANDFIRE DOWNLOAD:
check metadata gets moved

QSUB:
need to detach and wait long time for job to finish before declaring failure? right now aspect job never submits but ideally all qsubs would happen same time
FIXED -- Log file is outputting to a folder literally called $1
FIXED -- Runtime and memory aren't formatted correctly

CONDA ENV:
Automatic env creation/usage inside of snakefile