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
# to run snakemake and have it wait 24hrs before failing anything, submit up to 50 concurrent jobs, keep going if one part of the workflow fails
snakemake -c 1 \
--cluster-config cluster.yaml \
--cluster "qsub -cwd \
             -o {log} \
             -j y \
             -l h_rt={resources.runtime}:00:00,h_data={resources.mem_gb}G \
             -M {cluster.email} \
             -m bea \" \
--latency-wait 86400 \
--max-jobs-per-second 10 \
--jobs 50 \
--keep-going \
--rerun-incomplete \
--groups rap_download_group=group0 \
--group-components group0=1 \

## POSSIBLY ADD
--immediate-submit \
--notemp \

# TODO:
later -- consider cleanup of subprocess.run, to be more robust to other operating systems


BASELAYERS MERGE:
TOPO: **

CLIM: wait until after submit landfire download job

VEG: Waiting to decide RAP or MODIS or LANDFIRE EVT?

HDIST: TESTING NOW
. /u/local/Modules/default/init/modules.sh
module load anaconda3
conda activate RIO_GPD
cd /u/project/eordway/shared/surp_cd/fire_recovery
python workflow/get_baselayers/make_hdist.py \
    "/u/project/eordway/shared/surp_cd/fire_recovery/data/baselayers/temp/US_DIST/" \
    "data/baselayers/annual_dist_withagrdevmask.nc" \
    "0" \
    "int8" \
    "x" \
    "y" \
    "time" \
    "1999" \
    "2024" \
    "data/baselayers/hdist_merged.done"



ON PAUSE UNTIL WE TALK ABOUT MODIS: EVT <-> RAP RF MODEL:
Sample points from old merged EVT, sample points from new RAP @ 2016, 2020, 2023 -> run basic RF model to predict (clump dwarf shurubland in with regular shurbland, drop all 5, 11, 12, 13)


CONDA ENV:
Automatic env creation/usage inside of snakefile


TESTING: ** WRITE TEST CASES FOR SAMPLE POINTS NEXT
for all: 
create tiny ROI for tiny test cases that can be viewed on qgis easily too (projection comes in nicely), to make sure metadata show up nicely
print gdalinfo and review attrs make sense --> updated get_gdalinfo in geo_utils

landfire: ** TODO NEXT
confirm that clipped copy of data has smaller dimensions than unclipped
create shp of specific test values at different corners of CA (create geojson of points to extract from website, then convert to shpfile on QGIS?)
confirm that dtype, nodatavals are correct and that crs exist

mtbs: ** TODO NEXT
confirm # of unique IDs per year matches # of boundaries
confirm all fires have burn severity tif

rap: ON PAUSE -- MAY USE MODIS
create shp of specific test values at different corners of CA, same as landfire

gridmet:
add gridmet rule
create merged gridmet layer


# To run the main Snakefile
(1) Update the configs:
    * Add your own email
    * Change the ROI to your ROI
(2) Make sure tmux or screen is installed on your cluster. The main Snakefile will run for weeks (depending on the size/number of fires you're analyzing). I used tmux, so here are some relevant notes here.
    * tmux allows you to create windows that you can detach/reattach to. So, you can ssh into your cluster, submit the Snakefile, and then log out without stopping the Snakefile. The main commands to know are:
        * See your windows: tmux ls
        * Create a window:  prefix: ‘ctrl+b’ then ‘c’; to rename 'ctrl+b' then ','
        * Detach from a window: ‘ctrl+b’ then ‘d’
        * Attach to a window: tmux attach -t <window_name>
        * Delete a window (while you're inside of it): type 'exit' and press enter -- this will delete the window
(3) Create the conda environments you'll need 
    * STILL TODO
(4) Run the main workflow, putting in relevant values for the runtime, number of jobs to submit per second, and max concurrent jobs for your scheduler to handle:
    * snakemake -c 1 --latency-wait <runtime_in_secs> --max-jobs-per-second <n> --jobs <max_concurrent_jobs> --keep-going


# Data used
RAP: Allred, B.W., B.T. Bestelmeyer, C.S. Boyd, C. Brown, K.W. Davies, M.C. Duniway,
L.M. Ellsworth, T.A. Erickson, S.D. Fuhlendorf, T.V. Griffiths, V. Jansen, M.O.
Jones, J. Karl, A. Knight, J.D. Maestas, J.J. Maynard, S.E. McCord, D.E. Naugle,
H.D. Starns, D. Twidwell, and D.R. Uden. 2021. Improving Landsat predictions of
rangeland fractional cover with multitask learning and uncertainty. Methods in
ecology and evolution. http://dx.doi.org/10.1111/2041-210x.13564