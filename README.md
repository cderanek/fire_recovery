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
snakemake -c 1 --latency-wait 86400 --max-jobs-per-second 1 --jobs 50 --keep-going

# TODO:
RAP download:
convert to int8
compare to current EVT classes (pull 1 year that matche EVT year -> for each EVT class, show distribution of % cover for each RAP type)

/vsicurl/http://rangeland.ntsg.umt.edu/data/rap/rap-vegetation-cover/v3/ data/baselayers/downloadlogs_metadata/RAP/rap_checksum_filenames.csv 2020 data/ROI/california.shp data/baselayers/temp/RAP/ logs/get_rap_2020.log



TESTING:
Landfire:
confirm that clipped copy of data has smaller dimensions than unclipped
create shp of specific test values at different corners of CA (create geojson of points to extract from website, then convert to shpfile on QGIS?)

MTBS:
confirm # of unique IDs per year matches # of boundaries
confirm all fires have burn severity tif

LANDFIRE DOWNLOAD:
Fix metadata that didn't get moved
Fix .done files not made 

CONDA ENV:
Automatic env creation/usage inside of snakefile

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