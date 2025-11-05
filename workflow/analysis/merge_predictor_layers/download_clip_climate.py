import glob, os
from download_climate_data_helpers import *

ROI_SHP_PATH = 'data/ROI/california.shp'
DATA_DIR = 'data/california/climate'
WGET_FILE = 'data/baselayers/manual_downloads/metdata_wget.sh'

if __name__ == '__main__':
    os.makedirs(DATA_DIR, exist_ok=True)
    # Download and clip GRIDMET data for each product/year in our WGET_FILE
    # This function outputs 1 .nc file for each product containing all years data (aggregated to monthly dataset)
    # gridmet_download_clip(WGET_FILE, DATA_DIR, ROI_SHP_PATH)
    
    # # Calculate monthly anomalies based on a range of reference years
    for nc_path in glob.glob(f'{DATA_DIR}/*_clipped_sum.nc'):
        calculate_anomaly(nc_path)
    
    for nc_path in glob.glob(f'{DATA_DIR}/*_clipped_mean.nc'):
        calculate_anomaly(nc_path)

    # calculate_water_yr_avgs(os.path.join(DATA_DIR, f'pr_clipped_sum.nc'))