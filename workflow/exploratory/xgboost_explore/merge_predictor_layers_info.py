import rioxarray as rxr
import xarray as xr 
import rasterio as rio
from rasterio.enums import Resampling
import numpy as np
import os

TESTING=False
FEATURE_NAMES = [
    'elevation', 'aspect', 'slope', 
    'burn_bndy_dist_km', 'severity', 'count_burned_highsev_300mbuffer',  'count_pixels_unburnedlowsev_matchveg_300mbuffer'
    'vegetation_type', 
    'wateryr_avg_pr_total', 'hot_drought_categories',
    '1yrpre_winter_maxtempz_avg', '3yrpre_winter_maxtempz_avg', '1yrpost_winter_maxtempz_avg', '3yrpost_winter_maxtempz_avg',
    '1yrpre_summer_maxtempz_avg', '3yrpre_summer_maxtempz_avg', '1yrpost_summer_maxtempz_avg', '3yrpost_summer_maxtempz_avg',
    '1yrpre_summer_pdsi_avg', '3yrpre_summer_pdsi_avg', '1yrpost_summer_pdsi_avg', '3yrpost_summer_pdsi_avg',
    '10yr_fire_count', 'avg_annual_fires_since84'
    ]

### OUTPUT FILE PATHS
output_dir = '/u/project/eordway/shared/surp_cd/fire_recovery/data/exploratory_xgboost'
output_predictors_dir = os.path.join(output_dir, 'predictor_layers')
output_model_dir = os.path.join(output_dir, 'model_results')
output_predictions_dir = os.path.join(output_model_dir, 'prediction_layers')

os.makedirs(output_dir, exist_ok=True)
os.makedirs(output_predictors_dir, exist_ok=True)
os.makedirs(output_model_dir, exist_ok=True)
os.makedirs(output_predictions_dir, exist_ok=True)

### INPUT FILE PATHS
sampled_pts_shp_f = '/u/project/eordway/shared/surp_cd/timeseries_data/data/fullCArecovery_reconfig_temporalmasking/sampled_pts.shp'

# historical fire
burnarea_f = '/u/project/eordway/shared/surp_cd/fire_recovery/data/baselayers/manual_downloads/WUMI2024a/fire_maps/burnarea.nc'
burnarea_proj_f = '/u/project/eordway/shared/surp_cd/fire_recovery/data/baselayers/manual_downloads/WUMI2024a/az3135011111020160516_20160502_20160603_dnbr6.tif'

# climate
avg_annual_pr_f = '/u/project/eordway/shared/surp_cd/timeseries_data/data/GRIDMET/pr_clipped_sum_alltimeavg_wateryr_sum.nc'

# merged variables
def open_var(variable, test=TESTING):
    if TESTING:
        path = f'/u/project/eordway/shared/surp_cd/timeseries_data/data/fullCArecovery_shap/testing/merged_{variable}_200_300.tif'
    else: 
        path = f'/u/project/eordway/shared/surp_cd/timeseries_data/data/fullCArecovery_shap/merged_{variable}.tif'
    return rxr.open_rasterio(path).isel(band=0)

# topo
topo_f = '/u/project/eordway/shared/surp_cd/fire_recovery/data/california/baselayers/merged/topo.nc'

### DICTS
veg_type_dict = {
    'Closed tree canopy': 1,
    'Dwarf-shrubland': 2,
    'Herbaceous - grassland': 3,
    'Herbaceous - shrub-steppe': 4,
    'No Dominant Lifeform': 6,
    'Non-vegetated': 7,
    'Open tree canopy': 8,
    'Shrubland': 9,
    'Sparse tree canopy': 10,
    'Sparsely vegetated': 11
}

aspect_dict = {
    'Flat': 0,
    'North': 1,
    'East': 2,
    'South': 3,
    'West': 4
}

slope_dict = {
    'Flat': 0,
    'Gently sloping': 1,
    'Strongly sloping': 2,
    'Moderately steep': 3,
    'Steep': 4,
    'Very steep': 5,
    'Extremely steep': 6
}

severity_dict = {
    'Low': 2,
    'Medium': 3,
    'High': 4
}

hot_drought = {
    'None': 0,
    'Hot winter': 1,
    'Hot summer': 2,
    'Drought': 3,
    'Hot drought': 4    
}
