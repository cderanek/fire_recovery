import rioxarray as rxr
import pandas as pd
import numpy as np
import geopandas as gpd
import os, gc
import rasterio
from rasterio import sample
from shapely.geometry import Point
import tempfile

from merge_predictor_layers_info import *
from merge_predictor_layers_helper import *


#### MERGE/ALIGN EXISTING ####

## BASE
# 'recovery_time_yrs', 'recovery_status'
# 'elevation', 'aspect', 'slope', 'vegetation_type', 
# 'burn_bndy_dist_km', 'severity', 
recovery_time = open_var('recovery_time')
recovery_status = open_var('recovery_status')
topo = rxr.open_rasterio(topo_f)
fire_yr = open_var('fire_yr')
fire_yr.data[:] = 1982+fire_yr.data
uid = open_var('UID_h').astype('int16')
uid_to = open_var('UID_to')
uid.data[:] = (100*uid.data) + uid_to.data
del uid_to
gc.collect()

recovery_time, topo = reproj_align_rasters('reproj_match', recovery_time, topo)
# Extract base data
print('BASE DATA')
# for unrecovered pixels, recovery time is time at the last date observed unrecovered
# convert recovery time from seasons to years 
recovery_time_data = np.where(
    recovery_status.data.squeeze()==0, 
    2024 - (fire_yr.data.squeeze()), 
    np.round(recovery_time.data.squeeze()/4)).squeeze() # FOR NOW rough calculate of last date based on merged_fire_yr; in future, use exact igdate
recovery_time_data = np.where(recovery_time.data.squeeze()<0, -128, recovery_time_data).squeeze().astype('int8')
del fire_yr
gc.collect()

elev_data = topo.sel(band='Elev').data.astype('int16')
aspect_data = topo.sel(band='Asp').data.astype('int16')
slope_data = topo.sel(band='SlpD').data.astype('int16')
del topo
gc.collect()

elev_data = categorize_elev_data(elev_data).astype('int16')
aspect_data = categorize_aspect_data(aspect_data).astype('int16')
slope_data = categorize_slope_data(slope_data).astype('int16')


## HISTORICAL CLIMATE
print('HISTORICAL CLIMATE')
# 'wateryr_avg_pr_total' (reproj with bilinear interpolate)
hist_precip = xr.open_dataset(avg_annual_pr_f, format="NETCDF4", engine="netcdf4", chunks='auto')
hist_precip = hist_precip.rio.write_crs(hist_precip['spatial_ref'].crs_wkt)
hist_precip = hist_precip.to_dataarray(dim='precipitation_amount_sum_alltimeavg_wateryr_sum').rename({'precipitation_amount_sum_alltimeavg_wateryr_sum': 'band'})
recovery_time, hist_precip = reproj_align_rasters('reproj_match_bilinear', recovery_time, hist_precip)
print(hist_precip.shape)

## DISTURBANCE HISTORY
# print('DISTURBANCE HISTORY')
# # '10yr_fire_count', 'avg_annual_fires_since84'
# if not os.path.exists(os.path.join(output_predictors_dir,'average_annual_fires_since1984.tif')):
#     burnarea_proj = rxr.open_rasterio(burnarea_proj_f).spatial_ref.crs_wkt
#     burnarea = rxr.open_rasterio(burnarea_f).rio.write_crs(burnarea_proj)
#     num_years = len(np.unique(burnarea.time.dt.year))
#     valid_burn = (burnarea > 0) & (~np.isnan(burnarea))
#     burn_years_total = valid_burn.groupby('time.year').any(dim='time').sum(dim='year')
#     print(f'WUMI years represented: {np.unique(burnarea.time.dt.year)}')
#     avg_annual_fires_since84 = num_years / burn_years_total
#     recovery_time, avg_annual_fires_since84 = reproj_align_rasters('reproj_match', recovery_time, avg_annual_fires_since84)
#     avg_annual_fires_since84.rio.to_raster(os.path.join(output_predictors_dir,'average_annual_fires_since1984.tif'))
# else:
#     avg_annual_fires_since84 = rxr.open_rasterio(os.path.join(output_predictors_dir,'average_annual_fires_since1984.tif'))
#     avg_annual_fires_since84.rio.write_crs(avg_annual_fires_since84.spatial_ref.crs_wkt, inplace=True)
#     avg_annual_fires_since84 = avg_annual_fires_since84.isel(band=0).rio.reproject_match(recovery_time)
# print(avg_annual_fires_since84.shape)


#### MERGE ALL ####
print('MERGE')
# Save predictors to an nc file for input to xgboost, shap

predictor_vars = {
    var_name: (recovery_time.dims, open_var(var_name).data.squeeze())
    for var_name in FEATURE_NAMES_MERGED
}
predictor_vars['uid'] = (recovery_time.dims, uid.data.squeeze())
predictor_vars['elevation'] = (recovery_time.dims, elev_data.squeeze())
predictor_vars['aspect'] = (recovery_time.dims, aspect_data.squeeze())
predictor_vars['slope'] = (recovery_time.dims, slope_data.squeeze())
predictor_vars['wateryr_avg_pr_total'] = (recovery_time.dims, np.round(hist_precip.data).squeeze())

predictors_ds = (
    xr.Dataset(predictor_vars)
        .rio.write_crs(recovery_time.spatial_ref.crs_wkt)
        .rio.set_spatial_dims(x_dim='x', y_dim='y', inplace=True)
    )
predictors_ds['spatial_ref'] = recovery_time.spatial_ref

del predictor_vars
gc.collect()

for var_name in predictors_ds.data_vars:
    predictors_ds[var_name].rio.write_crs(recovery_time.spatial_ref.crs_wkt, inplace=True)

print(predictors_ds)

# Save recovery time, status to nc file for input to xgboost, shap
output_ds = (xr.Dataset({
        'recovery_time_yrs': (recovery_time.dims, recovery_time_data),
        'recovery_status_sampled': (recovery_time.dims, recovery_status.data.squeeze()),  # use the same dimensions as recovery_time
    })
    .rio.write_crs(recovery_time.spatial_ref.crs_wkt)
    .rio.set_spatial_dims(x_dim='x', y_dim='y', inplace=True)
    )
output_ds['spatial_ref'] = recovery_time.spatial_ref

for var_name in output_ds.data_vars:
    output_ds[var_name].rio.write_crs(recovery_time.spatial_ref.crs_wkt, inplace=True)

output_ds.rio.to_raster(os.path.join(output_predictions_dir,'merged_predictions.tif'))

#### SAMPLED POINTS --> CSV ####
print('SAMPLE POINTS')
gdf = gpd.read_file(sampled_pts_shp_f).to_crs(recovery_time.spatial_ref.crs_wkt)
sampled_coords = [(pt.x, pt.y) for pt in gdf.geometry]
xs, ys = zip(*sampled_coords)  # Unpack coordinates
sampled_df = {'point_ID': gdf.point_ID}

for var_name in predictors_ds.data_vars:
    print(f'Addding {var_name}')
    # Write DataArray to temporary GeoTIFF
    with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmp:
        predictors_ds[var_name].rio.to_raster(tmp.name)
        
        # Now use rasterio to sample
        with rasterio.open(tmp.name) as src:
            sampled_vals = list(sample.sample_gen(src, sampled_coords))
    
    sampled_df[var_name] = [d[0] for d in sampled_vals]

# Save predictors to tif (if memory allows)
try: 
    predictors_ds = predictors_ds.chunk(
        {"y": 2048, "x": 2048}
    )

    predictors_ds.rio.to_raster(
        os.path.join(output_predictors_dir,'merged_predictors_new.tif'),
        tiled=True,
        windowed=True,
        lock=False,
    )
except Exception as e:
    print(f'Couldnt save full predictors to tif: {e}', flush=True)

try: 
    august_geometry = gpd.read_file('/u/project/eordway/shared/surp_cd/timeseries_data/data/fullCArecovery_reconfig_temporalmasking/AUGUSTCOMPLEX_2020_CA3966012280920200817/ca3966012280920200817_20200715_20210718_burn_bndy.shp').to_crs(recovery_time.spatial_ref.crs_wkt).geometry
    predictors_ds_clipped = predictors_ds.rio.clip(august_geometry)

    predictors_ds_clipped.rio.to_raster(
        os.path.join(output_predictors_dir,'merged_predictors_clipped.tif'),
        tiled=True,
        windowed=True,
        lock=False,
    )
except Exception as e:
    print(f'Couldnt save clipped predictors to tif: {e}', flush=True)

del predictors_ds
gc.collect()

for var_name in output_ds.data_vars:
    print(f'Addding {var_name}')
    # Write da to temp tiff (so we can read as rasterio object)
    with tempfile.NamedTemporaryFile(suffix='.tif', delete=False) as tmp:
        output_ds[var_name].rio.to_raster(tmp.name)
        
        # Use rio to sample 
        with rasterio.open(tmp.name) as src:
            sampled_vals = list(sample.sample_gen(src, sampled_coords))
    
    sampled_df[var_name] = [d[0] for d in sampled_vals]
    print(f'\tAddded {var_name}')
sampled_df = pd.DataFrame.from_dict(sampled_df)
sampled_gdf = gpd.GeoDataFrame(sampled_df, geometry=gdf.geometry, crs=recovery_time.spatial_ref.crs_wkt)
sampled_gdf.to_file(os.path.join(output_dir, 'sampled_points_min_predictors_outcomes.gpkg'))