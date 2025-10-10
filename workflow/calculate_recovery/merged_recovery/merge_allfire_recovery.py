import os, re, shutil, gc
import rioxarray as rxr
import xarray as xr
import rasterio as rio
import pandas as pd
import numpy as np

# TODO: need to add layer for new recovery metrics, too

def extract_date(fireid):
    # use first 8 digits of fireid to get fire dates
    dir_basename = fireid.split('_')[0]
    match = re.search(r'(\d{8})$', dir_basename)
    if match: return int(match.group(1))
    else: return None
    

def sort_dirs_by_date(dir_list: List[str]) -> List[str]:
    # return directories sorted by date
    return sorted(dir_list, key=extract_date)


def get_ordered_fireUIDs(processing_progress_csv_path):
    ''' 
    Add unique IDs (ranging from 1-N) to processing progress summary CSV
    '''

    # check if UIDs were already made
    summary_csv = pd.read_csv(processing_progress_csv_path)
    
    # if already made
    if 'uid' in summary_csv.columns:
        uid, fire_id = pd.read_csv(temp_csv)[['uid','fireid']]
        uid_fireID_list = list(zip(uid, fire_id))

    # otherwise, make ordered fireids csv
    else:
        # Order fires by date --> assign UID to each fire
        sorted_fires = sort_dirs_by_date(summary_csv['fireid'])
        uid_fireID_list = list(enumerate(fireID_list, start=0)) # create unique integer IDs (1-N) for each fireid
        uid_fireID_df = pd.DataFrame(
            uid_fireID_list,
            columns=['uid', 'fireid']
            )
        
        # create backup of original summary csv
        shutil.copy(summary_csv_path, summary_csv_path.replace('.csv', '_backup.csv'))

        # Add UID column to existing summary csv and then save output
        pd.merge([summary_csv, uid_fireID_df], on='fireid').to_csv(summary_csv_path)
    
    return uid_fireID_list


def create_template_raster(baselayer_template, band_names):
    print(f'Creating template recovery time raster.', flush=True)
    template_raster = xr.open_dataset(baselayer_template, 
                                format="NETCDF4", 
                                engine="netcdf4", 
                                chunks='auto') # open template raster
    
    print(template_raster, flush=True)
    target_crs = template_raster.spatial_ref.crs_wkt
    transform = template_raster.spatial_ref.rio.transform()
    
    # Create layers initialized at nodata val
    num_bands = len(band_names)
    template_data = np.full(
        (len(template_raster.y), len(template_raster.x)),
        nodata_value,
        dtype=np.int8
    )
    out_raster = xr.Dataset(
        {
        band_name: (["y", "x"], template_data) for band_name in band_names
        },
        coords={
            'y': template_raster.y,
            'x': template_raster.x
        }
    )
    
    # Add CRS and transform information, globally and per-band
    out_raster = out_raster.rio.write_crs(target_crs)
    out_raster = out_raster.rio.write_transform(transform)
    # out_raster = out_raster.rio.set_nodata(nodata_value)

    for band_name in band_names:
        out_raster[band_name] = out_raster[band_name].rio.write_crs(target_crs)
        out_raster[band_name] = out_raster[band_name].rio.write_transform(transform)
        out_raster[band_name] = out_raster[band_name].rio.set_nodata(nodata_value)
    
    # Memory management
    del template_raster
    gc.collect()
    
    print(out_raster, flush=True)
    print(f'Finished creating template recovery time raster.', flush=True)

    return out_raster


def calculate_burnbndy_dist(severity_tif):
    # Create burn boundary raster using severity raster
    rasterized_boundary = np.where(severity_tif.data==0, 1, 0)

    # Create distance transform (pixels outside boundary get distance to nearest boundary)
    distance_arr = distance_transform_edt(~rasterized_boundary.astype(bool))
    
    # Convert pixel distances to km units
    pixel_size = abs(severity_tif.rio.transform()[0])  # assuming square pixels
    print(f'pixel size: {pixel_size}')
    distance_km_arr = np.ceil(distance_arr * pixel_size * 10**-2)
    distance_km_arr = np.where(distance_km_arr > 127, 127, distance_km_arr) # if >127*100m away, can't be repr with int8, but set as maxval
    distance_km_tif = severity_tif.copy(data=distance_km_arr)

    del distance_arr, distance_km_arr, rasterized_boundary
    gc.collect()

    return distance_km_tif


def update_recovery_tif_missingdatavals(recovery_tif, future_dist_agdev_mask, temporal_coverage_qa, matched_group_temporal_coverage_qa, severity_tif):
    # For recovery raster, set nodata/bad data/disturbed pixels to -128 and never recovered to 127
    recovery_tif.data[:] = np.where(
        (recovery_tif.data < 0) | (recovery_tif.data > 127), # previously, nodata=-9999
        127,
        recovery_tif.data)

    recovery_tif.data[:] = np.where(
        (future_dist_agdev_mask > 0) | (temporal_coverage_qa > 0) | (matched_group_temporal_coverage_qa > 0) | (severity_tif < 2), 
        -128, 
        recovery_tif.data).astype(np.int8)

    return recovery_tif


def add_fire_out_raster(perfire_config, uid, fireid, num_recovery_tifs, out_raster):
    fire_date = pd.to_datetime(extract_date(fire_dir), format='%Y%m%d')
    fire_yr = int(fire_date.year)

    ## ADD RECOVERY
    # Get recovery tif
    matched_recovery_f = perfire_config[fireid]['FILE_PATHS']['OUT_TIFS_D']['fire_recovery_time'][0].replace('.tif', '_clipped.tif')
    baseline_recovery_f = perfire_config[fireid]['FILE_PATHS']['OUT_TIFS_D']['prefire_baseline_recovery_time'][0].replace('.tif', '_clipped.tif')
    future_dist_tif_f = perfire_config[fireid]['FILE_PATHS']['OUT_TIFS_D']['future_dist_agdev_mask'][0] 
    temp_coverage_qa_tif_f = perfire_config[fireid]['FILE_PATHS']['OUT_TIFS_D']['temporal_coverage_qa'][0] 
    matched_group_qa_tif_f = perfire_config[fireid]['FILE_PATHS']['OUT_TIFS_D']['matched_group_temporal_coverage_qa'][0] 
    severity_tif_f = perfire_config[fireid]['FILE_PATHS']['OUT_TIFS_D']['severity'][0] 
    vegetation_tif_f = perfire_config[fireid]['FILE_PATHS']['OUT_TIFS_D']['groups'][0] 
    vegetation_csv_f = perfire_config[fireid]['FILE_PATHS']['BASELAYERS']['groupings_summary_csv']

    if os.path.exists(matched_recovery_f): 
        print(f'{uid}/{num_recovery_tifs}:\tAdding {os.path.basename(matched_recovery_f)} to full recovery time raster.', flush=True)
    else:
        print(f'No recovery raster found for {fireid}. Skipping.', flush=True) 
        return out_raster
    
    # Open relevant layers
    matched_recovery = rxr.open_rasterio(matched_recovery_f)
    baseline_recovery = rxr.open_rasterio(baseline_recovery_f)
    future_dist_agdev_mask = rxr.open_rasterio(future_dist_tif_f).rio.reproject_match(recovery_tif).squeeze() # update to match extent of template
    temporal_coverage_qa = rxr.open_rasterio(temp_coverage_qa_tif_f).rio.reproject_match(recovery_tif).squeeze()
    matched_group_temporal_coverage_qa = rxr.open_rasterio(matched_group_qa_tif_f).rio.reproject_match(recovery_tif).squeeze()
    severity_tif = rxr.open_rasterio(severity_tif_f).rio.reproject_match(recovery_tif).squeeze()
    vegetation_tif = rxr.open_rasterio(vegetation_tif_f)
    vegetation_csv = pd.read_csv(vegetation_csv_f)[['id','NLCD_NAME']]
    vegetation_tif = create_veg_layer(vegetation_tif, vegetation_csv)

    # For recovery raster, set nodata/bad data/disturbed pixels to -128 and never recovered to 127
    matched_recovery = update_recovery_tif_missingdatavals(
        matched_recovery, 
        future_dist_agdev_mask, 
        temporal_coverage_qa, 
        matched_group_temporal_coverage_qa, 
        severity_tif)

    baseline_recovery = update_recovery_tif_missingdatavals(
        baseline_recovery, 
        future_dist_agdev_mask, 
        temporal_coverage_qa, 
        matched_group_temporal_coverage_qa, 
        severity_tif)

    # Calcualte burn boundary distance
    distance_km_tif = calculate_burnbndy_dist(severity_tif)

    # Memory management
    del future_dist_agdev_mask, temporal_coverage_qa, matched_group_temporal_coverage_qa
    gc.collect()
    
    # Update template raster with this raster's recovery and burn severity, overwriting data from older fires, if necessary
    matched_recovery = matched_recovery.rio.reproject_match(out_raster).data.squeeze() # update to match extent of template
    severity_tif = severity_tif.rio.reproject_match(out_raster).data.squeeze()
    vegetation_tif = vegetation_tif.rio.reproject_match(out_raster).data.squeeze()
    distance_km_tif = distance_km_tif.rio.reproject_match(out_raster).data.squeeze()
    
    out_raster['matched_recovery_time'].data[:] = np.where(matched_recovery > 0, matched_recovery, out_raster['matched_recovery_time'].data).squeeze().astype(np.int8)
    out_raster['matched_recovery_status'].data[:] = np.where(matched_recovery > 0, 1, out_raster['matched_recovery_status'].data).squeeze().astype(np.int8)
    out_raster['matched_recovery_status'].data[:] = np.where(matched_recovery == 127, 0, out_raster['matched_recovery_status'].data).squeeze().astype(np.int8)
    
    out_raster['prefire_baseline_recovery_time'].data[:] = np.where(baseline_recovery > 0, baseline_recovery, out_raster['prefire_baseline_recovery_time'].data).squeeze().astype(np.int8)
    out_raster['prefire_baseline_recovery_time'].data[:] = np.where(baseline_recovery > 0, 1, out_raster['prefire_baseline_recovery_status'].data).squeeze().astype(np.int8)
    out_raster['prefire_baseline_recovery_time'].data[:] = np.where(baseline_recovery == 127, 0, out_raster['prefire_baseline_recovery_status'].data).squeeze().astype(np.int8)
    
    recovery_available_mask = (matched_recovery > 0) | (baseline_recovery > 0)
    out_raster['vegetation_type'].data[:] = np.where(recovery_available_mask, vegetation_tif, out_raster['vegetation_type'].data).squeeze().astype(np.int8)
    out_raster['UID_h'].data[:] = np.where(recovery_available_mask, uid // 100, out_raster['UID_h'].data).squeeze().astype(np.int8)
    out_raster['UID_to'].data[:] = np.where(recovery_available_mask, uid % 100, out_raster['UID_to'].data).squeeze().astype(np.int8)
    out_raster['severity'].data[:] = np.where(recovery_available_mask, severity_tif, out_raster['severity'].data).squeeze().astype(np.int8)
    out_raster['fire_yr'].data[:] = np.where(recovery_available_mask, fire_yr-1982, out_raster['fire_yr'].data).squeeze().astype(np.int8)
    out_raster['burn_bndy_dist_km_upperbound'].data[:] = np.where(recovery_available_mask, distance_km_tif, out_raster['burn_bndy_dist_km_upperbound'].data).squeeze().astype(np.int8)
    
    # Memory management
    del severity_tif, vegetation_tif, distance_km_tif, recovery_tif, matched_recovery, baseline_recovery
    gc.collect()

    return out_raster