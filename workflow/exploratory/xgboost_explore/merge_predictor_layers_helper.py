import xarray as xr 
import rasterio as rio
from rasterio.enums import Resampling
import numpy as np
import re

def reproj_align_rasters(
    reproj_type:str,
    target_raster: xr.DataArray, 
    *args: xr.DataArray
):
    """
    Reproject (and optionally match extent/resolution) of all input rasters to a target raster.
    
    Args:
        reproj_type: If 'reproj_match', snaps to same grid; otherwise, just reprojects
        target_raster: The target DataArray to align other rasters to
        *args: Variable number of DataArray objects to reproject and align
        
    Returns:
        Tuple of the reprojected and aligned DataArray objects
    """
    try:
        crs_info = target_raster['crs'].attrs
        target_crs = pyproj.CRS.from_cf(crs_info)
    except:
        crs_info = target_raster.spatial_ref.crs_wkt
        target_crs = crs_info
    # Ensure target raster has crs written explicitly
    target_raster.rio.write_crs(target_crs, inplace=True)

    if reproj_type=='reproj_match':
        resampled_rxrL = [resampled_rxr.rio.reproject_match(target_raster,resampling=Resampling.nearest) for resampled_rxr in args]

    if reproj_type=='reproj_match_bilinear':
        resampled_rxrL = [resampled_rxr.rio.reproject_match(target_raster,resampling=Resampling.bilinear) for resampled_rxr in args]

    else:
        resampled_rxrL = [resampled_rxr.rio.reproject(target_crs) for resampled_rxr in args]

    return target_raster, *resampled_rxrL


def categorize_slope_data(slope_data: np.ndarray) -> np.ndarray:
    slope_data = np.where(slope_data==-9999, -128, slope_data)
    slope_data= np.where((slope_data>=0) & (slope_data<3), 0, slope_data) # Flat = 0
    slope_data= np.where((slope_data>=3) & (slope_data<7), 1, slope_data) # Gently sloping = 1
    slope_data= np.where((slope_data>=7) & (slope_data<12), 2, slope_data) # Strongly sloping = 2
    slope_data= np.where((slope_data>=12) & (slope_data< 20), 3, slope_data) # Moderately steep = 3
    slope_data= np.where((slope_data>=20) & (slope_data< 30), 4, slope_data) # Steep = 4
    slope_data= np.where((slope_data>=30) & (slope_data< 40), 5, slope_data) # Very steep = 5
    slope_data= np.where(slope_data >=40, 6, slope_data).astype(np.int8) # Extremely steep = 6

    return slope_data

def categorize_aspect_data(aspect_data: np.ndarray) -> np.ndarray:
    ## -1: flat; 0-45, 315-360: N; 45-135: E; 135-225: S; 225-315: W
    aspect_data = np.where(aspect_data==-9999, -128, aspect_data)
    aspect_data= np.where(((aspect_data>= 0) & (aspect_data< 45)) | (aspect_data>= 315), 1, aspect_data) # N = 1
    aspect_data= np.where((aspect_data>= 45) & (aspect_data< 135), 2, aspect_data) # E = 2
    aspect_data= np.where((aspect_data>= 135) & (aspect_data< 225), 3, aspect_data) # S = 3
    aspect_data= np.where((aspect_data>= 225) & (aspect_data< 315), 4, aspect_data) # W = 4
    aspect_data= np.where(aspect_data== -1, 0, aspect_data).astype(np.int8) # Flat = 0

    return aspect_data

def categorize_elev_data(elev_data: np.ndarray, ELEV_BANDS_M=500) -> np.ndarray:
    elev_data = np.where(elev_data == -9999, -128, np.floor_divide(elev_data, ELEV_BANDS_M)).astype(np.int8)
    return elev_data


def extract_date(dir_name):
    # use last 8 digits of recovery_dir/fire_dir to get fire dates/incid IDs
    match = re.search(r'(\d{8})$', dir_name)
    if match: return int(match.group(1))
    else: return None