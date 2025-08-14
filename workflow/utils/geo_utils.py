import rioxarray as rxr
import xarray as xr
import rasterio as rio
import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
import subprocess
import re
import os
import json
from typing import Union, Tuple

NumericType = Union[int, float]

def clip_raster_to_poly(
    rxr_obj:xr.DataArray,
    poly_path:str
    )->xr.DataArray:
    '''
    Clip xarray object to the bounding box of an input polygon
    Returns clipped version of the data array.
    '''
    # Make sure polygon is in same CRS as rxr_obj
    try:
        crs_info = rxr_obj['crs'].attrs
        target_crs = pyproj.CRS.from_cf(crs_info)
    except:
        crs_info = rxr_obj.spatial_ref.crs_wkt
        target_crs = crs_info
        
    poly_gpd = gpd.read_file(poly_path).to_crs(target_crs)
    
    # Return clipped rxr_obj
    return rxr_obj.rio.clip(poly_gpd.geometry)


def buffer_firepoly(
    fire_shp_path:str, 
    buffer_distance:NumericType=10000
)->Tuple[pd.DataFrame, str]:
    """
    Buffer a fire polygon shapefile by a specified distance and ensure it's not a multipolygon.
    
    Parameters:
        fire_shp_path (str): Path to the fire polygon shapefile
        buffer_distance (int): Buffer distance in meters (default: 10000m)
        
    Returns:
        tuple: (GeoDataFrame of the buffered polygon, path to the new shapefile)
    """
    
    fire_poly = gpd.read_file(fire_shp_path)
    out_f = fire_shp_path.replace('.shp', '_buffered.shp')

    # Project to UTM zone (with units meters) and buffer 10km (10000m)
    fire_poly_utm = fire_poly.to_crs(fire_poly.estimate_utm_crs())
    fire_poly_utm['geometry'] = fire_poly_utm.geometry.buffer(buffer_distance)

    # Reproject the buffered gpd object to the original crs
    fire_poly = fire_poly_utm.to_crs(fire_poly.crs)
    
    # Get the envelope (smallest bbox) of the geometry to get rid of multipolygons
    total_burn_area = np.sum(fire_poly['BurnBndAc'])
    total_geom = fire_poly.union_all()
    
    ## Select only the row with the larged burned area to preserve, then update total burned area to be cumulative
    fire_poly = fire_poly[fire_poly['BurnBndAc']==np.nanmax(fire_poly['BurnBndAc'])]
    fire_poly['BurnBndAc'] = total_burn_area
    fire_poly.geometry = [total_geom]

    # Save to new shapefile
    fire_poly['Ig_Date'] = pd.to_datetime(fire_poly['Ig_Date'], unit='ms').dt.strftime('%Y-%m-%d') # update Ig_Date to be more readable
    fire_poly.to_file(out_f, mode='w')

    return fire_poly, out_f



def reproj_align_rasters(
    reproj_type:str,
    target_raster: xr.DataArray, 
    *args: xr.DataArray
) -> Tuple[xr.DataArray, ...]:
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
        resampled_rxrL = [resampled_rxr.rio.reproject_match(target_raster) for resampled_rxr in args]

    else:
        resampled_rxrL = [resampled_rxr.rio.reproject(target_crs) for resampled_rxr in args]

    return target_raster, *resampled_rxrL


def export_to_tiff(
        rxr_obj:xr.DataArray,
        out_path:str,
        dtype_out:str,
        nodata:NumericType=-9999,
        compression:str='LZW'
)->str:
    '''
    Export an xarray object to a GeoTIFF file.
    
    Parameters:
        rxr_obj (xarray.DataArray): Raster to export
        out_path (str): Output file path
        dtype_out (str): Data type to store raster values, int16 is smallest that allows -9999 nodata val
        nodata (int): No-data value
        
    Returns:
        str: Path to the exported file
        
    dtypes allowed:
        uint8: Unsigned 8-bit integer (0 to 255)
        uint16: Unsigned 16-bit integer (0 to 65,535)
        uint32: Unsigned 32-bit integer (0 to 4,294,967,295)
        int8: Signed 8-bit integer (-128 to 127)
        **int16: Signed 16-bit integer (-32,768 to 32,767)**
            ** Commonly used for INTEGER VALUES, allows for -9999 dtype
        int32: Signed 32-bit integer (-2,147,483,648 to 2,147,483,647)
        **float32: 32-bit floating point (approximately ±3.4*10^38 with 7 significant digits)**
            ** Commonly used for FLOAT VALUES, allows for -9999 dtype
        float64: 64-bit floating point (approximately ±1.8*10^308 with 15 significant digits)
        complex64: Complex number with two 32-bit floats
        complex128: Complex number with two 64-bit floats
    '''
    if isinstance(rxr_obj, xr.Dataset):
        for var in rxr_obj.rio.vars:
            rxr_obj[var].rio.set_nodata(nodata, inplace=True)
    else: rxr_obj.rio.set_nodata(nodata, inplace=True)
    (
        rxr_obj.fillna(nodata)
        .rio.to_raster(
                out_path, 
                driver='GTiff', 
                dtype=dtype_out, 
                nodata=nodata,
                compress=compression)
    )

    print(f'Successfully saved rxr object to {out_path}.', flush=True)
    
    return out_path


def get_crs(
    f:str, 
    crs_type:str='wkt2_2019'
)->pyproj.CRS:
        wkt = subprocess.run(['gdalsrsinfo', f, '-o', crs_type], capture_output=True, text=True).stdout
        return pyproj.CRS(wkt)


def get_gdalinfo(f:str)->dict:
    output = {}
    gdalinfo_json = subprocess.run(['gdalinfo', '-json', f], capture_output=True, text=True).stdout
    gdalinfo_dict = json.loads(gdalinfo_json)

    # For .nc with subdatasets
    if 'SUBDATASETS' in gdalinfo_dict['metadata'].keys():
        # Iterate over all subdatasets
        for metadata_key in gdalinfo_dict['metadata']['SUBDATASETS'].keys():
            if '_NAME' in metadata_key:
                subdataset_name = gdalinfo_dict['metadata']['SUBDATASETS'][metadata_key]
                output[subdataset_name] = get_gdalinfo(subdataset_name)

    else:
        # For tif or subdataset
        # If only 1 band, don't nest bands
        bands = gdalinfo_dict['bands']
        if len(bands) == 1:
            output['dtype'] = bands[0]['type']
            output['nodata'] = bands[0]['noDataValue']
        else:
            band_num = band['band']
            for band in bands:
                try:
                    try:
                        band_name = band['description']
                    except:
                        band_name = band['metadata']['']['NETCDF_VARNAME']
                except:
                    band_name = band_num # just default to band number if we can't find a name

                output[band_num] = {
                    'dtype': bands[0]['type'],
                    'nodata': bands[0]['noDataValue']
                    'name': band_name
                }

    return output

def calculate_bbox(ROI:gpd.GeoDataFrame, crs:pyproj.CRS):
    ROI_reproj = ROI.to_crs(crs)
    minx, miny, maxx, maxy = tuple(*list(ROI_reproj.bounds.to_records(index=False)))

    return minx, miny, maxx, maxy

def format_roi(ROI: str):
    # Get shapefile to later calculate bounding box
    ROI = gpd.read_file(ROI)
    ROI = ROI.explode(index_parts=False)
    ROI['area'] = ROI.explode(index_parts=False).area
    ROI = ROI[ROI['area']==ROI['area'].max()] # Just get the polygon for mainland CA, not the little islands

    return ROI