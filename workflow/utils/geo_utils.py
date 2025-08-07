import rioxarray as rxr
import xarray as xr
import rasterio as rio
import geopandas as gpd
import numpy as np
import pandas as pd
import pyproj
from typing import Union, Tuple

NumericType = Union[int, float]


def export_to_tiff(
        rxr_obj:xr.DataArray,
        out_path:str,
        dtype_out:str,
        nodata:NumericType=-9999
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
    (
        rxr_obj.fillna(NODATA)
        .rio.set_nodata(NODATA, inplace=True)
        .rio.to_raster(
                out_path, 
                driver='GTiff', 
                dtype=dtype_out, 
                nodata=NODATA)
    )

    print('Successfully saved {rxr_obj.name} to {out_path}.', flush=True)
    
    return out_path



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
)->Tuple[pd.GeoDataFrame, str]:
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

    # # Visually check that rasters align
    # target_shape = target_raster.data.shape
    # for resampled_rxr in resampled_rxrL:
    #     # Visually check that rasters align
    #     print('AFTER REPROJECTING:')
    #     print('Shapes:\t', target_shape, resampled_rxr.data.shape)
    #     try: print('CRS:\t', target_crs, resampled_rxr['spatial_ref'].attrs['crs_wkt'])
    #     except: pass
    #     try: print('GeoTr:\t', list(zip(target_raster['spatial_ref'].attrs['GeoTransform'].split(' '), resampled_rxr['spatial_ref'].attrs['GeoTransform'].split(' '))))
    #     except: pass
    #     print()
        
    return target_raster, *resampled_rxrL