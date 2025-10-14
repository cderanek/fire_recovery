import sys, os, glob
import numpy as np
import pandas as pd
import xarray as xr
import rioxarray as rxr
import rasterio as rio
import datetime

# typing
from typing import List, Tuple, Union
NumericType = Union[int, float]
RangeType = Tuple[NumericType, NumericType]

# helper fns
sys.path.append("workflow/utils/") 
from geo_utils import export_to_tiff, reproj_align_rasters, buffer_firepoly
from file_utils import get_prod_doy_tile

#### Scene download organizing helper function ####
def makeDF_uniqueIDs(
    LS_DATA_DIR: str, 
    VALID_LAYERS: list, 
    LS_OUT_DIR: str
    ) -> pd.DataFrame:
    """
    Create a DataFrame with unique IDs for Landsat files.
    
    Params:
    data_dir (str): Directory containing Landsat files
        valid_layers (list): List of valid band/layer names
        out_dir (str): Output directory for processed files
        
    Returns:
    pandas.DataFrame: DataFrame with organized file information
    """
    # Get list of all unique IDs in our LS data directory based on file names
    path_list = glob.glob(LS_DATA_DIR+'/*.tif')
    all_ids_list = [get_prod_doy_tile(p) for p in path_list]
    
    # set up DF of unique IDs
    all_ids_df = pd.DataFrame(all_ids_list, 
                             columns=['uid', 'product', 'doy', 'tile', 'band', 'path'])
    
    # reformat DF and specify out paths
    all_ids_df = all_ids_df[all_ids_df['band'].isin(VALID_LAYERS)]
    all_ids_df['LS_NUM'] = all_ids_df['product'].apply(
        lambda x: int(x[2])
        ) # Product is formatted as L0X.00Y  where X is Landsat number

    all_ids_df['year'] = all_ids_df['doy'].apply( 
        lambda juldate: int(juldate.strip('doy')[0:4])
        ) # doy is formatted as doyYYYYJJJ where JJJ is the julian date
    all_ids_df['month'] = all_ids_df['doy'].apply(
        lambda juldate: datetime.datetime.strptime(juldate.strip('doy'), '%Y%j').date().month
        )
    
    all_ids_df['ndvi_out_path'] = all_ids_df['uid'].apply(
        lambda uid: os.path.join(LS_OUT_DIR, 'daily_ndvi', uid.replace('.tif', '_ndvi_masked.tif'))
        )
    all_ids_df['rgb_out_path'] = all_ids_df['uid'].apply(
        lambda uid: os.path.join(LS_OUT_DIR, 'RGB', uid.replace('.tif', '_rgb.tif'))
        )

    os.makedirs(os.path.join(LS_OUT_DIR, 'RGB'), exist_ok=True)
    os.makedirs(os.path.join(LS_OUT_DIR, 'daily_ndvi'), exist_ok=True)

    all_ids_df.to_csv(f'{LS_DATA_DIR}_summary.csv')
    
    return all_ids_df


#### QA mask functions ####
# decode_bit and qa_mask are from the USGS tutorial: QA_Pixel_Decoding_and_Masking_v3.ipynb
def decode_bit(
    qa_arr: np.ndarray,
    bit: int
    ) -> np.ndarray:
    """
       Decodes the QA Bit
    """
    return (qa_arr & 1 << bit) > 0

def qa_mask(
    qa_arr: np.ndarray, 
    mask_type: str
    ) -> np.ndarray:
    """
    Creates a boolean mask based on the specified mask type.
    Params:
    qa_arr (np.ndarray): The quality assessment array.
    mask_type (str): The type of mask to create. Valid options are:
        "fill", "dilated", "cirrus", "cloud", "shadow", "snow", "clear", "water", 
        the high, mid and low masks refer to confidence levels.

    Returns:
    np.ndarray: The boolean mask with True and False values.
    """
    mask_type = mask_type.lower()  # Convert mask type to lowercase
    
    if mask_type == "fill":
        return decode_bit(qa_arr, 0)
    elif mask_type == "dilated":
        return decode_bit(qa_arr, 1)
    elif mask_type == "cirrus":
        return decode_bit(qa_arr, 2)
    elif mask_type == "cloud":
        return decode_bit(qa_arr, 3)
    elif mask_type == "shadow":
        return decode_bit(qa_arr, 4)
    elif mask_type == "snow":
        return decode_bit(qa_arr, 5)
    elif mask_type == "clear":
        return decode_bit(qa_arr, 6)
    elif mask_type == "water":
        return decode_bit(qa_arr, 7)
    elif mask_type == "high cloud":
        return decode_bit(qa_arr, 8) & decode_bit(qa_arr, 9)
    elif mask_type == "mid cloud":
        return ~decode_bit(qa_arr, 8) & decode_bit(qa_arr, 9)
    elif mask_type == "low cloud":
        return decode_bit(qa_arr, 8) & ~(decode_bit(qa_arr, 9))
    elif mask_type == "high shadow":
        return decode_bit(qa_arr, 10) & decode_bit(qa_arr, 11)
    elif mask_type == "mid shadow":
        return ~decode_bit(qa_arr, 10) & decode_bit(qa_arr, 11)
    elif mask_type == "low shadow":
        return decode_bit(qa_arr, 10) & ~decode_bit(qa_arr, 11)
    elif mask_type == "high snow/ice":
        return decode_bit(qa_arr, 12) & decode_bit(qa_arr, 13)
    elif mask_type == "mid snow/ice":
        return ~decode_bit(qa_arr, 12) & decode_bit(qa_arr, 13)
    elif mask_type == "low snow/ice":
        return decode_bit(qa_arr, 12) & ~decode_bit(qa_arr, 13)
    elif mask_type == "high cirrus":
        return decode_bit(qa_arr, 14) & decode_bit(qa_arr, 15)
    elif mask_type == "mid cirrus":
        return ~decode_bit(qa_arr, 14) & decode_bit(qa_arr, 15)
    elif mask_type == "low cirrus":
        return decode_bit(qa_arr, 14) & ~decode_bit(qa_arr, 15)
    else:
        raise ValueError(f"Invalid mask type: {mask_type}")


def create_masked_landsat(
    ndvi_rxr: xr.DataArray,
    qa_pixel_path: str,
    nodata: NumericType,
    allowable_val_range: RangeType = (0.2,1)
    ) -> xr.DataArray:
    """
    Apply QA masks to an NDVI raster.
    
    Params:
        ndvi_rxr (xarray.DataArray): Raster to mask
        qa_pixel_path (str): Path to QA pixel file
        nodata (int): No-data value
        
    Returns:
        xarray.DataArray: Masked NDVI raster
    """
    # mask invalid vegetation NDVI values (maybe <0.2, >1)
    min_valid_val, max_valid_val = allowable_val_range
    ndvi_mask = np.where((ndvi_rxr.data<min_valid_val) | (ndvi_rxr.data>max_valid_val), True, False)

    # mask according to QA_pixel data
    qa_pixel_rxr = rxr.open_rasterio(qa_pixel_path)
    qa_mask_arr = (ndvi_mask | qa_mask(qa_pixel_rxr.data,'fill') | qa_mask(qa_pixel_rxr.data,'cirrus') | qa_mask(qa_pixel_rxr.data,'cloud') | qa_mask(qa_pixel_rxr.data,'snow') | qa_mask(qa_pixel_rxr.data,'shadow') | qa_mask(qa_pixel_rxr.data,'water')).squeeze()
    
    # reshape to match ndvi_rxr
    qa_mask_broadcasted = np.broadcast_to(qa_mask_arr, ndvi_rxr.data.shape)

    # Apply mask to NDVI
    masked_ndvi_rxr = ndvi_rxr.where(~qa_mask_broadcasted).fillna(nodata)
    masked_ndvi_rxr.rio.set_nodata(nodata, inplace=True)
    return masked_ndvi_rxr


#### Create images for a single date ####
def calc_ndvi_rxr(
    landsat_bands_paths_df: pd.DataFrame, 
    NODATA: float,
    NDVI_BANDS_DICT: dict
    ) -> xr.DataArray:
    """
    Calculate NDVI from Landsat bands, return rxr object with NDVI band for a single date.
    
    Params:
    landsat_bands_paths_df : pandas.DataFrame
        DataFrame containing path and metadata information on Landsat band tifs, filtered to a single date.
        DataFram has the columns:
        | LS_NUM | band | path |
        LS_NUM: specifies Landsat 4-9, important for band selection
        band: which band the path goes to
        path: full path to the Landsat band tif for this date
        
    NODATA : float, optional
        Value to use for no data regions (default from DEFAULT_NODATA)
    
    Returns:
    xarray.DataArray
        Processed NDVI raster
    """
    # Determine Landsat number and corresponding NDVI bands
    LS_num = landsat_bands_paths_df['LS_NUM'].values[0]
    ndvi_bands = NDVI_BANDS_DICT[LS_num]

    # Open NDVI band rasters (each band is stored in a separate tif file)
    NDVI_rxr = [
        rxr.open_rasterio(
            landsat_bands_paths_df[landsat_bands_paths_df['band'] == band]['path'].values[0]
        ).rename({'band': band})
        for band in ndvi_bands
    ]
    
    # Use first NDVI rxr object as template for later calcualtions, attribute extraction
    template_ndvi = NDVI_rxr[0]

    # Extract scale and offset
    scale = template_ndvi.attrs['scale_factor']
    offset = template_ndvi.attrs['add_offset']

    # Calculate NIR and Red band values
    nir = NDVI_rxr[0].values * scale + offset
    red = NDVI_rxr[1].values * scale + offset

    # Calculate NDVI
    ndvi = (nir - red) / (nir + red)

    # Mask invalid values
    ndvi = np.where(
        (nir < 0) | (red < 0) | (ndvi < 0), 
        NODATA, 
        ndvi
    )

    # Prepare final NDVI raster
    NDVI_final = (
        template_ndvi
        .copy(data=ndvi)
        .rename({NDVI_rxr[0].dims[0]: 'NDVI'})
        .isel(NDVI=0)
    )

    # Set raster attributes
    NDVI_final.rio.set_attrs({'scale_factor': 1, 'offset': 0}, inplace=True)
    NDVI_final.rio.set_nodata(NODATA, inplace=True)

    return NDVI_final


def calc_rgb_rxr(
    landsat_bands_paths_df: pd.DataFrame, 
    NODATA: float,
    RGB_BANDS_DICT: dict
    ) -> tuple[xr.DataArray, List[xr.DataArray]]:
    """
    Stack separate Landsat RGB tifs into a single RGB rxr object for a single date.
    
    Params:
    landsat_bands_paths_df : pandas.DataFrame
        DataFrame containing path and metadata information on Landsat band tifs, filtered to a single date.
        DataFram has the columns:
        | LS_NUM | band | path |
        LS_NUM: specifies Landsat 4-9, important for band selection
        band: which band the path goes to
        path: full path to the Landsat band tif for this date
        
    NODATA : float, optional
        Value to use for no data regions (default -9999)
    
    Returns:
    Tuple containing:
    - Stacked RGB xarray DataArray
    - List of individual red, green, blue band DataArrays
    """
    # Determine Landsat number and corresponding RGB bands
    curr_LS_num = landsat_bands_paths_df['LS_NUM'].values[0]
    curr_bands = list(RGB_BANDS_DICT[curr_LS_num])

    # Open RGB band rasters
    rgb_rxr = [
        rxr.open_rasterio(
            landsat_bands_paths_df[landsat_bands_paths_df['band'] == band]['path'].values[0]
        ).rename({'band': band}) 
        for band in curr_bands
    ]

    # Extract scale and offset
    scale = rgb_rxr[0].attrs['scale_factor']
    offset = rgb_rxr[0].attrs['add_offset']

    # Process each band
    processed_bands = []
    for band, band_name in zip(rgb_rxr, curr_bands):
        # Apply scaling and offset
        band.values = band.values * scale + offset
        
        # Mask negative values
        band.values = np.where(band.values < 0, NODATA, band.values)
        
        # Reset attributes
        band.rio.set_attrs({'scale_factor': 1, 'offset': 0}, inplace=True)
        band.rio.set_nodata(NODATA, inplace=True)
        
        # Rename and select first band
        processed_band = band.rename({band_name: 'band'}).isel(band=0)
        processed_bands.append(processed_band)

    # Stack the bands into a single xarray DataArray
    rgb_stack = xr.concat(processed_bands, dim="rgb", coords='minimal')
    
    # Rename the bands
    rgb_stack["rgb"] = ["red", "green", "blue"]
    
    return rgb_stack, processed_bands

#### Merge scenes across dates ####
def process_each_scene_ndvi(
    group: pd.DataFrame, 
    nodata: float, 
    NDVI_BANDS_DICT: dict,
    RGB_BANDS_DICT: dict,
    make_rgb: bool = False, 
    make_daily_ndvi: bool = False
    ) -> List[np.ndarray]:
    """
    For each unique scene listed in the group DF, returns a list of each scene's NDVI. 
    Optionally creates tif file for each scene's RGB, NDVI
    
    Params:
    group : pandas.DataFrame
        DataFrame containing scene information
    nodata : float, optional
        No data value
    make_rgb : bool, optional
        Whether to create RGB image
    make_daily_ndvi : bool, optional
        Whether to export daily NDVI
    
    Returns:
    List of masked NDVI arrays
    """
    allNDVIs = []
    
    for uid in np.unique(group['uid']):
        try:
            # Calculate NDVI
            ndvi = calc_ndvi_rxr(group[group['uid'] == uid], nodata, NDVI_BANDS_DICT)
            
            # Optionally make RGB image
            if make_rgb:
                rgb, _ = calc_rgb_rxr(group[group['uid'] == uid], nodata, RGB_BANDS_DICT)
                rgb_out_path = group[group['uid'] == uid]['rgb_out_path'].values[0]
                export_to_tiff(
                    rgb, 
                    rgb_out_path,
                    dtype_out='float32',
                    nodata=nodata
                )
            
            # Apply QA mask to NDVI
            qa_path = group[
                (group['uid'] == uid) & (group['band'] == 'QA_PIXEL')
            ]['path'].values[0]
            
            masked = create_masked_landsat(ndvi, qa_path, nodata)
            allNDVIs.append(masked)
            
            # Optionally make daily NDVI
            if make_daily_ndvi:
                ndvi_out_path = group[group['uid'] == uid]['ndvi_out_path'].values[0]
                export_to_tiff(masked, ndvi_out_path, dtype_out='float32', nodata=nodata)

        except Exception as e:
            print(f'ERROR: Couldnt calculate NDVI for uid {uid}.\n{e}')
    
    return allNDVIs


def mosaic_export_from_ndvi_list(
    allNDVIs: List[xr.DataArray],
    year: int, 
    season: int, 
    output_dir: str, 
    file_suffix: str,
    nodata: float
    ) -> None:
    """
    Mosaic NDVI into single scene, using median NDVI for each pixel over all provided scenes, and export to GeoTIFF.
    
    Parameters:
    -----------
    allNDVIs : List[xr.DataArray]
        List of NDVI xr data array objects
    year : int
        Year of the scenes
    season : int
        Season of the scenes; 1=JFM=months(1,2, 3), 2=AMJ=months(4,5,6), 3=JAS=months(7,8,9); 4=OND=months(10,11, 12)
    output_dir : str
        Directory to export merged NDVI
    file_suffix: str, optional
        Suffix to add to tif file name
    nodata : float, optional
        No data value
    """
    # Create merged seasonal NDVI using median
    all_ndvis_reproj = reproj_align_rasters('reproj_match', *allNDVIs)    
    
    all_ndvi_arrs = [np.where(arr.data == nodata, np.nan, arr.data) for arr in all_ndvis_reproj] # replace all nodata values with nans so they aren't included in median
    merged_ndvi_data = np.nanmedian(all_ndvi_arrs, axis=0)
    merged_ndvi_data = np.nan_to_num(merged_ndvi_data, nan=nodata)

    # Create new NDVI DataArray with merged data
    original_ndvi = allNDVIs[0] # Use first NDVI raster for metadata
    merged_ndvi = original_ndvi.copy(data=merged_ndvi_data).rio.set_nodata(nodata)
    
    # Export merged seasonal NDVI
    out_merged_seasonal_path = os.path.join(
        output_dir, 
        f"{year}{season:02d}{file_suffix}"
    )
    export_to_tiff(merged_ndvi, out_merged_seasonal_path, dtype_out='float32', nodata=nodata)


def mosaic_ndvi_timeseries(
    LS_DATA_DIR: str, 
    VALID_LAYERS: List[str], 
    LS_OUT_DIR: str,
    NODATA: float, 
    NDVI_BANDS_DICT: dict = {},
    RGB_BANDS_DICT: dict = {},
    MAKE_RGB: bool = False,
    MAKE_DAILY_NDVI: bool = False,
    ) -> None:
    """
    Merge Landsat scenes across dates, creating seasonal NDVI composites.
    Given the path to a directory of LS images, creates seasonal merged and cloud masked images in the specified LS_OUT_DIR directory.
    Optionally deletes all files in the original LS_DATA_DIR
    
    Params:
    LS_DATA_DIR : str
        Input directory with Landsat scenes
    VALID_LAYERS : List[str]
        List of valid layer types
    LS_OUT_DIR : str
        Output directory for processed scenes
    NODATA : float, optional
        No data value
    MAKE_RGB : bool, optional
        Create RGB images for each scene
    MAKE_DAILY_NDVI : bool, optional
        Export daily NDVI images
    """
    # Set suffix for tif files
    file_suffix = '_season_mosaiced.tif'
    
    # Create output directory
    os.makedirs(LS_OUT_DIR, exist_ok=True)

    # Make sure RGB, NDVI dicts are formatted correctly
    NDVI_BANDS_DICT = {int(key): val for key, val in NDVI_BANDS_DICT.items()}
    RGB_BANDS_DICT = {int(key): val for key, val in RGB_BANDS_DICT.items()}
    
    # Organize Landsat files in input directory by unique scene IDs
    all_idsDF = makeDF_uniqueIDs(LS_DATA_DIR, VALID_LAYERS, LS_OUT_DIR)
    print(all_idsDF)
    
    # Set up seasonal info
    # OND=Q4=[10,11, 12]; JFM=Q1=[1,2, 3], AMJ=Q2=[4,5,6], JAS=Q3=[7,8,9]
    all_idsDF['season'] = all_idsDF['month'].apply(lambda x: ((x-1)//3) + 1)
    
    # Process scenes by season and year
    for (year, time_period), group in all_idsDF.groupby(['year', 'season']):
        print(year, time_period)
        print(group)
        # Process daily NDVI scenes, get list of NDVI scenes for this time period
        allNDVIs = process_each_scene_ndvi(
            group, 
            nodata=NODATA,
            NDVI_BANDS_DICT=NDVI_BANDS_DICT,
            RGB_BANDS_DICT=RGB_BANDS_DICT,
            make_rgb=MAKE_RGB, 
            make_daily_ndvi=MAKE_DAILY_NDVI
        )
        
        # If no valid scenes, skip this season and print warning
        if not allNDVIs:
            print(f'WARNING: No valid scenes found for {time_period}/{year} for group: {group}')
            continue
        
        # Merge NDVI into single scene for the season, and export to tif
        mosaic_export_from_ndvi_list(
            allNDVIs,
            year, 
            time_period, 
            LS_OUT_DIR,
            file_suffix,
            NODATA
        )