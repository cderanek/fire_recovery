import subprocess, sys, os, glob, gc
import numpy as np
import geopandas as gpd
import xarray as xr
import pyproj

sys.path.append("workflow/utils")
from geo_utils import export_to_tiff, get_crs, get_gdalinfo

def download_landfire(prod_name, prod_link, prod_checksum, download_dir):
    # Download .zip file
    downloaded_zip = download_zip(prod_name, prod_link, prod_checksum, download_dir)

    # Confirm checksum
    confirm_checksum(downloaded_zip, prod_checksum)

    # Unzip, delete original .zip files
    unzip_dir = unzip(prod_name, download_dir, downloaded_zip)
    subprocess.run(['rm', downloaded_zip])

    # Save metadata
    save_metadata(download_dir, metadata_dir)

    return unzip_dir


def clip_landfire(unzip_dir, download_dir, ROI):
    all_tifs = glob.glob(f'{download_dir}**/**/*.tif') + glob.glob(f'{download_dir}**/**/**/*.tif')
    all_crs = [get_crs(tif) for tif in all_tifs]
    crs_uniq = list(set(all_crs))

    bbox_by_crs = {}
    ROI = format_roi(ROI)
    for crs in crs_uniq:
        # Get bbox in target crs
        bbox_by_crs[crs] = calculate_bbox(ROI, crs)

    # Create new dir to hold clipped files
    clip_dir = f'{download_dir}clipped/'
    os.makedirs(clip_dir, exist_ok=True)
    
    # Clip all tifs
    for tif, crs in zip(all_tifs, all_crs):
        clip_tif(clip_dir, tif, *bbox_by_crs[crs])

    # Remove unclipped data
    print(f'Deleting {unzip_dir}', flush=True)
    # subprocess.run(['rm -r', unzip_dir])

    pass


### HELPER FNS ###
def download_zip(prod_name:str, prod_link:str, prod_checksum:str, download_dir:str):
    print(f'About to download {prod_name} at link {prod_link} to {download_dir}', flush=True)
    try:
        os.makedirs(download_dir, exist_ok=True)
        subprocess.run(['wget', '-q', '-P', download_dir, prod_link])
    except subprocess.CalledProcessError as e:
        print(f'Failed to download {prod_name}: {e}', flush=True)
        print(e.stderr, flush=True)

    downloaded_f = glob.glob(f'{download_dir}*.zip')[0]
    print(f'Successfully downloaded file {downloaded_f}', flush=True)
    return downloaded_f
    

def unzip(prod_name:str, download_dir:str, f:str):
    print(f'About to unzip {prod_name} at {f}', flush=True)
    try:
        subprocess.run(['unzip', f, '-d', download_dir])
        print(f.replace('.zip', '/*.zip'))
        subprocess.run(['unzip', f.replace('.zip', '/*.zip'), '-d', f.replace('.zip', '/')])
    except subprocess.CalledProcessError as e:
        print(f'Failed to unzip {prod_name}: {e}', flush=True)
        print(e.stderr, flush=True)

    print(f'Successfully unzipped file {f}', flush=True)
    return f.replace('.zip', '/')


def confirm_checksum(f:str, checksum:str):
    try:
        result = subprocess.run(['md5sum', f], capture_output=True, text=True)
        download_checksum = (result.stdout.split(' ')[0])
    except subprocess.CalledProcessError as e:
        print(f'Error calculating check sum: {e}', flush=True)
        print(e.stderr, flush=True)

    if download_checksum != checksum:
        sys.exit(f'CHECKSUM ERROR FOR {prod_name}.\nDownload checksum {download_checksum} did not match {prod_checksum}.\nPlease delete {os.path.basename(f)} after inspection.')

    print(f'Checksum matches for {prod_name}.\n {download_checksum}={prod_checksum}', flush=True)
    return True


def clip_tif(clip_dir, f, minx, miny, maxx, maxy):
    print(f'Currently clipping {f}')
    gdalinfo = get_gdalinfo(f)
    dtype_orig, nodata_orig = gdalinfo['dtype'], gdalinfo['nodata']

    # Open the file to clip
    dataset = xr.open_dataset(f)
    
    # Get riodataset for all vars and write crs
    crs_info = dataset.variables['spatial_ref'].attrs
    orig_crs = pyproj.CRS.from_cf(crs_info)
    rds = dataset[dataset.rio.vars].squeeze()
    rds.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    rds.rio.write_crs(orig_crs, inplace=True)

    # Clip to bbox
    rds_clipped = rds.rio.clip_box(minx=minx, miny=miny, maxx=maxx, maxy=maxy, crs=orig_crs)

    # Memory management
    del dataset, rds
    gc.collect()

    # Save output
    f_new = clip_dir + f.split('/')[-1].replace('.tif', '_clipped.tif')
    export_to_tiff(
        rds_clipped,
        out_path=f_new,
        dtype_out=dtype_orig,
        nodata=nodata_orig,
        compression='LZW')

    print(f'Saved clipped file to {f_new}.')

    # Memory management
    del rds_clipped
    gc.collect()

    return True


def save_metadata(download_dir:str, metadata_dir:str):
    os.makedirs(metadata_dir, exist_ok=True)
    clip_dir = f'{download_dir}clipped/'
    os.system(f'cp -r {download_dir}**/**/General_Metadata/*.xml {metadata_dir}')
    os.system(f'cp -r {download_dir}**/**/CSV_Data/*.csv {clip_dir}')
    os.system(f'cp -r {download_dir}**/**/Tif/*.tif.* {clip_dir}')
    os.system(f'cp -r {download_dir}**/**/Tif/*.tfw {clip_dir}')

    pass


def format_roi(ROI: str):
    # Get shapefile to later calculate bounding box
    ROI = gpd.read_file(ROI)
    ROI = ROI.explode(index_parts=False)
    ROI['area'] = ROI.explode(index_parts=False).area
    ROI = ROI[ROI['area']==ROI['area'].max()] # Just get the polygon for mainland CA, not the little islands

    return ROI

def calculate_bbox(ROI:gpd.GeoDataFrame, crs:pyproj.CRS):
    ROI_reproj = ROI.to_crs(crs)
    minx, miny, maxx, maxy = tuple(*list(ROI_reproj.bounds.to_records(index=False)))

    return minx, miny, maxx, maxy



if __name__ == '__main__':
    print(f'Running download_clip_landfire.py with arguments {'\n'.join(sys.argv)}\n')
    prod_name = sys.argv[1]
    prod_link = sys.argv[2]
    prod_checksum = sys.argv[3]
    download_dir = sys.argv[4]
    metadata_dir = sys.argv[5]
    ROI = sys.argv[6]

    unzip_dir = download_landfire(prod_name, prod_link, prod_checksum, download_dir)
    # unzip_dir = 'None'
    clip_landfire(unzip_dir, download_dir, ROI)
