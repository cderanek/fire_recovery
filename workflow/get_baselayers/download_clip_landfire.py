import subprocess, sys, os, glob, gc
import zipfile
import numpy as np
import geopandas as gpd
import xarray as xr
import pyproj

sys.path.append("workflow/utils")
from geo_utils import get_crs, calculate_bbox, format_roi, clip_tif
from file_utils import confirm_checksum

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
    print(f'Unzip dir: {unzip_dir}\nDownload dir: {download_dir}')
    unzip_tif_dirs = [os.path.join(unzip_dir, 'Tif'), os.path.join(download_dir, '**', '**', 'Tif')]
    print(unzip_tif_dirs)
    all_tifs = []
    for unzip_tif_dir in unzip_tif_dirs:
        all_tifs += glob.glob(f'{unzip_tif_dir}/*.tif')
    print(all_tifs)
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
    subprocess.run(['rm', '-r', unzip_dir])

    pass


### HELPER FNS ###
def download_zip(prod_name:str, prod_link:str, prod_checksum:str, download_dir:str):
    print(f'About to download {prod_name} at link {prod_link} to {download_dir}', flush=True)
    try:
        os.makedirs(download_dir, exist_ok=True)
        subprocess.run(['wget', '-q', '-P', download_dir, prod_link])
        print(f'Downloaded {prod_link} to {download_dir}.')
    except subprocess.CalledProcessError as e:
        print(f'Failed to download {prod_name}: {e}', flush=True)
        print(e.stderr, flush=True)

    if '/' != download_dir[-1]: download_dir += '/'
    downloaded_f = glob.glob(f'{download_dir}{prod_link.split('/')[-1]}')[0]
    print(f'Successfully downloaded file {downloaded_f}', flush=True)
    return downloaded_f
    

def unzip(prod_name:str, download_dir:str, f:str):
    print(f'About to unzip {prod_name} at {f}', flush=True)
    try:
        subprocess.run(['unzip', f, '-d', download_dir])

        # Get name of unzipped dir to unzip 1 level down
        with zipfile.ZipFile(f, 'r') as zip_ref:
            new_dir = zip_ref.namelist()[0].split('/')[0]
        new_path = os.path.join(download_dir, new_dir)
        subprocess.run(['unzip', f'{new_path}/*.zip', '-d', new_path])
    
    except subprocess.CalledProcessError as e:
        print(f'Failed to unzip {prod_name}: {e}', flush=True)
        print(e.stderr, flush=True)

    print(f'Successfully unzipped file {f}', flush=True)
    return new_path


def save_metadata(download_dir:str, metadata_dir:str):
    os.makedirs(metadata_dir, exist_ok=True)
    clip_dir = f'{download_dir}clipped/'
    os.makedirs(clip_dir, exist_ok=True)

    def find_dir(base_dir, folder_name):
        matches = glob.glob(f'{base_dir}**/{folder_name}/', recursive=True)
        return matches if matches else None

    # Find metadata, csv, tif directories
    metadata_source_l = find_dir(download_dir, 'General_Metadata')
    csv_source_l = find_dir(download_dir, 'CSV_Data') 
    tif_source_l = find_dir(download_dir, 'Tif')

    if metadata_source_l:
        for metadata_source in metadata_source_l:
            print(f'Metadata source: {metadata_source}')
            os.system(f'cp {metadata_source}*.xml {metadata_dir}')
    else: print(f'No .xml metadata found in {download_dir}')

    if csv_source_l:
        for csv_source in csv_source_l:
            print(f'csv source: {csv_source}')
            os.system(f'cp {csv_source}*.csv {metadata_dir}')
    else: print(f'No .csv metadata found in {download_dir}')

    if tif_source_l:
        for tif_source in tif_source_l:
            print(f'tif source: {tif_source}')
            os.system(f'cp {tif_source}*.tif.* {clip_dir}')
            os.system(f'cp {tif_source}*.tfw {clip_dir}')
    else: print(f'No tif data found in {download_dir}')

    pass


if __name__ == '__main__':
    print(f'Running download_clip_landfire.py with arguments {'\n'.join(sys.argv)}\n')
    prod_name = sys.argv[1]
    prod_link = sys.argv[2]
    prod_checksum = sys.argv[3]
    download_dir = sys.argv[4]
    metadata_dir = sys.argv[5]
    ROI = sys.argv[6]
    done_flag = sys.argv[7]

    unzip_dir = download_landfire(prod_name, prod_link, prod_checksum, download_dir)
    clip_landfire(unzip_dir, download_dir, ROI)
    subprocess.run(['touch', done_flag])