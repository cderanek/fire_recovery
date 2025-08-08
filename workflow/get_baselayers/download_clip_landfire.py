import subprocess, sys, os, glob, gc
import geopandas as gpd
import xarray as xr


def download_landfire(prod_name, prod_link, prod_checksum, download_dir):
    # Download .zip file
    downloaded_zip = download_zip()

    # Confirm checksum
    confirm_checksum(downloaded_zip)

    # Unzip, delete original .zip files
    unzip(prod_name, downloaded_zip)
    subprocess.run(['rm', downloaded_zip])

    # Save metadata
    save_metadata(download_dir, metadata_dir)

    return True


def clip_landfire(download_dir, ROI):
    all_tifs = glob.glob(download_dir, '*.tif')
    matching, target_crs = confirm_matching_crs(all_tifs)
    
    # Get bbox in target crs
    ROI = format_roi(ROI)
    minx, miny, maxx, maxy = calculate_bbox(ROI, target_crs)

    # Create new dir to hold clipped files
    clip_dir = f'{download_dir}clipped/'
    os.makedirs(clip_dir, exist_ok=True)
    
    # Clip all tifs
    for tif in all_tifs: clip_tif(clip_dir, tif, minx, miny, maxx, maxy)

    pass


### HELPER FNS ###
def download_zip(prod_name:str, prod_link:str, prod_checksum:str, download_dir:str):
    print(f'About to download {prod_name} at link {prod_link} to {download_dir}', flush=True)
    try:
        os.makedirs(download_dir, exist_ok=True)
        os.chdir(download_dir)
        subprocess.run(['wget', prod_link])
    except subprocess.CalledProcessError as e:
        print(f'Failed to download {prod_name}: {e}', flush=True)
        print(e.stderr, flush=True)

    downloaded_f = glob.glob(download_dir)[0]
    print(f'Successfully downloaded file {downloaded_f}', flush=True)
    return downloaded_f
    

def unzip(prod_name:str, f:str):
    print(f'About to unzip {prod_name} at {f}', flush=True)
    try:
        subprocess.run(['unzip', f])
    except subprocess.CalledProcessError as e:
        print(f'Failed to unzip {prod_name}: {e}', flush=True)
        print(e.stderr, flush=True)

    print(f'Successfully unzipped file {f}', flush=True)
    return True


def confirm_checksum(f:str, checksum:str):
    try:
        subprocess.run(['md5sum', f])
        download_checksum = result.stdout
    except subprocess.CalledProcessError as e:
        print(f'Error calculating check sum: {e}', flush=True)
        print(e.stderr, flush=True)

    if download_checksum != checksum:
        sys.exit(f'CHECKSUM ERROR FOR {prod_name}.\nDownload checksum {download_checksum} did not match {prod_checksum}.\nPlease delete {os.path.basename(f) after inspection.}')

    print(f'Checksum matches for {prod_name}.\n {download_checksum}={prod_checksum}' flush=True)
    return True


def confirm_matching_crs(all_tifs: list):
    def get_tif_crs(tif):
        return subprocess.run(['gdalsrsinfo', tif, '-o', 'wkt2_2019'], capture_output=True, text=True)

    target_crs = get_tif_crs(all_tifs[0])
    crs_not_match = [get_tif_crs(tif)!=target_crs for tif in all_tifs]
    
    matching = sum(crs_not_match)==0
    if not matching:
        print('WARNING: Not all wkts match for this product. Proceed with caution.')

    return matching, target_crs
    
    
def clip_tif(clip_dir, f, minx, miny, maxx, maxy):
    print(f'Currently clipping {f}')

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
    f_new = clipped_dir + f.split('/')[-1].replace('.tif', '_clipped.tif')
    rds_clipped.rio.to_raster(f_new)
    print(f'Saved clipped file to {f_new}.')

    # Memory management
    del rds_clipped
    gc.collect()

    return True


def save_metadata(download_dir:str):
    # # Save metadata and delete folders containing unclipped data
    # os.system('cp -r '+dir_path+'/*metadata* '+new_out_dir)
    # os.system('cp -r '+dir_path+'/*.txt '+new_out_dir)
    # os.system('cp -r '+dir_path+'/*.dbf '+new_out_dir)
            
    # os.system('cp -r '+dir_path+'/CSV_Data/* '+new_out_dir)
    # os.system('cp -r '+dir_path+'/General_Metadata/* '+new_out_dir)
    # os.system('cp -r '+dir_path+'/*.txt '+new_out_dir)
    # os.system('cp -r '+dir_path+'/Tif/*.dbf '+new_out_dir)
    pass


def format_roi(ROI: str):
    # Get shapefile to later calculate bounding box
    ROI = gpd.read_file(ROI)
    ROI = ROI.explode(index_parts=False)
    ROI['area'] = ROI.explode(index_parts=False).area
    ROI = ROI[ROI['area']==ROI['area'].max()] # Just get the polygon for mainland CA, not the little islands

    return ROI

def calculate_bbox(ROI:geopandas.GeoDataFrame, crs:str):
    ROI_reproj = ROI.to_crs(orig_crs)
    minx, miny, maxx, maxy = tuple(*list(ROI_reproj.bounds.to_records(index=False)))

    return minx, miny, maxx, maxy


if __name__ == '__main__':
    prod_name = sys.argv[1]
    prod_link = sys.argv[2]
    prod_checksum = sys.argv[3]
    download_dir = sys.argv[4]
    metadata_dir = sys.argv[5]
    ROI = sys.argv[6]

    download_landfire(prod_name, prod_link, prod_checksum, download_dir)
    clip_landfire(download_dir, ROI)
