import os, sys, subprocess
import datetime

def confirm_checksum(f:str, checksum:str):
    try:
        result = subprocess.run(['md5sum', f], capture_output=True, text=True)
        download_checksum = (result.stdout.split(' ')[0])
    except subprocess.CalledProcessError as e:
        print(f'Error calculating check sum: {e}', flush=True)
        print(e.stderr, flush=True)

    if download_checksum != checksum:
        sys.exit(f'CHECKSUM ERROR FOR {f}.\nDownload checksum {download_checksum} did not match {checksum}.\nPlease delete {os.path.basename(f)} after inspection.')

    print(f'Checksum matches for {f}.\n {download_checksum}={checksum}', flush=True)
    return True


def get_prod_doy_tile(path: str) -> list:
    """
    Parse product, day of year, and tile information from a Landsat file path.
    
    Parameters:
        path (str): Path to the Landsat file
        
    Returns:
        list: [uid, prod, doy, tile, band, path]
    """
    path_split = path.split('/')[-1].split('_')
    
    uid = '_'.join([path_split[0]]+path_split[3:]) 
    prod = path_split[0]
    doy = path_split[-2]
    tile = path_split[-1]
    band = '_'.join(path_split[1:3])
    
    return [uid, prod, doy, tile, band, path]