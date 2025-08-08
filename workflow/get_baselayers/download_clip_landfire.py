import subprocess, sys, os, glob

def download_landfire(prod_name, prod_link, prod_checksum, download_dir):
    # wget
    print(f'About to download {prod_name} at link {prod_link} to {download_dir}')
    try:
        os.makedirs(download_dir, exist_ok=True)
        os.chdir(download_dir)
        result = subprocess.run(['wget', prod_link], capture_output=True, text=True)
        downloaded_f = glob.glob(download_dir)[0]
        print(f'Successfully downloaded file {downloaded_f}')
    except subprocess.CalledProcessError as e:
        print(f'Failed to download {prod_name}: {e}')
        print(e.stderr)

    confirm_checksum(downloaded_f)

    # unzip



def confirm_checksum(f, checksum):
    try:
        result = subprocess.run(['md5sum', f], capture_output=True, text=True)
        download_checksum = result.stdout
    except subprocess.CalledProcessError as e:
        print(f'Error calculating check sum: {e}')
        print(e.stderr)

    if download_checksum != checksum:
        sys.exit(f'CHECKSUM ERROR FOR {prod_name}.\nDownload checksum {download_checksum} did not match {prod_checksum}.\nPlease delete {os.path.basename(f) after inspection.}')

    return True


def clip_tifs(tif_f, ROI):
    pass


def save_metadata(download_dir):
    pass


if __name__ == '__main__':
    prod_name = sys.argv[1]
    prod_link = sys.argv[2]
    prod_checksum = sys.argv[3]
    download_dir = sys.argv[4]

    download_landfire(prod_name, prod_link, prod_checksum, download_dir)