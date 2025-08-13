import os, sys, subprocess


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
