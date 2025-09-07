import subprocess, os, glob, sys
import pandas as pd
import rioxarray as rxr
import xml.etree.ElementTree as ET

sys.path.append("workflow/utils")
from geo_utils import get_crs, calculate_bbox, format_roi, clip_tif

'''
python workflow/get_baselayers/download_clip_nlcd.py https://www.mrlc.gov/downloads/sciweb1/shared/mrlc/data-bundles/Annual_NLCD_LndCov_YEAR_CU_C1V1.zip data/baselayers/temp/NLCD/ 1990 1991 data/ROI/california.shp
'''

def download_clip_nlcd(f, out_dir, ROI):
    downloaded_f = download_nlcd(f, out_dir)

    # then, clip
    unzip_clip(downloaded_f, out_dir, ROI)

    # export vegtype_code_name.csv
    year_df = get_code_vegname_df(downloaded_f)

    return year_df

def download_nlcd(f, out_dir):
    print(f'About to download {f} to {out_dir}', flush=True)
    try:
        os.makedirs(out_dir, exist_ok=True)
        subprocess.run(['wget', '-q', '-P', out_dir, f])
        print(f'Downloaded {f} to {out_dir}.')
    except subprocess.CalledProcessError as e:
        print(f'Failed to download {f}: {e}', flush=True)
        print(e.stderr, flush=True)

    if out_dir[-1] != '/': out_dir += '/'
    suffix=f.split('/')[-1]
    downloaded_f = glob.glob(f'{out_dir}{suffix}')[0]
    print(f'Successfully downloaded file {downloaded_f}', flush=True)
    return downloaded_f


def unzip_clip(f, out_dir, ROI):
    print(f'About to unzip {f} to {out_dir}', flush=True)
    try:
        subprocess.run(['unzip', f, '-d', out_dir])
    except subprocess.CalledProcessError as e:
        print(f'Failed to unzip {f}: {e}', flush=True)
        print(e.stderr, flush=True)
    
    print(f'About to clip {f} to {ROI}.', flush=True)
    tif = f.replace('.zip', '.tif')
    crs = get_crs(tif)

    bbox_by_crs = {}
    ROI = format_roi(ROI)
    bbox = calculate_bbox(ROI, crs)
    
    clip_tif(out_dir, tif, *bbox)

    # Remove unclipped data
    print(f'Deleting zip file {f} and unclipped tif {tif}', flush=True)
    subprocess.run(['rm', '-r', f])
    subprocess.run(['rm', '-r', tif])
    
def get_code_vegname_df(downloaded_f):
    rat = downloaded_f.replace('.zip', '.tif.aux.xml')
    print(f'Reading {rat}')
    tree = ET.parse(rat)
    root=tree.getroot()
    codes, class_names = [], []
    for nlcd_info in root.findall('.//Row'): 
        index = nlcd_info.get('index')
        codes.append(int(nlcd_info.findall('F')[0].text))
        class_names.append(nlcd_info.findall('F')[-1].text)
        
    df = pd.DataFrame({
        'NLCD_CODE':codes,
        'NLCD_NAMES':class_names
    })
    return df


if __name__ == '__main__':
    print(f'Running download_clip_nlcd.py with arguments {'\n'.join(sys.argv)}\n')
    download_link=sys.argv[1]
    out_dir=sys.argv[2]
    vegcodes_csv=sys.argv[3]
    start_year=int(sys.argv[4])
    end_year=int(sys.argv[5])
    ROI=sys.argv[6]
    done_flag=sys.argv[7]

    df_list = []
    for yr in range(start_year, end_year+1):
        df = download_clip_nlcd(download_link.replace('YEAR', str(yr)), out_dir, ROI)
        df['year'] = yr
        df_list.append(df)

    os.makedirs(os.path.dirname(vegcodes_csv), exist_ok=True)
    pd.concat(df_list).to_csv(vegcodes_csv)
    subprocess.run(['touch', done_flag])


