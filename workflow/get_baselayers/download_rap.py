import pandas as pd
import xarray as xr
import rioxarray as rxr
import numpy as np
import sys, subprocess, os, gc

sys.path.append("workflow/utils")
from geo_utils import get_crs, get_gdalinfo, calculate_bbox, format_roi, export_to_tiff
from file_utils import confirm_checksum

if __name__ == '__main__':
    print(f'Running download_rap.py with arguments {'\n'.join(sys.argv)}\n')
    prod_link = sys.argv[1]
    checksum_filename_ref = sys.argv[2]
    curr_year = sys.argv[3]
    ROI = sys.argv[4]
    out_dir = sys.argv[5]
    out_done_f = sys.argv[6]

    # Download curr_year of RAP
    try:
        # Make sure we just have mainland CA
        ROI = format_roi(ROI)

        # get checksum, filename for current year
        ref_csv = pd.read_csv(checksum_filename_ref)
        checksum, f = ref_csv[ref_csv['year']==int(curr_year)][['checksum', 'file_name']].values[0]
        os.makedirs(out_dir, exist_ok=True)
        out_f = out_dir + f
        f = prod_link + str(f)

        # get bbox window for download
        target_crs = get_crs(f)
        minx, miny, maxx, maxy = calculate_bbox(ROI, target_crs)

        # get nodata val for original data
        print(f'About to get gdalinfo for {f}', flush=True)
        gdalinfo_dict = get_gdalinfo(f)
        nodataval = np.unique([gdalinfo_dict[band]['nodata'] for band in gdalinfo_dict.keys()])
        if len(nodataval) > 1:
            print(f'WARNING: Found multiple nodatavals for different band: {nodataval}. Moving forward with 0th nodataval')
        nodataval = nodataval[0]
        
        # download just desired window
        print(f'About to download {f}.', flush=True)
        cmd = [
            'gdal_translate',
            '-co', 'compress=lzw',
            '-ot', 'Int8',
            f,
            '-projwin', minx, maxy, maxx, miny, 
            '-a_nodata', nodataval,
             out_f
        ]
        cmd = [str(i) for i in cmd]
        subprocess.run(cmd)
        print(f'Downloaded {f}.', flush=True)

        # add band names
        rap_ds = rxr.open_rasterio(out_f)

        # Update band names and re-save
        rap_ordered_bands = ['annual_forb_grass', 'bare_ground', 'litter', 'perennial_forb_grass', 'shrub', 'tree']
        rap_ds['band'] = rap_ordered_bands 
        rap_ds.rio.to_raster(
            out_f,
            dtype='int8', 
            compress='LZW'
        )

        # Create output done file
        subprocess.run(['touch', out_done_f])

    except Exception as e:
        print(f'WARNING: Failed to download {curr_year} RAP data. Error message:\n{e}')