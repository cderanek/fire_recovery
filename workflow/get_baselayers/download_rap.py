import pandas as pd
import xarray as xr
import numpy as np
import sys, subprocess, os, gc

sys.path.append("workflow/utils")
from geo_utils import get_crs, get_gdalinfo, calculate_bbox, format_roi, export_to_tiff
from file_utils import confirm_checksum

if __name__ == '__main__':
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
        out_f_temp = out_f.replace('.tif', '_temp.tif')
        f = prod_link + str(f)

        # get bbox window for download
        target_crs = get_crs(f)
        minx, miny, maxx, maxy = calculate_bbox(ROI, target_crs)

        # get nodata val for original data
        nodataval = get_gdalinfo(f)['nodata']

        # download just desired window
        print(f'About to download {f}.', flush=True)
        cmd = [
            'gdal_translate',
            '-co', 'compress=lzw',
            '-ot', 'Int8',
            f,
            '-projwin', minx, maxy, maxx, miny, 
            '-a_nodata', nodataval,
             out_f_temp
        ]
        cmd = [str(i) for i in cmd]
        # subprocess.run(cmd)
        print(f'Downloaded {f}.', flush=True)

        # convert to int8 dataset
        rap_ds = xr.open_dataset(out_f_temp)

        # convert to dataset with separate, named bands for each layer
        rap_ordered_bands = ['annual_forb_grass', 'bare_ground', 'litter', 'perennial_forb_grass', 'shrub', 'tree']
        rap_ds['band'] = rap_ordered_bands 
        band_dict = {
            band: rap_ds['band_data'].sel(band=band).drop_vars('band')
            for band in rap_ordered_bands
        }
        rap_ds = xr.Dataset(band_dict)
        print(rap_ds)

        del band_dict
        gc.collect()

        # convert to int8, and export each band to a separate tif
        ## TODO: export to .nc instead
        print(f'About to convert to int8 and export to single-band tifs.', flush=True)
        for band in rap_ordered_bands:
            print(f'Working on band {band}')
            # Convert int8
            rap_ds[band].data[:] = np.where(np.isnan(rap_ds[band].data), -128, rap_ds[band].data)
            rap_ds[band].data[:] = np.where((rap_ds[band].data<0) | (rap_ds[band].data>100), -128, rap_ds[band].data)
            rap_ds[band] = rap_ds[band].rio.set_nodata(-128).astype('int8')
            # export to tif
            (
                rap_ds[band]
                .rio.to_raster(
                    out_f.replace('.tif',f'_{band}.tif'),
                    dtype='int8', 
                    nodata=-128,
                    compress='LZW'
                )
            )
            print(f'Completed band {band}')
        rap_ds.rio.set_nodata(-128).astype('int8').to_netcdf(out_f.replace('.tif', '.nc'))
        print(f'Converted to int8 and exported to single-band tifs and multiband nc.', flush=True)

        # # Confirm checksum Checksums WON'T MATCH b/c not downloading full extent
        # confirm_checksum(out_f, checksum)

        # subprocess.run(
        #     'rm',  out_f_temp
        # )

        # Create output done file
        subprocess.run(['touch', out_done_f])

    except Exception as e:
        print(f'WARNING: Failed to download {curr_year} RAP data. Error message:\n{e}')