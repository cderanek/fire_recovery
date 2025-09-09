import sys, glob, gc, os, subprocess
import pandas as pd
import numpy as np
import xarray as xr
import rioxarray as rxr

sys.path.append("workflow/utils")
from geo_utils import reproj_align_rasters

def update_agdev_mask(r, rat, agdev_mask_combined, dtype_out):
    '''
    Given an EVT raster r and a RAT file with corresponding veg codes, returns an np 2d array
    representing an agricultural/development mask for the raster r.
    0: unmasked values, not known to be ag or dev
    1: masked values, ag or dev
    '''
    
    # make np array with 1s for agdev, 0 for non-agdev
    ag_dev_values = rat['NLCD_CODE'][rat['NLCD_NAMES'].str.lower().str.contains('agricult|develop|crop|pasture|cultiv')].values
    ag_dev_mask = np.where(np.isin(r.data.squeeze(), ag_dev_values), 1, 0).astype(dtype_out)
    
    # create new mask if none exists
    if agdev_mask_combined is None:
        agdev_mask_combined = ag_dev_mask

    # update existing mask        
    else:
        agdev_mask_combined = np.nanmax(
            np.array(
                [agdev_mask_combined, ag_dev_mask]
                ).astype(dtype_out), 
            axis=0
            ).squeeze()
        
    # memory management
    del ag_dev_mask, ag_dev_values
    gc.collect()
    
    return agdev_mask_combined

def create_agdev_mask(nlcd_dir, vegcodes_csv, merged_out_path, dtype_out):
    # glob
    all_nlcd_tifs = glob.glob(os.path.join(nlcd_dir,'*_clipped.tif'))
    template_tif = all_nlcd_tifs[0]

    # open agdev rasters + NLCD code/name mapping
    template_r = rxr.open_rasterio(template_tif)
    vegcodes_df = pd.read_csv(vegcodes_csv)
    vegcodes_df['NLCD_CODE']=vegcodes_df['NLCD_CODE'].astype('int')
    vegcodes_df['year']=vegcodes_df['year'].astype('int')

    # update agdev mask with each year's agdev info
    agdev_mask_combined = None
    for f in all_nlcd_tifs:
        print(f'Adding information from {f} to agrdev mask.')
        # get vegcodes for current year
        curr_yr = int(os.path.basename(f).split('_')[3])
        vegcodes_yr = vegcodes_df[vegcodes_df['year']==curr_yr]

        # reproj align
        r = rxr.open_rasterio(f)
        _, r = reproj_align_rasters('reproj_match', template_r, r)

        # update mask
        agdev_mask_combined = update_agdev_mask(r, vegcodes_yr, agdev_mask_combined, dtype_out)

        # memory management
        del r, vegcodes_yr
        gc.collect()

        print(f'Finished adding information from {f} to agrdev mask.')

    print(agdev_mask_combined)
    print('--------')
    print(template_r.coords, template_r.dims, template_r.rio.crs)

    # create merge rxr da
    agdev_mask_da = xr.DataArray(
        np.array([agdev_mask_combined.squeeze()]),
        coords=template_r.coords,
        dims=template_r.dims
    ).rio.write_crs(template_r.rio.crs)

    # memory management
    del agdev_mask_combined
    gc.collect()

    # save output
    agdev_mask_da.rio.to_raster(merged_out_path, dtype=dtype_out)
    
    # save printout to summary txt file
    with open(merged_out_path.replace('.tif', '_summary.txt'), 'w') as f:
        print(agdev_mask_da, file=f)



if __name__ == "__main__":
    print(f'Running make_agdev_mask.py with arguments {'\n'.join(sys.argv)}\n')
    nlcd_dir = sys.argv[1]
    vegcodes_csv = sys.argv[2]
    merged_out_path = sys.argv[3]
    dtype_out = sys.argv[4]
    done_flag = sys.argv[5]
    
    create_agdev_mask(nlcd_dir, vegcodes_csv, merged_out_path, dtype_out)
    
    subprocess.run(['touch', done_flag])