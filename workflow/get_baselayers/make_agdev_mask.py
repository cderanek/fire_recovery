import sys, glob
import xarray as xr
import rioxarray as rxr

def update_agdev_mask(r, rat, ag_dev_mask_combined, dtype_out):
    '''
    Given an EVT raster r and a RAT file with corresponding EVT codes, returns an np 2d array
    representing an agricultural/development mask for the raster r.
    0: unmasked values, not known to be ag or dev
    1: masked values, ag or dev
    '''
    ag_dev_values = rat['VALUE'][rat['SAF_SRM'].str.contains('Agricult|agricult|Develop|develop')].values
    ag_dev_mask = np.where(np.isin(r.data.squeeze(), ag_dev_values), 1, 0).astype(dtype_out)
    
    # update existing mask
    if ag_dev_mask_combined is None:
        ag_dev_mask_combined = ag_dev_mask
        
    else:
        ag_dev_mask_combined = np.nanmax(
            np.array(
                [ag_dev_mask_combined, ag_dev_mask]
                ).astype(dtype_out), 
            axis=0
            ).squeeze()
        
    # Free memory
    del ag_dev_mask, ag_dev_values
    gc.collect()
    
    return ag_dev_mask_combined

def create_agdev_mask(evt_dir, merged_out_path, dtype_out):
    # glob
    all_evt_tifs = glob.glob()
    template_tif = all_evt_tifs[0]
    template_r = rxr.open_rasterio(template_tif)

    ag_dev_mask_combined = None
    for f in all_evt_tifs:
        # get rat
        rat = gpd.read_file(f.replace('_clipped.tif', '.tif.vat.dbf'))

        # reproj align
        r = rxr.open_rasterio(f)
        _, r = reproj_align_rasters('reproj_match', template_r, r)

        # update mask
        ag_dev_mask_combined = update_agdev_mask(r, rat, ag_dev_mask_combined)

    # save to output file

    # save printout to summary txt file

if __name__ == "__main__":
    evt_dir = sys.argv[1]
    merged_out_path = sys.argv[2]
    dtype_out = sys.argv[3]
    done_flag = sys.argv[4]