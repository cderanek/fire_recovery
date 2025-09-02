import sys, glob, gc
import xarray as xr
import rioxarray as rxr

def update_agdev_mask(r, rat, agdev_mask_combined, dtype_out):
    '''
    Given an EVT raster r and a RAT file with corresponding EVT codes, returns an np 2d array
    representing an agricultural/development mask for the raster r.
    0: unmasked values, not known to be ag or dev
    1: masked values, ag or dev
    '''
    ag_dev_values = rat['VALUE'][rat['SAF_SRM'].str.contains('Agricult|agricult|Develop|develop')].values
    ag_dev_mask = np.where(np.isin(r.data.squeeze(), ag_dev_values), 1, 0).astype(dtype_out)
    
    # update existing mask
    if agdev_mask_combined is None:
        agdev_mask_combined = ag_dev_mask
        
    else:
        agdev_mask_combined = np.nanmax(
            np.array(
                [agdev_mask_combined, ag_dev_mask]
                ).astype(dtype_out), 
            axis=0
            ).squeeze()
        
    # Free memory
    del ag_dev_mask, ag_dev_values
    gc.collect()
    
    return agdev_mask_combined

def create_agdev_mask(evt_dir, merged_out_path, dtype_out):
    # glob
    all_evt_tifs = glob.glob(f'{evt_dir}clipped/*_clipped.tif')
    template_tif = all_evt_tifs[0]
    template_r = rxr.open_rasterio(template_tif)

    agdev_mask_combined = None
    for f in all_evt_tifs:
        print(f'Adding information from {f} to agrdev mask.')
        # get rat
        rat = gpd.read_file(f.replace('_clipped.tif', '.tif.vat.dbf'))

        # reproj align
        r = rxr.open_rasterio(f)
        _, r = reproj_align_rasters('reproj_match', template_r, r)

        # update mask
        agdev_mask_combined = update_agdev_mask(r, rat, agdev_mask_combined)

        del r, rat
        gc.collect()

        print(f'Finished adding information from {f} to agrdev mask.')

    # save to output file
    agdev_mask_da = xr.DataArray(
        agdev_mask_combined,
        coords=template_r.coords,
        dims=template_r.dims
    ).rio.write_crs(template_r.rio.crs)
    del agdev_mask_combined
    gc.collect()
    agdev_mask_da.rio.to_raster(merged_out_path, dtype=dtype_out)
    
    # save printout to summary txt file
    with open({merged_out_path.replace('.tif', '_summary.txt')}, 'w') as f:
        print(agdev_mask_da, file=f)


if __name__ == "__main__":
    evt_dir = sys.argv[1]
    merged_out_path = sys.argv[2]
    dtype_out = sys.argv[3]
    done_flag = sys.argv[4]
    
    create_agdev_mask(evt_dir, merged_out_path, dtype_out)
    
    subprocess.run(['touch', done_flag])