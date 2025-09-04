

def merge_topo(landfire_dir, out_f, h_dist_path, evt_path, clipBBox=None):
    print('\n\n ------------- Entering merge_topo  -------------', flush=True)
    
    # Get the paths to all topo features
    elev_path = glob.glob(landfire_dir+'*Elev*/*.tif')[0]
    asp_path = glob.glob(landfire_dir+'*Asp*/*.tif')[0]
    slpD_path = glob.glob(landfire_dir+'*SlpD*/*.tif')[0]
    slpP_path = glob.glob(landfire_dir+'*SlpP*/*.tif')[0]

    print(f'Opening cumulative disturbance and ag_dev masks', flush=True)
    anydist_rxr = (
        xr.open_dataset(h_dist_path, format='NETCDF4', engine='netcdf4')
        .cumulative_annual_dist
        .sortby('time')
        .isel(time=-1)
        .drop_vars('time') 
        ) # Get latest date with cumulative disturbances to make a mask for any disturbance
    anydist_rxr['spatial_ref'] = xr.open_dataset(h_dist_path, format='NETCDF4', engine='netcdf4')['spatial_ref']
    anydist_rxr.rio.write_crs(anydist_rxr['spatial_ref'].crs_wkt, inplace=True)
    ag_dev_mask_rxr = xr.open_dataset(evt_path, format='NETCDF4', engine='netcdf4').ag_dev_mask
    ag_dev_mask_rxr['spatial_ref'] = xr.open_dataset(evt_path, format='NETCDF4', engine='netcdf4')['spatial_ref']
    ag_dev_mask_rxr.rio.write_crs(ag_dev_mask_rxr['spatial_ref'].crs_wkt, inplace=True)
    
    if clipBBox != None: 
        clipBBox = clipBBox.to_crs(anydist_rxr.spatial_ref.crs_wkt)
        anydist_rxr = anydist_rxr.rio.clip(clipBBox.geometry, clipBBox.crs)
        ag_dev_mask_rxr = ag_dev_mask_rxr.rio.clip(clipBBox.geometry, clipBBox.crs)
    template_rxr = anydist_rxr
    
    print(f'Creating combined mask for ag_devl and cumulative disturbance', flush=True)
    anydist_rxr, ag_dev_mask_rxr = reproj_align_rasters(anydist_rxr, ag_dev_mask_rxr)
    anydist_rxr.data = np.where(anydist_rxr.data  > 0, 1, 0)
    anydist_rxr.data = np.where(ag_dev_mask_rxr.data  > 0, 1, anydist_rxr).astype('int8')
    anydist_rxr = (
        anydist_rxr
        .expand_dims(dim='band')
        .assign_coords(band=['anydist'])
        .transpose('band','y','x')
        .fillna(-128)
        .rio.set_nodata(-128)
        .astype('int8')
        )
    
    print(f'Opening and aligning elevation and aspect tifs', flush=True)
    # Align the topo layers to the disturbance layer
    if clipBBox != None:
        _, elev_rxr, asp_rxr, slpD_rxr, slpP_rxr = reproj_align_rasters(template_rxr, *[rxr.open_rasterio(f).rio.clip(clipBBox.geometry, clipBBox.crs) for f in [elev_path, asp_path, slpD_path, slpP_path]])
    else: 
        _, elev_rxr, asp_rxr, slpD_rxr, slpP_rxr = reproj_align_rasters(template_rxr, *[rxr.open_rasterio(f) for f in [elev_path, asp_path, slpD_path, slpP_path]])
    
    topo_rxrL = [elev_rxr, asp_rxr, slpD_rxr, slpP_rxr]
    band_namesL = ['Elevation', 'Aspect', 'Slope_Degrees', 'Slope_Percent']
    
    del elev_rxr, asp_rxr, slpD_rxr, slpP_rxr
    gc.collect()

    print(f'Merging all topo and mask layers --> saving to netcdf', flush=True)
    # Merge the layers and save to a .nc file with dims band, y, x
    topo_rxrL = [r.assign_coords(band=[band_name]).transpose('band','y','x').fillna(-9999).rio.set_nodata(-9999).astype('int32') for r, band_name in zip(topo_rxrL, band_namesL)]
    out_rxr_merged = xr.concat([anydist_rxr, *topo_rxrL], dim=('band'))
    out_rxr_merged = copy_spatial_properties(template_rxr, out_rxr_merged)
    out_rxr_merged.to_netcdf(out_f)
    print(f'Saved to {os.path.basename(out_f)}', flush=True)

    del out_rxr_merged, topo_rxrL
    gc.collect()
    
    return True