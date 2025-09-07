import glob, subprocess
import xarray as xr
import rioxarray as rxr
import numpy as np
import geopandas as gpd
import sys, os, gc

sys.path.append("workflow/utils")
from geo_utils import get_crs, reproj_align_rasters

def make_hdist(dist_dir, out_f, clipBBox=None, NO_DIST_VAL=0, dtype_out='int8', xdim='x', ydim='y', timedim='time', valid_yrs=range(1999,2024)):
    '''Returns:
    an rxr object with dims (time, y, x)
        bands = 
            annual disturbance with 1, 2, 3 for low, med, high severity disturbance
            cumulative annual disturbance summed over previous years
    '''
    
    # Get all the dist/hdist rasters
    all_dist_paths = list_dist_tifs(dist_dir)

    # Create output raster (filled with NO_DIST_VALs to start) to fill with annual disturbance severity info
    template_rxr, annual_dist_da = create_dist_template(all_dist_paths, NO_DIST_VAL, valid_yrs, clipBBox)
    
    # For each Dist tif, extract all known annual disturbances and update our output dist raster
    for i, f in enumerate(all_dist_paths):
        # Open the current raster layer and associated RAT
        print(f'Processing file {i+1}/{len(all_dist_paths)}: {os.path.basename(f)}', flush=True)
        r = rxr.open_rasterio(f)
        rat = gpd.read_file(f.replace('_clipped.tif', '.tif.vat.dbf'))
        rat.columns = rat.columns.str.upper()
        
        # Make sure this raster is aligned with the template
        _, r = reproj_align_rasters('reproj_match', template_rxr, r)

        # Get the year column in the VAT
        rat_yr_col = rat.columns.values[np.isin(rat.columns.values, ['YEAR', 'HDIST_YR', 'DIST_YEAR', 'CALENDAR_Y'])][0]

        for curr_yr in np.unique(rat[rat_yr_col].dropna()):
            try:
                if int(curr_yr) in valid_yrs:
                    print(f'\tUpdating annual severity layer for {curr_yr}')
                    curr_date = np.datetime64('-'.join([str(curr_yr), '12', '31']), 'ns')

                    # Make a layer with 0-3 disturbance classification (No, Low, Med, High sev) for current year
                    lowsevvals, medsevvals, highsevvals = [rat['VALUE'][(rat['SEVERITY']==sev) & (rat[rat_yr_col]==curr_yr)].values for sev in ['Low', 'Medium', 'High']]
                    sev_layer = np.where(np.isin(r, lowsevvals), 1, 0)
                    sev_layer = np.where(np.isin(r, medsevvals), 2, sev_layer)
                    sev_layer = np.where(np.isin(r, highsevvals), 3, sev_layer)

                    # For every year represented in this file, update the annual_dist_da "annual_dist" band to have the max of it's previous value and this sev_layer
                    # print(f'annual dist_da {annual_dist_da}', flush=True)
                    annual_dist_da.annual_dist.sel(time=curr_date).data[:] = np.nan_to_num(
                        np.max(
                            [
                                annual_dist_da.annual_dist.sel(time=curr_date).data, 
                                sev_layer.squeeze()
                            ], 
                            axis=0),
                        nan=-128).astype(dtype_out)
            except:
                print(f'WARNING: {curr_yr} not a valid year.', flush=True)
                
        # Memory management
        del _, r, rat, rat_yr_col, sev_layer
        gc.collect()
    
    # Sum up each year's annual cumulative disturbance rasters
    print(f'Adding cumulative annual disturbance variable', flush=True)
    annual_dist_da.cumulative_annual_dist.data[:] = annual_dist_da.annual_dist.cumsum(dim=timedim, dtype=dtype_out).astype(dtype_out)
    
    print(f'Adding spatial properties, nodata vals', flush=True)
    annual_dist_da.annual_dist.rio.set_nodata(NO_DIST_VAL, inplace=True)
    annual_dist_da.cumulative_annual_dist.rio.set_nodata(NO_DIST_VAL, inplace=True)
    annual_dist_da.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
    print(annual_dist_da, flush=True)
    
    # Save to final output file
    print(f'Saving annual disturbance, and annual cumulative disturbance to {out_f}', flush=True)
    annual_dist_da.to_netcdf(out_f, format='NETCDF4', engine='netcdf4')
    print(f'Successfully saved annual_dist_da\n{annual_dist_da}', flush=True)

    # Save summary of data structure
    with open(out_f.replace('.nc', '_summary.txt'), 'w') as f:
        print(annual_dist_da, file=f)
        
    del annual_dist_da
    gc.collect()
    
    return True


def list_dist_tifs(dist_dir):
    print(dist_dir)
    all_dist_paths = glob.glob(dist_dir+'clipped/*clipped.tif')
    print(f'Found {len(all_dist_paths)} disturbance rasters: \n\t{'\n\t'.join([os.path.basename(f) for f in all_dist_paths])}', flush=True)
    
    return all_dist_paths


def create_dist_template(all_dist_paths, NO_DIST_VAL, valid_yrs, clipBBox):
    # create a template rxr object for just one year of data
    template_crs = get_crs(all_dist_paths[0])
    template_rxr = (
        rxr.open_rasterio(all_dist_paths[0])
        .assign_coords(band=["annual_dist"])
        .rio.set_nodata(NO_DIST_VAL)
        .rio.write_crs(template_crs)
    )
    template_rxr.attrs['_FillValue'] = NO_DIST_VAL
    template_rxr.data = np.full(template_rxr.data.shape, fill_value=NO_DIST_VAL, dtype=dtype_out)
    
    if clipBBox != None: 
        clipBBox = clipBBox.to_crs(template_rxr.spatial_ref.crs_wkt)
        template_rxr = template_rxr.rio.clip(clipBBox.geometry, clipBBox.crs)

    # create a template to hold all years disturbance + cumulative disturbance    
    annual_dist_rxrL = [template_rxr.copy().expand_dims(time=[np.datetime64('-'.join([str(yr), '12', '31']), 'ns')]) for yr in valid_yrs]
    annual_dist_da = xr.concat(annual_dist_rxrL, dim=timedim).transpose('band', timedim, ydim, xdim).astype(dtype_out)
    annual_dist_da = xr.Dataset({
        'annual_dist': annual_dist_da.squeeze(dim='band', drop=True),  # Remove band dimension if present
        'cumulative_annual_dist': annual_dist_da.squeeze(dim='band', drop=True)
    }).transpose(timedim, ydim, xdim).astype(dtype_out)

    return template_rxr, annual_dist_da


if __name__ == '__main__':
    print('\n\n ------------- MAKING ANNUAL, CUMULATIVE DISTURBANCE BASELAYER  -------------', flush=True)
    print(f'Arguments input: {'\n'.join(sys.argv)}\n')
    annual_dist_dir = sys.argv[1]
    merged_nc_out_f = sys.argv[2]
    nodataval = sys.argv[3]
    dtype_out = sys.argv[4]
    xdim, ydim, timedim = sys.argv[5], sys.argv[6], sys.argv[7]
    start_year, end_year = int(sys.argv[8]), int(sys.argv[9])
    done_flag = sys.argv[10]

    make_hdist(annual_dist_dir, merged_nc_out_f, clipBBox=None, NO_DIST_VAL=nodataval, dtype_out=dtype_out, xdim=xdim, ydim=ydim, timedim=timedim, valid_yrs=range(start_year,end_year))
    subprocess.run(['touch', done_flag])