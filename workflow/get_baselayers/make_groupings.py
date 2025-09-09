import sys, subprocess, os, gc, glob
import pandas as pd
import numpy as np
import xarray as xr
import rioxarray as rxr
from itertools import product

sys.path.append("workflow/utils")
from geo_utils import reproj_align_rasters

'''
input:
elevation bands
nlcd dir
nlcd vegcodes csv
merged topo .nc
output path
done flag

output:
.nc file with annual groupings
- dims time, y, x
- format: UID for unique VEGCODE x ELEV grouping 
new csv with groupings code; 0 reserved for groupings we don't care about (ignore Water, Snow, Develop, Crop, Barren, Pasture classes)
.txt file with rxr summary
'''

def make_singleyear_groupings(nlcd_tif, elev_groupings_tif, nlcd_yr_csv, output_csv, nodataval):
    # starts with np array initialized with maskvals -> fills in for each NLCD value x elev grouping
    out_data = np.full(nlcd_tif.data.shape, nodataval)
    for _, row in output_csv.iterrows():
        group_code = int(row['id'])
        nlcd_name = row['NLCD_NAME']
        nlcd_val = int(nlcd_yr_csv['NLCD_CODE'][nlcd_yr_csv['NLCD_NAMES'] == nlcd_name].iloc[0])
        elev_band = int(row['ELEV_BAND'])
        
        out_data = np.where(
            (nlcd_tif.data==nlcd_val) & (elev_groupings_tif.data==elev_band),
            group_code,
            out_data
            )
    
    # return copy of nlcd_tif with new data
    return nlcd_tif.copy(data=out_data)


def get_elev_groupings(template_tif, merged_topo, elevation_band_m):
    topo_rxr = xr.open_dataset(merged_topo, format='NETCDF4', engine='netcdf4')
    crs_orig = topo_rxr['spatial_ref'].crs_wkt
    elev_rxr = topo_rxr.sel(band='Elev').__xarray_dataarray_variable__.rio.write_crs(crs_orig)
    
    # memory management
    del topo_rxr
    gc.collect()

    # open, align
    _, elev_rxr = reproj_align_rasters(
        'reproj_match', 
        template_tif, 
        elev_rxr)

    # set nodata to nan for np division
    elev_rxr.data = np.where(elev_rxr.data == -9999, np.nan, elev_rxr.data)

    # convert to bands
    elev_rxr.data = np.floor_divide(elev_rxr.data, elevation_band_m)
    elev_rxr = elev_rxr.astype('int8')

    # return single-band elev groupings
    return elev_rxr


def get_groupings_csv(nlcd_csv, elev_rxr, elevation_band_m):
    # Determine output groupings, dtype based on max unique groups
    unique_veg_groups = np.unique(nlcd_csv['NLCD_NAMES'][~nlcd_csv['NLCD_NAMES'].str.lower().str.contains('agricult|develop|crop|pasture|cultiv|barren|snow|water')])
    elev_groupings = list(range(np.nanmin(elev_rxr.data), np.nanmax(elev_rxr.data)))
    groups = list(product(unique_veg_groups, elev_groupings))

    # create output csv to organize groupings codes to NLCD name, elev band
    output_csv = pd.DataFrame({
        'id': range(1, len(groups)+1),
        'NLCD_NAME': [group[0] for group in groups],
        'ELEV_BAND': [group[1] for group in groups],
        'ELEV_LOWER_BOUND': [group[1]*elevation_band_m for group in groups]
    })

    # Determine output dtype for final .nc file
    if len(groups)+1 < np.iinfo(np.int8).max: output_dtype = np.int8
    else: output_dtype = np.int16
    nodataval = 0

    return output_csv, output_dtype, nodataval


def make_allyr_groupings(elevation_band_m, nlcd_dir, nlcd_csv, merged_topo, output_f):
    # open inputs
    all_tifs = glob.glob(os.path.join(nlcd_dir, '*_clipped.tif'))
    nlcd_csv = pd.read_csv(nlcd_csv)
    template_tif = rxr.open_rasterio(all_tifs[0])

    # Open, align elevation to template tif -> convert to elev bands groupings
    elev_rxr = get_elev_groupings(template_tif, merged_topo, elevation_band_m)

    # Determine output groupings, dtype based on max unique groups
    output_csv, output_dtype, nodataval = get_groupings_csv(nlcd_csv, elev_rxr, elevation_band_m)
    output_csv.to_csv(output_f.replace('.nc', '_groupings_summary.csv'))

    # Get groupings layer for each year
    yr_layers = []
    for tif_f in all_tifs:
        # align to original tif, extract this year's data
        _, tif = reproj_align_rasters('reproj_match', template_tif, rxr.open_rasterio(tif_f))
        
        # get this year's original NLCD csv + format year as np datetime
        year = int(os.path.basename(tif_f).split('_')[3])
        nlcd_yr_csv = nlcd_csv[nlcd_csv['year'] == year]
        curr_date = np.datetime64('-'.join([str(year), '12', '31']), 'ns')

        # get this year's grouping layer, add date dim, dtype info]
        yr_layer = make_singleyear_groupings(tif, elev_rxr, nlcd_yr_csv, output_csv, nodataval)
        yr_layers.append(
            yr_layer
            .expand_dims(time=[curr_date])
            .transpose('band', 'time', 'y','x')
            .fillna(nodataval)
            .rio.set_nodata(nodataval)
            .astype(output_dtype)
        )

    # merge all tifs along band dim and save
    merged_groupings = xr.concat(yr_layers, dim=('time'))
    merged_groupings.rio.write_crs(yr_layers[0].rio.crs, inplace=True)
    merged_groupings.to_netcdf(output_f)
    print(f'Saved to {output_f}', flush=True)

    # save printout to summary txt file
    with open(output_f.replace('.nc', '_summary.txt'), 'w') as f:
        print(merged_groupings, file=f)

    return True


if __name__ == '__main__':
    print(f'Running make_groupings.py with arguments {'\n'.join(sys.argv)}\n')
    elevation_band_m = int(sys.argv[1])
    nlcd_dir = sys.argv[2]
    nlcd_vegcodes_csv = sys.argv[3]
    merged_topo = sys.argv[4]
    output_f = sys.argv[5]
    done_flag = sys.argv[6]

    make_allyr_groupings(elevation_band_m, nlcd_dir, nlcd_vegcodes_csv, merged_topo, output_f)

    subprocess.run(['touch', done_flag])