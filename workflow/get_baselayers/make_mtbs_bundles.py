import pandas as pd
import geopandas as gpd
import subprocess, os, glob, shutil, sys
import rioxarray as rxr
import xarray as xr
from rasterio.features import rasterize
from multiprocessing import Pool, cpu_count
import pyproj

sys.path.append("workflow/utils")
from geo_utils import clip_raster_to_poly, export_to_tiff

'''
takes in 
    WUMI CSV with subfires, 
    WUMI raster with projection info,
    WUMI dir (with internal dirs per year, per fire), 
    MTBS sev raster dir (with internal dirs for year, each year has 1 full CA tif),
    start_year, end_year,
    output_dir to save extracted, clipped severity raster + burn boundary shp file

filters WUMI CSV to get WUMI fireid's for:
    CA
    start_year <= fire year <= end_year


extracts MTBS sev raster for each subfire (clipped to subfire boundary)
checks that >90% of pixels in the burn boundary are marked as burned and <10% outside the burn boundary are burned --> assert statement for debugging
check that subfire poly is not a mulitpolygon --> assert statement for debugging

creates new dir with structure data/recovery_maps/{wumi_fireid}/spatialinfo/ containing shapefiles of fire boundaries and clipped severity rasters
the severity raster and shapefiles should be called {wumi_fireid}_burnbndy.shp and {wumi_fireid}_sevraster.tif
'''


def get_wumi_id_years(
    subfires_csv:str, 
    start_year:int, 
    end_year:int):
    subfires = pd.read_csv(subfires_csv)

    # filter to only CA fires in the year range
    subfires = subfires[
        subfires['mtbs_ID'].str.lower().str.startswith('ca', na=False) & 
        (subfires['year']>=start_year) & 
        (subfires['year']<=end_year)]

    return zip(subfires['fireid'].astype('str'), subfires['year'].astype('int')), len(subfires)


def confirm_burned(
    fire_sev_tif:xr.DataArray, 
    burn_poly: gpd.GeoDataFrame
    ) -> bool:
    # returns True if >90% of pixels in the burn boundary are true MTBS burn values (1<=val<=6) AND all pixels out of the burn boundary are nan
    
    # Rasterize burn poly
    burn_poly = burn_poly.to_crs(fire_sev_tif.rio.crs)
    mask = rasterize(
        burn_poly.geometry,
        out_shape=fire_sev_tif.data.squeeze().shape,
        transform=fire_sev_tif.rio.transform(),
        fill=0, # outside burn boundary
        default_value=1, # inside burn boundary
        dtype='uint8',
        all_touched=True
    )

    # Count total pixels, make bool layer of burned pixels 
    total_burn_pixels, total_unburned_pixels = np.sum(mask), np.sum(mask==0)
    burn_data = fire_sev_tif.data.squeeze()
    burn_pixels = (burn_data>0) & (burn_data<=6)

    # Count burn pixels in/out fire boundary
    burn_pixels_in_boundary = burn_pixels & mask
    burn_pixels_out_boundary = burn_pixels & (mask==0)

    pct_burned_in_boundary = np.sum(burn_pixels_in_boundary) / np.sum(mask==1)
    pct_burned_out_boundary = np.sum(burn_pixels_out_boundary) / np.sum(mask)

    if (pct_burned_in_boundary > 0.9) and (pct_burned_out_boundary < 0.1):
        return True
    else:
        print(f'FAILED confirm burn boundary test.\npct_burned_in_boundary: {pct_burned_in_boundary}\npct_burned_out_boundary: {pct_burned_out_boundary}')
        return False


def make_fire_spatialbundle(args):
    try:
        # Function takes in a tuple of args to allow for multiprocessing with multiprocessing.Pool
        fireid, year, wumi_data_dir, mtbs_sevraster_dir, wumi_projection, output_dir = args

        # locate the relevant WUMI subidr, MTBS tif file
        wumi_dir = os.path.join(wumi_data_dir, str(year), fire_id)
        mtbs_sev_tif = glob.glob(os.path.join(mtbs_sevraster_dir, f'*_{year}.tif'))
        assert len(mtbs_sev_tif) == 1, f'Found {len(mtbs_sev_tif)} tif files in {os.path.join(mtbs_sevraster_dir, str(year))}, when there should be exactly 1. \nExiting.'
        mtbs_sev_tif = mtbs_sev_tif[0]

        # output will be saved to {output_dir}/{wumi_fireid}/spatialinfo/
        fire_output_dir = f'{output_dir}/{fireid}/spatialinfo/'
        
        # copy WUMI MTBS polygon to new dir 
        # save with CRS info from provided projection
        f = glob.glob(os.path.join(wumi_dir, '*_mtbs*.shp'))
        assert len(f) == 1, f'Found {len(f)} shapefiles in {os.path.join(wumi_dir, '*_mtbs*.shp')}, when there should be exactly 1. \nExiting.'
        f = f[0]
        wumi_mtbs_shp_f = os.path.join(fire_output_dir, f'{fireid}_wumi_mtbs_poly.shp')
        (
            gpd.read_file(f)
            .set_crs(wumi_projection)
            .to_file(wumi_mtbs_shp_f)
        )
        assert len(gpd.read_file(f))==1, f'{f} is a multipolygon. len({f})!=1. \nExiting.'

        # extract sevraster for this polygon and save to {output_dir}/{wumi_fireid}/spatialinfo/
        mtbs_full = rxr.open_rasterio(mtbs_sev_tif)
        fire_sev_tif = clip_raster_to_poly(rxr_obj=mtbs_full, poly_path=wumi_mtbs_shp_f)
        del mtbs_full
        gc.collect()

        assert confirm_burned(fire_sev_tif, gpd.read_file(wumi_mtbs_shp_f)), f'ERROR: {fire_sev_tif} does not meet the confirm_burned criteria.'

        print(f'Successfully extracted {fireid}. Saving to {fire_sev_tif}.', flush=True)
        export_to_tiff(
            fire_sev_tif, 
            os.path.join(fire_output_dir, f'{fireid}_burnsev.tif'),
            'int8',
            -128
        )

        return f'SUCCESS: {fireid}'
    
    except Exception as e:
        error_message = f'ERROR processing {fireid}: {e}'
        print(error_message, flush=True)
        return error_message


def make_fire_bundles_parallel(
        fireid_years_list: list, 
        wumi_data_dir: str, 
        mtbs_sevraster_dir: str, 
        wumi_projection: pyproj.CRS, 
        output_dir: str,
        n_processes: int
    ) -> None:
    
    # Prepare args for each fire
    args_list = [
        (fireid, year, wumi_data_dir, mtbs_sevraster_dir, wumi_projection, output_dir)
        for fireid, year in fireid_years_events
    ]

     # Process fires in parallel
    with Pool(processes=n_processes) as pool:
        results = pool.map(make_fire_spatialbundle, args_list)
    
    # Print summary
    successes = [r for r in results if r.startswith("SUCCESS")]
    errors = [r for r in results if r.startswith("ERROR")]
    
    print(f"\nProcessing complete!")
    print(f"Successful: {len(successes)}")
    print(f"Failed: {len(errors)} {'\n'.join(errors)}")


if __name__ == '__main__':
    print(f'Running make_mtbs_bundles.py with arguments {'\n'.join(sys.argv)}\n')
    
    # INPUTS
    subfires_csv = sys.argv[1]  # list from wumi containing fire_id, mtbs_id, year for all subfires
    wumi_projection_raster = sys.argv[2]
    wumi_data_dir = sys.argv[3] # where WUMI shapefiles are stored
    mtbs_sevraster_dir = sys.argv[4]    # where MTBS annual severity rasters are stored
    start_year, end_year = int(sys.argv[5]), int(sys.argv[6]) # years range of fires to be considered for analysis
    output_dir = sys.argv[7]    # where to store the clipped sev rasters + copy of shapefiles
    done_flag = sys.argv[8]
    n_processes = sys.argv[9]

    # FILTER -- LIST OF ALL MTBS SUBFIRES IN CA FOR DESIRED YEARS
    fireid_years_events, total_count = get_wumi_id_years(subfires_csv, start_year, end_year)

    # CREATE SPATIAL INFO BUNDLE FOR EACH FIRE TO PROCESS
    wumi_projection = rxr.open_rasterio(wumi_projection_raster).rio.crs

    make_fire_bundles_parallel(
        list(fireid_years_events), 
        wumi_data_dir, 
        mtbs_sevraster_dir, 
        wumi_projection,
        output_dir,
        n_processes
    )

    # DONE FLAG
    subprocess.run(['touch', done_flag])
    



