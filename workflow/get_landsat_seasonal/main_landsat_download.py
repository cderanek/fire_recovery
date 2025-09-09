  
import sys, os, glob, json
import pandas as pd
import filelock
import concurrent.futures
from functools import partial

from typing import Tuple, Union
NumericType = Union[int, float]
RangeType = Tuple[NumericType, NumericType]    

sys.path.append("../utils")
from geo_utils import buffer_firepoly
from earthaccess_downloads import download_landsat_data
from merge_process_landsat_scenes import mosaic_ndvi_timeseries



### Helper functions to process individual years, organize all years downloads, report results ###
def process_year(
    yr: int, 
    bufferedfireShpPath: str, 
    LS_DATA_DIR: str, 
    VALID_LAYERS: list,
    LS_SEASONAL_DIR: str,
    DEFAULT_NODATA: NumericType, 
    PRODUCT_LAYERS: dict
    ) -> bool:
    """
    Download/mosaic/QA mask 1 year of LS data for given fire poly. 
    Returns True if processed/saved with no errors.
    """
    
    try:
        num_seasons_mosaiced = len(glob.glob(LS_SEASONAL_DIR + '*' + str(yr) + '*_season_mosaiced.tif'))
        print(f'Num seasons already mosaiced for {yr}: {num_seasons_mosaiced} in dir: {LS_SEASONAL_DIR}', flush=True)
        
        skip_criteria = num_seasons_mosaiced==4
        if not skip_criteria:
            try:
                year_out_dir = download_landsat_data(
                    roi_path=bufferedfireShpPath, 
                    start_date=f'01-01-{yr}', 
                    end_date=f'12-31-{yr}',
                    product_layers=PRODUCT_LAYERS, 
                    out_dir=LS_DATA_DIR
                )
                
                mosaic_ndvi_timeseries(
                    year_out_dir, VALID_LAYERS, LS_SEASONAL_DIR, NODATA=DEFAULT_NODATA, 
                    MAKE_RGB=False, MAKE_DAILY_NDVI=False, DELETE_ORIG=True
                )
                return True
                
            except Exception as e:
                try:
                    print(f'ATTEMPT 1/2 ERROR WITH SINGLE FIRE LANDSAT DOWNLOAD FOR YEAR {yr}: {e}', flush=True)
                    print('Retrying...')
                    year_out_dir = download_landsat_data(
                        roi_path=bufferedfireShpPath, 
                        start_date=f'01-01-{yr}', 
                        end_date=f'12-31-{yr}',
                        product_layers=PRODUCT_LAYERS, 
                        out_dir=LS_DATA_DIR
                    )
                    
                    mosaic_ndvi_timeseries(
                        year_out_dir, VALID_LAYERS, LS_SEASONAL_DIR, NODATA=DEFAULT_NODATA, 
                        MAKE_RGB=False, MAKE_DAILY_NDVI=False, DELETE_ORIG=False
                    )
                    
                    return True
                    
                except Exception as e:
                    print(f'ERROR WITH SINGLE FIRE LANDSAT DOWNLOAD FOR YEAR {yr}: {e}', flush=True)
                    return False
        else:
            return True
            
    except Exception as e:
        print(f'ERROR WITH SINGLE FIRE LANDSAT DOWNLOAD FOR YEAR {yr}: {e}', flush=True)
        return False


def process_all_years(
    YEARS_RANGE: RangeType, 
    bufferedfireShpPath: str, 
    LS_DATA_DIR: str, 
    VALID_LAYERS: list, 
    LS_SEASONAL_DIR: str,
    DEFAULT_NODATA: NumericType, 
    PRODUCT_LAYERS: dict,
    max_workers: int = 4
    ) -> dict:
    
    # set up partial (process_year is only param that changes for each year)
    process_year_partial = partial(
        process_year,
        bufferedfireShpPath=bufferedfireShpPath,
        LS_DATA_DIR=LS_DATA_DIR,
        VALID_LAYERS=VALID_LAYERS,
        LS_SEASONAL_DIR=LS_SEASONAL_DIR,
        DEFAULT_NODATA=DEFAULT_NODATA,
        PRODUCT_LAYERS=PRODUCT_LAYERS
    )
    
    # store results summary in dict
    results = {}
    
    # use conucrrent futures to have multiple threads processing years, checking in on download status, and reporting back 
    # (helpful b/c lots of downtime as we wait for AppEEARS download link to be ready)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # submit all years for processing
        future_to_year = {executor.submit(process_year_partial, yr): yr for yr in YEARS_RANGE}
        
        # update results summary as LS downloads finish
        for future in concurrent.futures.as_completed(future_to_year):
            yr = future_to_year[future]
            try:
                success = future.result()
                results[yr] = success
            except Exception as e:
                results[yr] = False
    
    return results


def report_results(results, organizer_csv):
    # list successful/unsuccessful years downloads
    successful_years = [yr for yr, success in results.items() if success]
    failed_years = [yr for yr, success in results.items() if not success]
    if len(failed_years) == 0: 
        download_status='Complete'
    else: 
        download_status='Failed'
    
    # update submissions organizer csv
    lock_file = organizer_csv + '.lock'
    lock = filelock.FileLock(lock_file, timeout=60)  # wait for lock if necessary (other batch jobs for other fires may also be waiting to update csv)
    try:
        with lock:
            csv = pd.read_csv(organizer_csv)
               
            # update row associated with just completed downloads
            mask = csv['fire_shpfile_path'] == args['fire_shp']
            csv.loc[mask, 'download_status'] = download_status
            csv.loc[mask, 'successful_years'] = str(successful_years)
            csv.loc[mask, 'failed_years'] = str(failed_years)
            
        # Save the updated csv
        csv.to_csv(args['ORGANIZER_CSV'], index=False)
        
    except filelock.Timeout:
        print("Could not acquire lock on file after waiting", flush=True)



if __name__ == "__main__":
    print(f'Running make_agdev_mask.py with arguments:')
    args = {
        'fire_shp': sys.argv[1],
        'LS_DATA_DIR': sys.argv[2],
        'LS_SEASONAL': sys.argv[3],
        'ORGANIZER_CSV': sys.argv[4],
        'VALID_LAYERS': json.loads(sys.argv[5]),
        'DEFAULT_NODATA': int(sys.argv[6]),
        'PRODUCT_LAYERS': json.loads(sys.argv[7]),
        'max_workers': int(sys.argv[8]),
    }
    if len(sys.argv) > 9: 
        args['YEARS_RANGE'] = range(int(sys.argv[9]), int(sys.argv[10]))
    else: 
        args['YEARS_RANGE'] = range(1982, 2025)

    for (key, val) in args.items():
        print(key, val, flush=True)
    
    # get buffered fire polygon for requesting Landsat data in a fire + 10km buffer
    bufferedfire_gdf, bufferedfireShpPath = buffer_firepoly(args['fire_shp'])
    
    # process all years with parallel workers (will skip any years that were successfully downloaded prior)
    results = process_all_years(
        YEARS_RANGE=args['YEARS_RANGE'],
        bufferedfireShpPath=bufferedfireShpPath,
        LS_DATA_DIR=args['LS_DATA_DIR'],
        VALID_LAYERS=args['VALID_LAYERS'],
        LS_SEASONAL_DIR=args['LS_SEASONAL'],
        DEFAULT_NODATA=args['DEFAULT_NODATA'],
        PRODUCT_LAYERS=args['PRODUCT_LAYERS'],
        max_workers=args['NUM_PARALLEL_WORKERS']
    )

    # Print, store results summary
    report_results(results, args['ORGANIZER_CSV'])