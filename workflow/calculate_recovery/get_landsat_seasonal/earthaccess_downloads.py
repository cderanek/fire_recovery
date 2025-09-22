"""
EarthAccess API downloader for Landsat data. Made with lots of help from Erik Bolch, USGS EROS

USGS EROS tutorial:
https://github.com/nasa/AppEEARS-Data-Resources
"""

import sys, os, re, time, requests, json
import earthaccess
import geopandas as gpd
import numpy as np
from netrc import netrc


APPEEARS_API_ENDPOINT = 'https://appeears.earthdatacloud.nasa.gov/api/'
SLEEP_TIME = 60*2 # 2min pause between requests


def login_earthaccess():
    try:
        # Login to EarthAccess
        earthaccess.login(persist=True)
        
        # Get token information and store in header
        urs = 'urs.earthdata.nasa.gov'
        token_response = requests.post(
            f'{APPEEARS_API_ENDPOINT}login', 
            auth=(netrc().authenticators(urs)[0], 
                netrc().authenticators(urs)[2])
        ).json()
        token = token_response['token']
        head = {'Authorization': f'Bearer {token}'}
        
        return head
    
    except Exception as e:
        print(f'Error logging into EarthAccess. Error: {e}', flush=True)
        time.sleep(SLEEP_TIME)
        login_earthaccess() # Retry until we can login
        return None


def stream_bundle_file(task_id, head, f, max_retries=30):
    failures=0
    while True:
        try:
            # Request bundle
            dl = requests.get(f'{APPEEARS_API_ENDPOINT}bundle/{task_id}/{f}', 
                                headers=head,
                                stream=True,
                                allow_redirects = 'True') # Get a stream to the bundle file
            return dl
            
        except Exception as e:
            failures+=1
            print(f'Request to stream bundle file for {f} failed {failures}/{max_retries} times. Error: {e}', flush=True)
            
            if failures >= max_retries: 
                print(f'Request to stream bundle file for {f} failed {failures}/{max_retries} times. Deleting job {task_id}.', flush=True)
                response = requests.delete(
                    f'{APPEEARS_API_ENDPOINT}task/{task_id}', 
                    headers=head)
                print(f'Response code for deletion: {response.status_code}', flush=True)
                return False
            else: time.sleep(SLEEP_TIME)    


def try_get_bundle_once(task_id, head):
    try:
        # Request bundle
        bundle = requests.get('{}bundle/{}'.format(APPEEARS_API_ENDPOINT,task_id), headers=head).json()  # Call API and return bundle contents for the task_id as json
        return bundle
            
    except Exception as e:
        print(f'Request for bundle with task_id {task_id} failed', flush=True)
        return np.nan    


def post_request(task_json, head, max_retries=30):
    failures=0
    while True:
        try:
            # Post request
            task_response = requests.post('{}task'.format(APPEEARS_API_ENDPOINT), json=task_json, headers=head).json()
            print(task_response, flush=True)
            task_id = task_response['task_id']
            return task_id
            
        except Exception as e:
            failures+=1
            print(f'Request failed {failures}/{max_retries} times. Error: {e}', flush=True)
            
            if failures >= max_retries: 
                print(f'Request in post_request failed {failures}/{max_retries} times. Exiting.', flush=True)
                return False
            else: time.sleep(SLEEP_TIME)           


def ping_appears_once(task_id, head):
    try:
        response = requests.get(f'{APPEEARS_API_ENDPOINT}task/{task_id}', headers=head).json()['status'] 
        if response == 'done':
                print(f'Finished processing {task_id}.', flush=True)
                return True

        else: return False
    
    except Exception as e:
        print(f'Request {task_id} in ping_appears failed. Error: {e}', flush=True)

    
def download_landsat_bundle(bundle, task_id, head, dest_dir):
    try:
        # Fill dictionary with file_id as keys and file_name as values
        if type(bundle)==type('s'): bundle = json.loads(bundle)
        files = {f['file_id']: f['file_name'] for f in bundle['files']}
        
        # Iterate over all files in bundle, downloading all tif & nc files
        for fileid, filename in files.items():
            if ('tif' in filename) or ('nc' in filename):
                dl = stream_bundle_file(task_id, head, fileid)
                
                # Create dir to store downloaded data, if it doesn't exist
                if filename.endswith('.tif'):
                    filename = filename.split('/')[1]
                else:
                    filename = filename
                filepath = os.path.join(dest_dir, filename)
                os.makedirs(dest_dir, exist_ok=True)
                
                # Write data 
                with open(filepath, 'wb') as f: 
                    for data in dl.iter_content(chunk_size=8192): f.write(data) 
        print('Downloaded files can be found at: {}'.format(dest_dir), flush=True)
        
        return dest_dir
    
    except Exception as e:
        print(f'Error downloading files for {dest_dir}. Error: {e}', flush=True)
        return np.nan


def create_product_request_json(task_name: str, start_date:str, end_date:str, shp_file_path:str, product_layers:dict, file_type:str='geotiff'):
    """
    Create a JSON request for the AppEEARS API.
    
    Parameters:
        task_name (str): Name for the task
        start_date (str): Start date in format 'MM-DD-YYYY'
        end_date (str): End date in format 'MM-DD-YYYY'
        shp_file_path (str): Path to shapefile defining the region
        product_layers (dict): Dictionary of products and their layers
        file_type (str): Output file type (default: 'geotiff')
        
    Returns:
        dict: JSON-formatted request for earth access API
    """
    # Format list of coords from shp file
    coords_curr = list(gpd.read_file(shp_file_path).to_crs(4326).geometry.get_coordinates().values)
    coords_curr = [list(coords) for coords in coords_curr]
    if coords_curr[-1] != coords_curr[0]: coords_curr.append(coords_curr[0])
    
    # Format product layers for earth access json
    prodLayer = list(np.array([[
        {"layer": band, "product": product} for band in product_layers[product]
    ] for product in product_layers]).flatten())

    task_json = {
        'task_type': 'area',
        'task_name': task_name,
        "params": {
            "geo": {
                "type": "FeatureCollection", 
                "features": [{
                    "type": "Feature", 
                    "geometry": {
                        "type": "Polygon", 
                        "coordinates": [coords_curr]
                    }, 
                    "properties": {}
                }], 
                "fileName": "User-Drawn-Polygon"
            }, 
            "dates": [{
                "endDate": end_date, 
                "recurring": False, 
                "startDate": start_date, 
                "yearRange": [1982, 2026]}], 
            "layers": prodLayer, 
            "output": {
                "format": {"type": file_type}, 
                "projection": "native", 
                "additionalOptions": {"orthorectify": True}
            }
        }
    }

    return task_json