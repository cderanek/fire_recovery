import yaml, json, sys
import pandas as pd
import numpy as np
from functools import partial

sys.path.append('rules/')
from common import get_path

'''
needs main config for recovery params, landsat download params, wumi info
## Generate main recovery config -- not per-fire specific
Uses main config, drops all RECOVERY_PARAMS to json
Also adds in path to baselayers
Also adds in Landsat download params

## Generate per-fire recovery configs
Dict with keys = fireid
{
    fireid:
        fire_metadata:

        file_paths: 
}
'''

def create_main_config_json(config_path, out_path):
    out_data = {}
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
        out_data['RECOVERY_PARAMS'] = config_data['RECOVERY_PARAMS']
        out_data['LANDSAT'] = config_data['LANDSAT']
        
    with open(out_path, 'w') as f:
        json.dump(out_data, f, indent=4)


def get_fire_metadata(config, ROI_PATH, fireinfo):
    return {
        'FIRE_NAME': fireinfo['name'].values[0],
        'FIRE_HA': fireinfo['burn_area_ha'].values[0],
        'FIRE_DATE': np.datetime64(f'{fireinfo['year'].values[0].astype('str')}-{str.zfill(fireinfo['month'].values[0].astype('str'),2)}-{str.zfill(fireinfo['day'].values[0].astype('str'),2)}'),
        'FIRE_YEAR': fireinfo['year'].values[0],
        'FIRE_BOUNDARY_PATH': get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_MAPS_DIR']}{fireinfo['name'].values[0]}_{fireinfo['fireid'].values[0]}/spatialinfo/', ROI_PATH)
    }

def get_file_paths(config, ROI_PATH, fireinfo):
    prefix = f'{fireinfo['name'].values[0]}_{fireinfo['fireid'].values[0]}'
    maps_fire_dir = get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_MAPS_DIR']}{prefix}/', ROI_PATH)

    return {
        'DATA_DIR_PATH': maps_fire_dir,
        'OUT_MERGED_NDVI_NC': f'{maps_fire_dir}{prefix}_merged_ndvi.nc',
        'OUT_MERGED_THRESHOLD_NC': f'{maps_fire_dir}{prefix}_merged_threshold_ndvi.nc',
        'OUT_SUMMARY_CSV': f'{maps_fire_dir}{prefix}_time_series_summary_df.csv',
        'RECOVERY_COUNTS_SUMMARY_CSV': f'{maps_fire_dir}{prefix}_grouping_counts_recovery_summary.csv',
        'PLOTS_DIR': get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_PLOTS_DIR']}{prefix}/', ROI_PATH)
        'OUT_TIFS_D': {
            'severity': (
                f'{maps_fire_dir}{prefix}_severity.tif',
                'int8', -1),
            'dist_mask': (
                f'{maps_fire_dir}{prefix}_alldist_mask.tif',
                'int8', -1),
            'future_dist_agdev_mask': (
                f'{maps_fire_dir}{prefix}_postfire_dist_agdev.tif',
                'int8', -1),
            'past_dist_agdev_mask': (
                f'{maps_fire_dir}{prefix}_prefire_dist_agdev.tif',
                'int8', -1),
            'fire_recovery_time': (
                 f'{maps_fire_dir}{prefix}_{config['MIN_SEASONS']}seasons_recovery.tif',
                'int32', -9999),
            'elevation': (
                 f'{maps_fire_dir}{prefix}_elevation.tif',
                'int32', -9999),
            'evt': (
                 f'{maps_fire_dir}{prefix}_evt.tif',
                'int16', -9999),
            'groups': (
                 f'{maps_fire_dir}{prefix}_groups.tif',
                'int32', -9999),
            'temporal_coverage_qa': (
                 f'{maps_fire_dir}{prefix}_temporal_coverage_qa.tif',
                'int8', -1),
            'matched_group_temporal_coverage_qa': (
                 f'{maps_fire_dir}{prefix}_matched_group_temporal_coverage_qa.tif',
                'int8', -1),
        }
    }


def create_perfire_config_json(config_path, fireslist_txt, out_path):
    out_data = {}
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
        
        # Get ROI path for formatting output paths
        if config['TESTING']: ROI_PATH=config['TEST_ROI']
        else: ROI_PATH=config['ROI']

        # Get filtered wumi data path
        wumi_data_path = get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}wumi_data.csv', ROI_PATH)

        # Get fire metadata, using WUMI csv
        out_data[fireinfo['fireid'].values[0]] = {
            {'FIRE_METADATA': get_fire_metadata(config, ROI_PATH, fireinfo),
            'FILE_PATHS': get_file_paths(config, ROI_PATH, fireinfo)}
            for fireinfo in wumi_data
            }

    with open(out_path, 'w') as f:
        json.dump(out_data, f, indent=4)



if __name__ == '__main__':
    print(f'Running generate_recovery_configs.py with arguments {'\n'.join(sys.argv)}\n')
    config_path = sys.argv[1]
    fireslist_txt = sys.argv[2]
    main_config_out = sys.argv[3]
    perfire_config_out = sys.argv[4]
    done_flag = sys.argv[5]

    # Create main config file (across all fires)
    create_main_config_json(config_path, main_config_out)
    
    # Create per-fire file paths and fire metadata dict
    create_perfire_config_json(config_path, fireslist_txt, perfire_config_out)

    # # DONE FLAG
    # subprocess.run(['touch', done_flag])