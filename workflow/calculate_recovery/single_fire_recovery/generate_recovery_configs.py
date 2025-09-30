import yaml, json, sys, subprocess, os, glob
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

        # Get ROI path for formatting output paths
        if config_data['TESTING']: ROI_PATH=config_data['TEST_ROI']
        else: ROI_PATH=config_data['ROI']
        
        # Save relevant parts of config
        out_data['RECOVERY_PARAMS'] = config_data['RECOVERY_PARAMS']
        out_data['LANDSAT'] = config_data['LANDSAT']
        out_data['BASELAYERS'] = config_data['BASELAYERS']
        out_data['SENSITIVITY_ANALYSIS'] = config_data['SENSITIVITY_ANALYSIS']
        out_data['SENSITIVITY'] = config_data['SENSITIVITY']

        # Update file paths
        for key in ['RECOVERY_MAPS_DIR', 'RECOVERY_PLOTS_DIR', 'RECOVERY_CONFIGS', 'LOGGING_PROCESS_CSV']:
            out_data['RECOVERY_PARAMS'][key] = get_path(config_data['RECOVERY_PARAMS'][key], ROI_PATH)
        
        out_data['LANDSAT']['dir_name'] = get_path(config_data['LANDSAT']['dir_name'], ROI_PATH)

        for layer in config_data['BASELAYERS']:
            out_data['BASELAYERS'][layer]['fname'] = get_path(config_data['BASELAYERS'][layer]['fname'], ROI_PATH)
        out_data['BASELAYERS']['groupings']['summary_csv'] = get_path(config_data['BASELAYERS']['groupings']['summary_csv'], ROI_PATH)
        
        out_data['SENSITIVITY']['plots_dir'] = get_path(config_data['SENSITIVITY']['plots_dir'], ROI_PATH)
        
    with open(out_path, 'w') as f:
        json.dump(out_data, f, indent=4)

    return out_data


def get_fire_metadata(config, ROI_PATH, fireinfo, sensitivity_fireids):
    return {
        'FIRE_NAME': fireinfo['name'],
        'FIRE_HA': fireinfo['burn_area_ha'],
        'FIRE_DATE': f'{fireinfo['year']}-{str.zfill(str(fireinfo['month']),2)}-{str.zfill(str(fireinfo['day']),2)}',
        'FIRE_YEAR': fireinfo['year'],
        'FIRE_BOUNDARY_PATH': get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_MAPS_DIR']}{fireinfo['name']}_{fireinfo['fireid']}/spatialinfo/', ROI_PATH),
        'SENSITIVITY_ANALYSIS': fireinfo['fireid'] in sensitivity_fireids
    }


def get_file_paths(config, ROI_PATH, fireinfo):
    prefix = f'{fireinfo['name']}_{fireinfo['fireid']}'
    maps_fire_dir = get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_MAPS_DIR']}{prefix}/', ROI_PATH)
    print(f'fireid folder: {prefix}')
    print(maps_fire_dir)
    return {
        'INPUT_LANDSAT_DATA_DIR': get_path(f'{config['LANDSAT']['dir_name']}unmerged_scenes/{prefix}/', ROI_PATH),
        'INPUT_LANDSAT_SEASONAL_DIR': get_path(f'{config['LANDSAT']['dir_name']}seasonal/{prefix}/', ROI_PATH),
        'OUT_MAPS_DATA_DIR_PATH': maps_fire_dir,
        'OUT_MERGED_NDVI_NC': f'{maps_fire_dir}{prefix}_merged_ndvi.nc',
        'OUT_MERGED_THRESHOLD_NC': f'{maps_fire_dir}{prefix}_merged_threshold_ndvi.nc',
        'OUT_SUMMARY_CSV': f'{maps_fire_dir}{prefix}_time_series_summary_df.csv',
        'RECOVERY_COUNTS_SUMMARY_CSV': f'{maps_fire_dir}{prefix}_grouping_counts_recovery_summary.csv',
        'PLOTS_DIR': get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_PLOTS_DIR']}{prefix}/', ROI_PATH),
        'BASELAYERS': {
            'severity': next(iter(glob.glob(get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_MAPS_DIR']}{prefix}/spatialinfo/*_burnsev.tif', ROI_PATH))), None),
            'agdev_mask': get_path(config['BASELAYERS']['agdev_mask']['fname'], ROI_PATH),
            'annual_dist':get_path(config['BASELAYERS']['annual_dist']['fname'], ROI_PATH),
            'groupings': get_path(config['BASELAYERS']['groupings']['fname'], ROI_PATH),
            'groupings_summary_csv': get_path(config['BASELAYERS']['groupings']['summary_csv'], ROI_PATH)
        },
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
                 f'{maps_fire_dir}{prefix}_{config['RECOVERY_PARAMS']['MIN_SEASONS']}seasons_recovery.tif',
                'int32', -9999),
            'prefire_baseline_recovery_time': (
                 f'{maps_fire_dir}{prefix}_{config['RECOVERY_PARAMS']['MIN_SEASONS']}seasons_prefire_baseline_recovery.tif',
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
            'prefire_ndvi_baseline': (
                 f'{maps_fire_dir}{prefix}_prefire_ndvi_baseline.tif',
                'float32', -1)
        }
    }


def create_perfire_config_json(config_path, out_path, sensitivity_fireids):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
        
        # Get ROI path for formatting output paths
        if config['TESTING']: ROI_PATH=config['TEST_ROI']
        else: ROI_PATH=config['ROI']

        # Get filtered wumi data path
        wumi_data_path = get_path(f'{config['RECOVERY_PARAMS']['RECOVERY_CONFIGS']}wumi_data.csv', ROI_PATH)
        wumi_data = pd.read_csv(wumi_data_path)
        # Get fire metadata, using WUMI csv
        out_data = {
            fireinfo['fireid']: {
                'FIRE_METADATA': get_fire_metadata(config, ROI_PATH, fireinfo, sensitivity_fireids),
                'FILE_PATHS': get_file_paths(config, ROI_PATH, fireinfo)
                }
            for _, fireinfo in wumi_data.iterrows()
            }
            
    with open(out_path, 'w') as f:
        json.dump(out_data, f, indent=4)



if __name__ == '__main__':
    print(f'Running generate_recovery_configs.py with arguments {' '.join(sys.argv)}\n')
    config_path = sys.argv[1]
    main_config_out = sys.argv[2]
    perfire_config_out = sys.argv[3]
    done_flag = sys.argv[4]

    # Create main config file (across all fires)
    config = create_main_config_json(config_path, main_config_out)
    print('created main config')
    
    # Create per-fire file paths and fire metadata dict
    sensitivity_df = pd.read_csv(config['SENSITIVITY']['output_sensitivity_selected_csv'])
    sensitivity_fireids = sensitivity_df.loc[sensitivity_df['sensitivity_selected'], 'fireid']
    create_perfire_config_json(config_path, perfire_config_out, sensitivity_fireids)
    print('created perfire config')

    # # DONE FLAG
    os.makedirs(os.path.dirname(done_flag), exist_ok=True)
    subprocess.run(['touch', done_flag])
    print('created done flag')