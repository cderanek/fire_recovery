import numpy as np
import pandas as pd
import rioxarray as rxr
import os, sys, yaml
import seaborn as sns
import matplotlib.pyplot as plt

from plot_helpers import create_static_fire_map


def count_fire_size(uid_h_data, uid_to_data, recovery_csv, wumi_csv):
    # using V1 recovery maps, count # of pixels per fire id
    fire_ids = np.full(uid_h_data.shape, np.nan) # create empty array to hold all fire ids
    fire_ids = np.where(
        (uid_h_data != -128) & (uid_to_data != -128), # where not nodata
        (100*uid_h_data.astype('int16')) + uid_to_data.astype('int16'), # hundreds place + tens/ones place
        fire_ids    # else, nans
    ).flatten()

    uids, counts = np.unique(fire_ids, return_counts=True, equal_nan=True)
    counts_df = pd.DataFrame({'uid': uids, 'counts': counts}).dropna()

    # merge counts_df, fire id csv, wumi csv to get the WUMI UID for each MTBS UID
    recovery_csv['mtbs_ID'] = recovery_csv['fire_id'].apply(lambda s: s.split('_')[-1].lower())
    counts_df = pd.merge(counts_df, recovery_csv, on='uid')[['uid', 'mtbs_ID', 'counts']]
    counts_df = pd.merge(counts_df, wumi_csv, on='mtbs_ID', how='right').dropna(subset='uid')[['uid', 'mtbs_ID', 'fireid', 'name', 'year', 'lat', 'lon', 'burn_area_ha', 'cause_human_or_natural', 'cause_specific', 'counts']]

    return counts_df


if __name__ == '__main__':
    ### Read in config ###
    config_path = sys.argv[1]
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    if os.path.exists(config['SENSITIVITY']['output_sensitivity_selected_csv']):
        fire_counts_df = pd.read_csv(config['SENSITIVITY']['output_sensitivity_selected_csv'])
    
    else:
        ### Filter to fires with >1500 valid pixels in the final recovery map ###
        # Some fires occurred primarily in developed/ag lands, or were later largely reburned, 
        # meaning they don't appear in the final maps
        uid_h_data = rxr.open_rasterio(config['SENSITIVITY']['merged_recovery_fireids_h']).data
        uid_to_data = rxr.open_rasterio(config['SENSITIVITY']['merged_recovery_fireids_to']).data
        recovery_csv = pd.read_csv(config['SENSITIVITY']['merged_recovery_fireids_csv'])
        wumi_csv = pd.read_csv(config['WUMI_PRODUCTS']['subfires_csv_f'])
        fire_counts_df = count_fire_size(uid_h_data, uid_to_data, recovery_csv, wumi_csv)

        ### Randomly select 100 large enough fires and add sensitivity indicator col to wumi_csv ###
        eligible_ids = np.unique(fire_counts_df.loc[fire_counts_df['counts']>5000, 'uid'])
        sensitivity_selected = np.random.choice(eligible_ids, size=100, replace=False)
        fire_counts_df['sensitivity_selected'] = fire_counts_df['uid'].apply(lambda uid: True if uid in sensitivity_selected else False)
        fire_counts_df.to_csv(config['SENSITIVITY']['output_sensitivity_selected_csv'])

    ### Plot the properties of selected fires, compared with all fires ###
    plots_dir = os.path.join(config['SENSITIVITY']['plots_dir'], 'sensitivity_fires_selection/')
    os.makedirs(plots_dir, exist_ok=True)
    
    # median lat/lon distribution of sampled vs all fires
    sns.scatterplot(data=fire_counts_df, x='lat', y='lon', hue='sensitivity_selected', alpha=0.2)
    sns.rugplot(data=fire_counts_df, x='lat', y='lon', hue='sensitivity_selected', alpha=0.2)
    plt.savefig(os.path.join(plots_dir, 'lat_lon_selected_pts.png'))
    plt.clf()

    # also map points over the state 
    create_static_fire_map(fire_counts_df, output_path=os.path.join(plots_dir, 'lat_lon_selected_pts_map.png'), figsize=(15, 12), dpi=300)
    
    # fire size distribution
    print(min(fire_counts_df['burn_area_ha']), max(fire_counts_df['burn_area_ha']))
    fire_counts_df['log_burn_area_ha'] = np.log10(fire_counts_df['burn_area_ha'])
    sns.kdeplot(data=fire_counts_df, x='log_burn_area_ha', hue='sensitivity_selected', common_norm=False)
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, 'log)burn_area_ha_selected_pts.png'))
    plt.clf()

    # years histogram
    fire_counts_df['year'] = fire_counts_df['year'].astype('int')
    sns.histplot(data=fire_counts_df, x='year', hue='sensitivity_selected', multiple='stack', bins=list(range(1984, 2024)))
    plt.tight_layout()
    plt.savefig(os.path.join(plots_dir, 'year_selected_pts.png'))
    plt.clf()