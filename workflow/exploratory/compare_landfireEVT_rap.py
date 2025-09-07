import pandas as pd
import xarray as xr
import rioxarray as rxr
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

import sys, glob, os, gc
sys.path.append("workflow/utils")
from geo_utils import reproj_align_rasters
sys.path.append("/u/project/eordway/shared/surp_cd/timeseries/visualization")
from color_palettes import veg_color_palette

'''
This script creates KDEs showing the distribution of RAP % cover for each RAP band for each EVT class. 
Note that this is a messier testing/preliminary exploration script (not part of make workflow), and contains
paths to data in my own file system. To recreate these figures, download LANDFIRE/RAP data for your ROI and 
update file paths or reach out to caderanek@g.ucla.edu for data used here.

The analysis is done for CA, in the year 2020 for RAP and EVT, masking ag/dev land

RAP bands:
Band 1 - annual forb and grass
Band 2 - bare ground
Band 3 - litter
Band 4 - perennial forb and grass
Band 5 - shrub
Band 6 - tree

EVT groups from /u/project/eordway/shared/surp_cd/timeseries_data/data/CA_wide_readonly/cawide_testing/annual_evt_groupings_fullCA500m_withagrdevmask.tif.vat.dbf:
,UPDATED_CLASS_VALUE,CLASS_NAME,Color
1 Closed tree canopy
2 Dwarf-shrubland
3 Herbaceous - grassland
4 Herbaceous - shrub-steppe
5 NA 
6 No Dominant Lifeform 
7 Non-vegetated 
8 Open tree canopy 
9 Shrubland 
10 Sparse tree canopy 
11 Sparsely vegetated
'''

## GLOBALS
evt_raster = '/u/project/eordway/shared/surp_cd/timeseries_data/data/CA_wide_readonly/cawide_testing/annual_evt_groupings_fullCA500m_withagrdevmask.nc' #'/u/project/eordway/shared/surp_cd/timeseries_data/data/CA_wide_readonly/annual_evt_class_groupings_wdtsclipped500m_withagrdevmask.nc'
rap_types = ['annual_forb_grass','perennial_forb_grass', 'shrub', 'tree']
rap_rasters_pattern = '/u/project/eordway/shared/surp_cd/fire_recovery/data/baselayers/temp/RAP/vegetation-cover-v3-2020_*.tif'
os.makedirs('/u/project/eordway/shared/surp_cd/fire_recovery/data/baselayers/downloadlogs_metadata/RAP/', exist_ok=True)
summary_csv_out = '/u/project/eordway/shared/surp_cd/fire_recovery/data/baselayers/downloadlogs_metadata/RAP/summary_rap_evt_counts.csv'#'/u/project/eordway/shared/surp_cd/fire_recovery/data/baselayers/downloadlogs_metadata/RAP/summary_rap_evt_counts_jointdistr.csv'
fig_out = '/u/project/eordway/shared/surp_cd/fire_recovery/results/rap_exploratory_analysis/'
JOINT_DISTR = False

evt_groups_dict = {
    1.0: 'Closed tree canopy',
    2.0: 'Dwarf-shrubland',
    3.0: 'Herbaceous - grassland',
    4.0: 'Herbaceous - shrub-steppe',
    6.0: 'No Dominant Lifeform',
    7.0: 'Non-vegetated',
    8.0: 'Open tree canopy',
    9.0: 'Shrubland',
    10.0: 'Sparse tree canopy',
    11: 'Sparsely vegetated'
}

if not os.path.exists(summary_csv_out):
    ## Read data
    # only get relevant year of LANDFIRE EVT data
    evt_da = (
        xr.open_dataset(evt_raster, format='NETCDF4', engine='netcdf4', chunks='auto')[['evt','spatial_ref']]
        .sel(time=slice('2020-01-01', '2020-12-31'))
        ).squeeze()
    evt_crs = evt_da['spatial_ref'].attrs['crs_wkt']
    evt_da = evt_da['evt'].rio.write_crs(evt_crs)
    evt_da.data[:] = np.where(evt_da.data==-9999, -128, evt_da.data)
    evt_da.data[:] = np.where(np.isnan(evt_da.data), -128, evt_da.data)
    evt_da = evt_da.astype('int8')

    ## Write to summary CSV for each EVT subclass, RAP veg type
    ## Summary CSV format, each row is a non-ag/dev pixel: | EVT_code | EVT_name | RAP_type | percent_cover |
    # create list to hold all rows
    summary_rows = []
    summary_df_exists = False

    # apply ag_dev mask, no EVT mask
    evt_valid_mask = (evt_da['ag_dev_mask']==0) & (evt_da.data > 0) & (evt_da.data < 12) & (evt_da.data != 5)

    # for each RAP veg type, update mask and group by EVT
    for rap_name in rap_types:
        print(f'Processing {rap_name}.')
        # Open and format rap tif
        rap_f = rap_rasters_pattern.replace('*', rap_name)
        rap_da = rxr.open_rasterio(rap_f).squeeze()

        ## Align RAP and LANDFIRE data
        evt_da, rap_da = reproj_align_rasters('reproj_match', evt_da, rap_da)

        # Get percent cover summary for this rap type across all EVTs
        print(f'Summarizing percent cover data for {rap_name}')

        # update valid mask for this rap type
        valid_mask = evt_valid_mask & (rap_da.data >= 0) & (rap_da.data <= 100)

        # flatten data
        rap_values = rap_da.where(valid_mask).values.flatten().astype('int8')
        evt_values = evt_da.where(valid_mask).values.flatten().astype('int8')
        print(np.min(rap_values), np.max(rap_values))
        print(len(rap_values), len(evt_values))

        if not JOINT_DISTR:
            # pd df to hold evtXrap data
            temp_df = pd.DataFrame({
                    'EVT': evt_values,
                    'percent_cover': rap_values
                })

            # summarize # of pixels for each unique percent cover, EVT type
            grouped = temp_df.groupby(['EVT', 'percent_cover']).size().reset_index()
            grouped.columns = ['EVT', 'percent_cover', 'pixel_count']
            grouped['RAP_type'] = rap_name
            grouped = grouped[['EVT', 'RAP_type', 'percent_cover', 'pixel_count']]

            # append to main list of rows
            summary_rows.append(grouped)

            print(f'Finished summarizing percent cover data for {rap_name}')

        if JOINT_DISTR:
            # pd df to hold evtXrap data
            if not summary_df_exists:
                summary_df = pd.DataFrame({
                        'EVT': evt_values,
                        f'{rap_name}_percent_cover': rap_values
                    }).astype('int8')
                summary_df_exists = True
            else:
                summary_df[f'{rap_name}_percent_cover'] = rap_values

        del evt_values, rap_values, rap_da
        gc.collect()

    if not JOINT_DISTR:  
        # Combine all RAP types
        print("\nCombining results...")
        summary_df = pd.concat(summary_rows, ignore_index=True)

        # Sort by EVT, RAP_type, percent_cover
        summary_df = summary_df.sort_values(['EVT', 'RAP_type', 'percent_cover']).reset_index(drop=True)

        print(summary_df)
        summary_df = summary_df[(summary_df['percent_cover'] != 0) & (summary_df['EVT'] != 0)]
        summary_df['EVT'] = summary_df['EVT'].map(evt_groups_dict)
        summary_df.to_csv(summary_csv_out)

    if JOINT_DISTR:
        # summarize # of pixels for each unique percent cover, EVT type
        print(summary_df.columns.tolist())
        print(summary_df)
        for evt in np.unique(summary_df['EVT']):
            if evt != 0:
                summary_df_subset = summary_df[summary_df['EVT'] == evt]
                grouped = summary_df_subset.astype('int8').groupby(summary_df_subset.columns.tolist()).size().reset_index()
                grouped.columns = summary_df.columns.tolist() + ['pixel_count']
                print(grouped.columns.tolist())
                # grouped.sort_values(summary_df.columns)
                grouped['EVT'] = grouped['EVT'].map(evt_groups_dict)
                print(grouped)
                grouped.to_csv(summary_csv_out.replace('.csv', f'_{evt_groups_dict[evt]}.csv'))
    
    
## Create histograms

## NOTE: Claude generated this function -- not in final analysis, just for early exploration of RAP vs EVT veg types
def create_evt_rap_visualization(df, out_f):
    # Get unique values
    unique_evts = sorted(df['EVT'].unique())
    unique_raps = df['RAP_type'].unique()
    
    # Separate forb grass types from other RAP types
    forb_grass_types = ['annual_forb_grass', 'perennial_forb_grass']
    other_raps = [rap for rap in unique_raps if rap not in forb_grass_types]
    
    # Set up color palette
    # Special green shades for forb grass types
    color_map = {
        'annual_forb_grass': '#2d5016',      # Dark green
        'perennial_forb_grass': '#7cb342'    # Light green
    }
    
    # Other colors for remaining RAP types
    if other_raps:
        other_colors = sns.color_palette("husl", n_colors=len(other_raps))
        for i, rap in enumerate(other_raps):
            color_map[rap] = other_colors[i]
    
    # Calculate figure size - forb grasses share top row, others get individual rows
    n_evts = len(unique_evts)
    n_subplot_rows = 1 + len(other_raps)  # 1 for combined forb grasses + individual rows for others
    fig_width = max(12, n_evts * 4)
    fig_height = max(8, n_subplot_rows * 3)
    
    # Create subplots
    fig, axes = plt.subplots(n_subplot_rows, n_evts, 
                            figsize=(fig_width, fig_height),
                            sharex=True, sharey=False)
    
    # Handle case where there's only one EVT or one subplot row
    if n_evts == 1 and n_subplot_rows == 1:
        axes = [[axes]]
    elif n_evts == 1:
        axes = [[ax] for ax in axes]
    elif n_subplot_rows == 1:
        axes = [axes]
    
    # Create plots
    for evt_idx, evt in enumerate(unique_evts):
        # Top row: Stacked forb grass types
        ax = axes[0][evt_idx]
        
        # Get data for both forb grass types for this EVT
        annual_data = df[(df['EVT'] == evt) & (df['RAP_type'] == 'annual_forb_grass')]
        perennial_data = df[(df['EVT'] == evt) & (df['RAP_type'] == 'perennial_forb_grass')]
        
        # Get all percent cover values for this EVT from both forb grass types
        all_percent_covers = sorted(set(list(annual_data['percent_cover']) + list(perennial_data['percent_cover'])))
        
        if all_percent_covers:
            # Prepare data for stacked bar chart
            annual_counts = []
            perennial_counts = []
            
            for pct in all_percent_covers:
                annual_val = annual_data[annual_data['percent_cover'] == pct]['pixel_count'].sum()
                perennial_val = perennial_data[perennial_data['percent_cover'] == pct]['pixel_count'].sum()
                annual_counts.append(annual_val)
                perennial_counts.append(perennial_val)
            
            # Create stacked bar plot
            x_pos = range(len(all_percent_covers))
            ax.bar(x_pos, annual_counts, color=color_map['annual_forb_grass'], 
                   label='Annual Forb Grass', alpha=0.8)
            ax.bar(x_pos, perennial_counts, bottom=annual_counts, 
                   color=color_map['perennial_forb_grass'], 
                   label='Perennial Forb Grass', alpha=0.8)
            
            # Customize x-axis
            n_ticks = min(11, len(all_percent_covers))
            tick_positions = np.linspace(0, len(all_percent_covers)-1, n_ticks, dtype=int)
            tick_labels = [all_percent_covers[i] for i in tick_positions]
            ax.set_xticks(tick_positions)
            ax.set_xticklabels(tick_labels, rotation=45)
        
        # Set title for top row
        ax.set_title(f'LANDFIRE EVT {evt}', fontsize=12, fontweight='bold')
        
        if evt_idx == 0:
            ax.set_ylabel('Forb Grass\nPixel Count', fontsize=10)
        else:
            ax.set_ylabel('')
        
        # Add legend only to the first subplot of top row
        if evt_idx == 0:
            ax.legend(loc='upper right', fontsize=8)
        
        # Format y-axis and add grid
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1e6:.1f}M' if x >= 1e6 else f'{x/1e3:.0f}K'))
        ax.grid(True, alpha=0.3)
        
        # Other RAP types in individual rows
        for rap_idx, rap in enumerate(other_raps):
            ax = axes[rap_idx + 1][evt_idx]  # +1 because top row is for forb grasses
            
            # Filter data for this EVT and RAP type
            subset = df[(df['EVT'] == evt) & (df['RAP_type'] == rap)]
            
            if not subset.empty:
                # Create barplot
                sns.barplot(data=subset, 
                           x='percent_cover', 
                           y='pixel_count',
                           color=color_map[rap],
                           ax=ax)
                
                # Customize x-axis to show fewer labels for readability
                n_ticks = min(11, len(subset))
                tick_positions = np.linspace(0, len(subset)-1, n_ticks, dtype=int)
                tick_labels = subset['percent_cover'].iloc[tick_positions].values
                ax.set_xticks(tick_positions)
                ax.set_xticklabels(tick_labels, rotation=45)
            
            # Set labels
            if evt_idx == 0:  # Left column
                ax.set_ylabel(f'{rap}\nPixel Count', fontsize=10)
            else:
                ax.set_ylabel('')
            
            if rap_idx == len(other_raps) - 1:  # Bottom row
                ax.set_xlabel('Percent Cover', fontsize=10)
            else:
                ax.set_xlabel('')
            
            # Format y-axis for better readability
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1e6:.1f}M' if x >= 1e6 else f'{x/1e3:.0f}K'))
            
            # Add grid for better readability
            ax.grid(True, alpha=0.3)
    
    # Adjust layout
    plt.tight_layout()
    
    # Add overall title
    # fig.suptitle('Pixel Count by Percent Cover for Each EVT and RAP Type', 
    #              fontsize=16, fontweight='bold', y=1.02)
    
    # Create legend
    legend_elements = [plt.Rectangle((0,0),1,1, facecolor=color_map[rap], label=rap) 
                      for rap in unique_raps]
    fig.legend(handles=legend_elements, 
              title='RAP categories',
              loc='center right', 
              bbox_to_anchor=(0.98, 0.5),
              ncol=1,#len(unique_raps),
              fontsize=10)

    plt.savefig(out_f)


def plot_joint_density(df, x_col, y_col, out_f):
    """
    Create a simple joint density plot with marginal distributions, weighted by pixel counts.
    
    Parameters:
    df (DataFrame): Your data
    x_col (str): Column name for x-axis 
    y_col (str): Column name for y-axis
    weight_col (str): Column name for weights (default: 'pixel_count')
    """
    # Expand data based on pixel counts
    sns.jointplot(data=df, x=x_col, y=y_col, hue='EVT', kind='hex', palette=veg_color_palette)
    plt.xlim(0,100)
    plt.ylim(0,100)
    plt.savefig(out_f)
    plt.clf()


if not JOINT_DISTR:
    summary_df = pd.read_csv(summary_csv_out)
    print(summary_df)
    summary_df['EVT'] = summary_df['EVT'].map(evt_groups_dict)
    summary_df = summary_df.dropna()
    print(summary_df)
    for evt in np.unique(summary_df['EVT']):
        summary_df_subset = summary_df[summary_df['EVT'] == evt]
        f = f'{fig_out}{evt}.png'
        create_evt_rap_visualization(summary_df_subset, f)

if JOINT_DISTR:
    sampled_rows = []
    for evt in evt_groups_dict.values():
        try:
            if (evt != 'Non-vegetated') & (evt != 'Sparsely vegetated'):
                # sample rows proportional to number of pixels
                summary_df = pd.read_csv(summary_csv_out.replace('.csv', f'_{evt}.csv'))
                summary_df = summary_df.sample(n=100000, weights='pixel_count', replace=True)
                summary_df['forb_grass_percent_cover'] = summary_df['annual_forb_grass_percent_cover'] + summary_df['perennial_forb_grass_percent_cover'] 
                summary_df['EVT'] = evt
                sampled_rows.append(summary_df)
        except Exception as e:
            print(f'Failed to add EVT {evt}. Error: {e}')
            print('Skipping.')

    summary_df = pd.concat(sampled_rows, ignore_index=True)
    print(summary_df)
    g = sns.PairGrid(data=summary_df[['EVT', 'forb_grass_percent_cover', 'shrub_percent_cover', 'tree_percent_cover']], hue='EVT', palette=veg_color_palette)
    g.map_upper(plt.hexbin, gridsize=10)
    g.map_lower(sns.kdeplot)
    g.map_diag(sns.kdeplot)
    g.add_legend()
    g.savefig(fig_out + f'jointdistr_pairplot.png')
    plt.clf()

    plot_joint_density(summary_df, 'annual_forb_grass_percent_cover', 'perennial_forb_grass_percent_cover', fig_out + f'jointdistr_annual_perennial_sampled.png')
    plot_joint_density(summary_df, 'forb_grass_percent_cover', 'shrub_percent_cover', fig_out + f'jointdistr_forbgrass_shrub_sampled.png')
    plot_joint_density(summary_df, 'shrub_percent_cover', 'tree_percent_cover', fig_out + f'jointdistr_shrub_tree_sampled.png')
    plot_joint_density(summary_df, 'forb_grass_percent_cover', 'tree_percent_cover', fig_out + f'jointdistr_forbgrass_tree_sampled.png')

    