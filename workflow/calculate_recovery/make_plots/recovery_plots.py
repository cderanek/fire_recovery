import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
import seaborn as sns

from typing import List



# Note: the plot_time_series function, create_density_plot functions were created by feeding my original code to claude, 
# and asking claude to add/update features, 
# including creating the legend with pixel counts and fixing the facet grid issues I was running into
colors = {
    "UNDISTURBED": "grey",
    "ALL": "black",
    "LOW_SEV": "green",
    "MED_SEV": "orange",
    "HIGH_SEV": "red",
    }


def create_density_plot(
    group_med_ndvi_arr: np.ndarray, 
    bin_edges: List, 
    quantiles: List, 
    plots_dir: str, 
    fname: str):
    """
    Create and save a density plot of values.
    
    Parameters:
    -----------
    group_med_ndvi_arr : np array
        The array of values to plot
    bin_edges : list
        the cutoffs for NDVI groupings
    quantiles : list
        the quantiles used to create each NDVI grouping cutoff
    plots_dir : str
        Directory to save the plot 
    fname : str
        File name to save the plot
    """
    # Filter out NaN values
    valid_values = values[~np.isnan(values)]
    
    # Create figure and axis
    plt.figure(figsize=(10, 6))
    
    # Create the density plot using seaborn
    sns.kdeplot(valid_values, fill=True, color='steelblue', alpha=0.7)
    
    # Plot vertical lines for quantile boundaries
    n_quantiles = len(quantiles)
    for i, edge in enumerate(bin_edges):
        if i > 0 and i < len(bin_edges):  # Skip the first and last edge
            plt.axvline(edge, color='r', linestyle='--', alpha=0.7, 
                        label=f'Q{i}/{n_quantiles}' if i == 1 else None)
            plt.text(edge, plt.ylim()[1]*0.95, f'{quantiles[i]:.1%}', 
                    rotation=90, ha='right', va='top')
    
    # Add labels and title
    plt.xlabel('Median pre-fire NDVI')
    plt.ylabel('Density')
    plt.title(title)
    
    # Format y-axis as percentage
    plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
    
    # Add grid for better readability
    plt.grid(True, alpha=0.3)
    
    # Add legend
    plt.legend(title='Quantile Boundaries')
    
    # Ensure directory exists
    output_path = os.path.join(plots_dir, fname)
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
    
    # Save the plot
    plt.tight_layout()
    plt.savefig(output_path, dpi=300)
    plt.close()
    
    print(f"Density plot saved to {output_path}")


def plot_time_series(
    summary_df: pd.DataFrame, 
    fire_date: np.datetime64, 
    PLOTS_DIR: str, 
    min_pixel_count: int) -> None:

    summary_df = summary_df.reset_index().dropna(subset='lower') # if no NDVI values, drop row
    cols_of_interest = ["time", "50pctl", "Masked", "10pctl", "90pctl", "groups", "Elevation", "Count", "prefire_median_NDVI"]
        
    # Get data 5-yrs pre-fire to present
    summary_df['time'] = pd.to_datetime(summary_df['time'])
    summary_df = summary_df[summary_df['time'] >= fire_date - pd.Timedelta(weeks=52*5)]

    for veg_type in np.unique(summary_df["Vegetation_Name"].astype('str')):
        print(veg_type)
        small_df = summary_df[(summary_df["Vegetation_Name"] == veg_type)][cols_of_interest].dropna()
        
        for elevation in np.unique(small_df["Elevation"]):
            small_df_elev = small_df[small_df["Elevation"] == elevation]
            
            if len(small_df_elev) != 0:

                # Create the figure and axes
                fig, ax = plt.subplots(figsize=(10, 4))

                # Create a FacetGrid with rows for each unique value in prefire_median_NDVI
                g = sns.FacetGrid(
                    data=small_df_elev,
                    row='prefire_median_NDVI',
                    height=4,
                    aspect=3,
                    sharey=False
                )

                # First map to create all plots but remove legends
                g.map_dataframe(
                    sns.lineplot,
                    x="time", 
                    y="50pctl", 
                    hue="Masked",
                    palette=colors,
                    legend=False  # Don't add legend yet
                )
                    
                # Apply the confidence ribbon function for UNDISTURBED to each facet
                g.map_dataframe(add_confidence_ribbon)
                    
                # Now manually add legends with counts to each facet
                # Get unique prefire_median_NDVI values
                ndvi_values = small_df_elev['prefire_median_NDVI'].unique()
                    
                # For each facet (row)
                for i, ndvi_val in enumerate(ndvi_values):
                    if i < len(g.axes):  # Make sure we have an axis for this NDVI value
                        ax = g.axes[i, 0]  # Get the axis for this row
                        
                        # Filter data for this facet
                        facet_data = small_df_elev[small_df_elev['prefire_median_NDVI'] == ndvi_val]
                            
                        # Get latest entries for each Masked value in this facet
                        latest_entries = facet_data.sort_values('time').groupby('Masked').last().reset_index()
                            
                        # Get unique Masked values in this facet
                        masked_values = facet_data['Masked'].unique()
                            
                        # Create legend handles and labels with counts
                        handles = []
                        labels = []
                            
                        for j, mask_val in enumerate(masked_values):
                            # Get color for this Masked value
                            if isinstance(colors, dict):
                                color = colors.get(mask_val, 'gray')
                            else:
                                color_idx = list(small_df_elev['Masked'].unique()).index(mask_val) if mask_val in small_df_elev['Masked'].unique() else j
                                color = colors[color_idx % len(colors)] if isinstance(colors, list) else 'gray'
                                
                            # Create line handle
                            handle = plt.Line2D([0], [0], color=color, lw=2)
                            handles.append(handle)
                                
                            # Create label with count
                            mask_data = latest_entries[latest_entries['Masked'] == mask_val]
                            if not mask_data.empty and 'Count' in mask_data.columns:
                                count = mask_data['Count'].iloc[0]
                                label = f"{mask_val} (n={int(count)})"
                            else:
                                label = mask_val
                                
                            labels.append(label)
                            
                        # Add legend to this facet
                        ax.legend(handles, labels, title="Groups", loc='best', fontsize='small')
                            
                        for col, min_pixel_count in [('black', 500), ('green', 300), ('orange', 100), ('red', 50)]:
                            # Add lines for bad dates for this facet
                            mask = (facet_data["Count"] < min_pixel_count) & (facet_data["Masked"] == "UNDISTURBED")
                            bad_dates = facet_data[mask]["time"]
                            print(f'Flagging bad_dates: {bad_dates}')
                                
                            # Add bad_dates as grey lines
                            for date in bad_dates:
                                # Convert string date to datetime if necessary
                                if isinstance(date, str):
                                    date = pd.to_datetime(date)
                                    
                                # Add vertical line
                                ax.axvline(x=date, color=col, linestyle='--', alpha=0.7)
                    
                # Common settings for all facets
                for ax in g.axes.flat:
                    # Add the vertical line for the fire date
                    ax.axvline(x=fire_date, color='red', linestyle='--')

                    # Configure the x-axis to show yearly ticks
                    ax.xaxis.set_major_locator(mdates.YearLocator())
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
                        
                    # Rotate x-axis labels to be vertical
                    plt.setp(ax.get_xticklabels(), rotation=90, ha='center')
                    
                # Apply classic theme
                sns.set_style("ticks")
                plt.tight_layout()

                # Save the figure
                output_path = PLOTS_DIR + str(elevation) + veg_type.replace("/", "").replace(" ", "_") + ".png"
                plt.savefig(output_path, dpi=300)
                plt.close()

# Function to add confidence ribbon for each facet
def add_confidence_ribbon(data, **kwargs):    
    ax = plt.gca()
                
    undisturbed_data = data[data["Masked"] == "UNDISTURBED"]
    if not undisturbed_data.empty:
        ax.fill_between(
            undisturbed_data["time"], 
            undisturbed_data["10pctl"], 
            undisturbed_data["90pctl"], 
            alpha=0.3, 
            color=colors["UNDISTURBED"] if isinstance(colors, dict) else colors[list(data["Masked"].unique()).index("UNDISTURBED")]
        )


def plot_random_sampled_pt(
    ndvi_da: xr.DataArray, 
    summary_df: pd.DataFrame, 
    data_dir: str) -> None:

    # Randomly select (x, y) coordinate
    x_idx = np.random.choice(ndvi_da.dims['x'])
    y_idx = np.random.choice(ndvi_da.dims['y'])

    # Get data 5-yrs pre-fire to present
    fire_date = pd.to_datetime([ndvi_da.NDVI.attrs['fire_date']], format=ndvi_da.NDVI.attrs['fire_date_format'])[0]
    print(fire_date)
    ds = ndvi_da.sel(time=slice(fire_date - pd.Timedelta(weeks=52*5), pd.Timestamp.now()))
    
    # Get NDVI/threshold time series + grouping info for the selected pixel over time
    ndvi_time_series = ds['NDVI'][:, y_idx, x_idx].values  # NDVI time series for the selected pixel
    threshold_time_series = ds['threshold'][:, y_idx, x_idx].values  # Threshold time series for the selected pixel
    
    uid = ds['groups'][y_idx, x_idx].values
    recovery_time = ds.coords['fire_recovery_time'][y_idx, x_idx].values
    curr_df = summary_df.reset_index()[(summary_df.reset_index()['groups']==uid) & (summary_df.reset_index()['Masked']=='UNDISTURBED')]
    elevation, vegetation_type = curr_df[['Elevation', 'Vegetation_Name']].values[0]
    times, lower, upper = pd.to_datetime(curr_df['time'].values), curr_df['lower'].values, curr_df['upper'].values

    # Create a color array based on whether the threshold is above 1 (blue) or not (red)
    colors = np.where(threshold_time_series == 1, 'blue', 'red')

    # Plot the NDVI time series with color determined by threshold value
    plt.figure(figsize=(10, 6))

    # Create confidence ribbon
    plt.fill_between(times, lower, upper, color='gray', alpha=0.3, label='Confidence Interval +/-1std')

    # Scatter plot with colors based on threshold
    scatter = plt.scatter(ds['time'].values, ndvi_time_series, c=colors, edgecolors='k')
    plt.plot(ds['time'].values, ndvi_time_series)
    
    # Add vertical line for fire date 
    plt.axvline(x=fire_date, color='red', linestyle='--', label='FIRE IGNITION')  # Add vertical line

    # Set the date limits
    plt.xlim(fire_date - pd.Timedelta(weeks=52*5), pd.Timestamp.now())

    # Add labels and title
    plt.xlabel('Time')
    plt.ylabel('NDVI')
    plt.suptitle(f'NDVI Time Series for Pixel ({x_idx}, {y_idx}) with UID {uid} and recovery time {recovery_time}')
    plt.title(f'Elevation {elevation}; Veg type {vegetation_type}')

    # add a legend for color scheme
    red_patch = plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='blue', markersize=10, label='Above threshold')
    blue_patch = plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='red', markersize=10, label='Not above threshold')
    plt.legend(handles=[red_patch, blue_patch], title='Threshold')

    # Save plot
    plt.xticks(rotation=45)
    plt.tight_layout()
    print(f'Saved at: {data_dir}timerest_recovery{recovery_time}{x_idx}_{y_idx}samplept_timeseries.png')
    plt.savefig(f'{data_dir}timerest_recovery{recovery_time}{x_idx}_{y_idx}samplept_timeseries.png')