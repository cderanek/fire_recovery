import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import contextily as ctx
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.patches as mpatches

## NOTE: Used Claude to generate this visualiztion code base on my recovery counts df

def create_static_fire_map(df, output_path='fire_data_map.png', figsize=(15, 12), dpi=300):
    """
    Create a static map with Google satellite basemap and fire data points
    colored by sensitivity_selected
    """
    
    # Remove rows with missing coordinates
    df_clean = df.dropna(subset=['lat', 'lon'])
    
    # Create GeoDataFrame
    geometry = [Point(xy) for xy in zip(df_clean['lon'], df_clean['lat'])]
    gdf = gpd.GeoDataFrame(df_clean, geometry=geometry, crs='EPSG:4326')
    
    # Convert to Web Mercator for contextily
    gdf = gdf.to_crs('EPSG:3857')
    
    # Color based on sensitivity_selected
    # True = Red, False = Blue
    gdf['color'] = gdf['sensitivity_selected'].map({True: '#FF0000', False: '#0000FF'})
    
    # Set uniform marker size
    marker_size = 30
    
    # Create the plot
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    
    # Plot points
    gdf.plot(ax=ax, 
             color=gdf['color'], 
             markersize=marker_size,
             alpha=0.7,
             edgecolors='black',
             linewidth=0.5)
    
    # Add satellite basemap
    try:
        ctx.add_basemap(ax, 
                       crs=gdf.crs.to_string(),
                       source=ctx.providers.Esri.WorldImagery,
                       attribution='Esri',
                       zoom='auto')
    except Exception as e:
        print(e)
        # Fallback to OpenStreetMap if Google Satellite fails
        print("Satellite not available, using OpenStreetMap")
        ctx.add_basemap(ax, 
                       crs=gdf.crs.to_string(),
                       source=ctx.providers.OpenStreetMap.Mapnik)
    
    # Set title
    ax.set_title('California Fire Data Map\n' + 
                f'({df_clean["year"].min()}-{df_clean["year"].max()}) - {len(df_clean)} fires',
                fontsize=16, fontweight='bold', pad=20)
    
    # Remove axis ticks and labels for cleaner look
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_xlabel('')
    ax.set_ylabel('')
    
    # Create legend for sensitivity_selected
    legend_elements = [
        mpatches.Patch(color='#FF0000', label='Sensitivity Selected: True'),
        mpatches.Patch(color='#0000FF', label='Sensitivity Selected: False')
    ]
    
    # Add legend
    legend = ax.legend(handles=legend_elements, 
                      loc='upper right', 
                    #   bbox_to_anchor=(0.02, 0.98),
                      fontsize=12,
                      framealpha=0.9)
    legend.get_frame().set_facecolor('white')
    legend.get_frame().set_edgecolor('black')
    
    # Adjust layout and save
    plt.tight_layout()
    plt.savefig(output_path, dpi=dpi, bbox_inches='tight', 
                facecolor='white', edgecolor='none')
    
    print(f"Static map saved as '{output_path}'")
    print(f"Total fires plotted: {len(df_clean)}")
    
    # Show the plot
    plt.clf()
    
    return fig, ax