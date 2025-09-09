from sankeyflow import Sankey
import matplotlib.pyplot as plt
import pandas as pd


out_plot = '1999_2020_sankey.svg'
f = 'nlcd_1999_2020_transitions_summary.csv'
df = pd.read_csv(f).dropna()

# Define colors by land cover type
def get_landcover_color(label):
    """Assign colors based on land cover type"""
    if 'Water' in label:
        return '#0066cc80'
    elif 'Developed' in label or 'Urban' in label:
        return '#ff000080'
    elif 'Forest' in label:
        return '#228b2280'
    elif 'Wetland' in label:
        return '#87ceeb80'
    elif 'Crop' in label or 'Cultivated' in label:
        return '#ffd70080'
    elif 'Grass' in label or 'Pasture' in label:
        return '#90ee9080'
    elif 'Barren' in label:
        return '#d2691e80'
    elif 'Shrub' in label:
        return '#dda0dd80'
    else:
        return '#80808080'  # Default gray



# Remove self-loops (where source == target)
df_filtered = df[df['counts']>10**6]#df[df['source_labels'] != df['target_labels']].copy()
df_filtered['source_labels'] = df_filtered['source_labels'].astype('str').apply(lambda s: s+'_1999')
df_filtered['target_labels'] = df_filtered['target_labels'].astype('str').apply(lambda s: s+'_2020')
df_filtered['color'] = df_filtered['target_labels'].apply(lambda s: get_landcover_color(s))

flow = df_filtered[['source_labels', 'target_labels', 'counts', 'color']].values.tolist()
flow = [(s, t, count, {'color': color}) for s, t, count, color in flow]
print(flow)

print(f"Original flows: {len(df)}")
print(f"Filtered flows: {len(df_filtered)}")
print(f"Removed {len(df) - len(df_filtered)} self-loops")

s = Sankey(
    flows=flow,#df_filtered[['source_labels', 'target_labels', 'counts']].values.tolist(),
    scale=1000
)
# Draw the plot
plt.figure(figsize=(12, 20))
s.draw()
plt.title('NLCD 1999-2020 Transitions')
plt.tight_layout()

# Save using matplotlib
plt.savefig(out_plot, dpi=300, bbox_inches='tight')
plt.close()  # Close to free memory