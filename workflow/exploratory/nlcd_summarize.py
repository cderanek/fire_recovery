import pandas as pd
import numpy as np
import rioxarray as rxr

NLCD_codes_csv = 'data/baselayers/downloadlogs_metadata/NLCD/vegcodes_summary.csv'
NLCD_start_f= 'data/baselayers/temp/NLCD/Annual_NLCD_LndCov_1999_CU_C1V1_clipped.tif'
NLCD_end_f= 'data/baselayers/temp/NLCD/Annual_NLCD_LndCov_2020_CU_C1V1_clipped.tif'
out_csv = 'nlcd_1999_2020_transitions_summary.csv'

# NLCD_codes_csv = 'vegcodes_summary.csv'
# NLCD_start_f= 'Annual_NLCD_LndCov_1999_CU_C1V1_clipped.tif'
# NLCD_end_f= 'Annual_NLCD_LndCov_2020_CU_C1V1_clipped.tif'
# out_csv = 'nlcd_1999_2020_transitions_summary.csv'

# open input files
nlcd_codes = pd.read_csv(NLCD_codes_csv) # cols: NLCD_CODE,NLCD_NAMES,year
nlcd_start_data = rxr.open_rasterio(NLCD_start_f).data.squeeze().astype(np.int16).flatten()
nlcd_end_data = rxr.open_rasterio(NLCD_end_f).data.squeeze().astype(np.int16).flatten()

# create transitions layer (val_1999*100 + val_2020)
nlcd_transitions = np.add(
    np.multiply(nlcd_start_data, 100), 
    nlcd_end_data
    )
nlcd_transitions = np.where(
    (nlcd_start_data==250) | (nlcd_end_data==250),
    0,
    nlcd_transitions)

# count unique values
unique_values, counts = np.unique(nlcd_transitions, return_counts=True)

# save to df
df = pd.DataFrame({
    'transition_vals': unique_values,
    'counts': counts,
})
df['transition_vals'] = df['transition_vals'].astype(str).str.zfill(4)
df['source'] = df['transition_vals'].apply(lambda s: s[:2])
df['target'] = df['transition_vals'].apply(lambda s: s[2:])


nlcd_codes_filtered = nlcd_codes[nlcd_codes['year']==1999]
nlcd_codes_dict = dict(list(zip(nlcd_codes_filtered['NLCD_CODE'].astype('int'), nlcd_codes_filtered['NLCD_NAMES'])))
nlcd_codes_dict[0] = np.nan
df['source_labels'] = df['source'].astype('int').map(nlcd_codes_dict)

nlcd_codes_filtered = nlcd_codes[nlcd_codes['year']==2020]
nlcd_codes_dict = dict(list(zip(nlcd_codes_filtered['NLCD_CODE'].astype('int'), nlcd_codes_filtered['NLCD_NAMES'])))
nlcd_codes_dict[0] = np.nan
df['target_labels'] = df['target'].astype('int').map(nlcd_codes_dict)

df.to_csv(out_csv)
