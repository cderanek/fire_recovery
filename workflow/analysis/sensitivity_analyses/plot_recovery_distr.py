import rioxarray as rxr
import rasterio as rio
import numpy as np
import pandas as pd
import xarray as xr
import glob, os, ast, yaml, gc, sys
import seaborn as sns
import matplotlib.pyplot as plt

sys.path.append('workflow/utils/')
from colors import sev_colors_num

def plot_distr_differences(
    param_name: str, 
    params_data_dict: dict, 
    plot_dir: str,
    severity_path: str) -> None:
    '''
    params_data_dict is formatted as:
        key: param value
        value: recovery data file path associated with that param value
    '''
    ## Create array w/ diff in recovery times (compared to default) 
    # open + mask default data
    default_val, default_path = list(params_data_dict['default'].items())[0]
    severity = rxr.open_rasterio(severity_path)
    default_recovery = rxr.open_rasterio(default_path)
    masked_default_recovery = default_recovery.copy().astype(np.float32)
    masked_default_recovery.data[:] = default_recovery.where(
        ((default_recovery.data >= 0) & (default_recovery.data <= 127)) & (severity.data != 0),
        np.nan
    ).data
    
    # calculate difference for each remaining param val
    diff_data = {}
    for param_val, path in params_data_dict.items():
        if param_val == 'default':
            continue
        recovery = rxr.open_rasterio(path).astype(np.float32)
        masked_recovery = recovery.where(
            ((recovery.data >= 0) & (recovery.data <= 127)) & (severity.data != 0),
            np.nan
        )
        diff = masked_recovery - masked_default_recovery
        diff_data[param_val] = diff

        del recovery, masked_recovery, diff
        gc.collect()

    # plot distribution of differences overall, and by severity
    n_params = len(diff_data)
    fig, axes = plt.subplots(1, n_params, figsize=(6*n_params, 5))
    if n_params == 1:
        axes = [axes]
    
    all_dfs = []
    for ax, (param_val, diff) in zip(axes, diff_data.items()):
        # create df for plotting
        df_list = []
        for sev_level in [2, 3, 4]:
            mask = severity.data[0] == sev_level
            vals = diff.data[0][mask & ~np.isnan(diff.data[0])]
            if len(vals) > 0:
                df_list.append(pd.DataFrame({
                    'Difference': vals, 
                    'Severity': sev_level,
                    'Pixel_Count': len(vals),
                    'Param': param_name,
                    'Param_Val': param_val
                }))
        
        if df_list:
            df = pd.concat(df_list, ignore_index=True)
            all_dfs.append(df)
            sns.histplot(data=df, x='Difference', hue='Severity', ax=ax)
            ax.set_title(f'{param_name} = {param_val}')
            ax.set_xlabel('Recovery Time Difference')
    
    del df_list, mask, vals, df
    gc.collect()

    plt.tight_layout()
    plt.savefig(f'{plot_dir}/{param_name}_distributions.png', dpi=300, bbox_inches='tight')
    plt.close()

    return pd.concat(all_dfs)


def organize_param_files(input_dir: str) -> dict:
    '''
    returns a dict with format:
        key: param name
        value: dict
            key: param value
            value: recovery data file paths associated with that param value
    '''
    subfolders = os.listdir(input_dir)
    recovery_file_pattern = '*_4seasons_recovery_clipped.tif'

    # Add all default vals
    default_params_folder = os.path.join(input_dir, 'default')
    default_recovery = glob.glob(os.path.join(default_params_folder, recovery_file_pattern))[0]
    with open(os.path.join(input_dir, 'default/params.txt'), "r") as f:
        dict_string = f.read()
        default_params_dict = ast.literal_eval(dict_string)

    # Loop over all params, storing param values and paths to data
    params_dict = {}
    for subfolder in subfolders:
        params_f = os.path.join(input_dir, subfolder, 'params.txt')
        if os.path.exists(params_f) and ('default' not in subfolder):
            param_name = '_'.join(subfolder.split('_')[:-1])
            param_val = float(subfolder.split('_')[-1])
            param_data_path = glob.glob(os.path.join(input_dir, subfolder, recovery_file_pattern))[0]

            if param_name not in params_dict.keys():
                # add default data path and current data path
                default_param_val = float(default_params_dict[param_name])
                params_dict[param_name] = {
                    param_val: param_data_path,
                    'default': {default_param_val: default_recovery}
                }
            else:
                params_dict[param_name][param_val] = param_data_path

    return params_dict


def plot_singlefire_distr_differences(input_dir: str, plot_dir: str) -> None:
    params_data_dict = organize_param_files(input_dir)
    severity_path = glob.glob(os.path.join(input_dir, 'spatialinfo', '*_burnsev.tif'))[0]
    all_dfs = []
    for param in params_data_dict.keys():
        df = plot_distr_differences(
            param_name=param, 
            params_data_dict=params_data_dict[param], 
            plot_dir=plot_dir,
            severity_path=severity_path)
        all_dfs.append(df)
    return pd.concat(all_dfs)


def plot_summary_distr(df, plot_dir):
    '''
    Creates a figure with histograms of differences colored by severity.
    Rows = parameters, Columns = parameter values.
    Uses Pixel_Count as weights for histogram/KDE.
    '''
    # Convert seasons to years
    df['Difference'] = df['Difference'] / 4
    sev_labels = {2: 'Low', 3: 'Medium', 4: 'High'}
    # Get unique params and param values
    params = df['Param'].unique()
    n_rows = len(params)
    
    # Determine max number of columns needed
    n_cols = max(df[df['Param'] == p]['Param_Val'].nunique() for p in params)
    
    # Create subplots
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(6*n_cols, 5*n_rows))
    
    # Handle single row/col cases
    if n_rows == 1 and n_cols == 1:
        axes = np.array([[axes]])
    elif n_rows == 1:
        axes = axes.reshape(1, -1)
    elif n_cols == 1:
        axes = axes.reshape(-1, 1)
    
    # Plot for each param and param_val
    for row_idx, param in enumerate(params):
        param_df = df[df['Param'] == param]
        param_vals = sorted(param_df['Param_Val'].unique())
        
        for col_idx, param_val in enumerate(param_vals):
            ax = axes[row_idx, col_idx]
            ax.axvline(x=0, color='grey', linestyle='--', alpha=0.7)

            subset = param_df[param_df['Param_Val'] == param_val]
            
            # Plot histogram for each severity level with weights
            for sev_level in [2, 3, 4]:
                sev_subset = subset[subset['Severity'] == sev_level]
                if len(sev_subset) > 0:
                    sns.kdeplot(data=sev_subset, 
                               x='Difference', 
                               weights='Pixel_Count',
                               ax=ax,
                               alpha=0.5,
                               label=f'{sev_labels[sev_level]} severity',
                               color=sev_colors_num[sev_level],
                               linewidth=1.5)
            
            ax.set_title(f'{param} = {param_val}')
            ax.set_xlabel('Recovery Time Difference (yrs)')
            ax.set_ylabel('Weighted Count')
            ax.legend()
        
        # Hide unused subplots
        for col_idx in range(len(param_vals), n_cols):
            axes[row_idx, col_idx].axis('off')
    
    plt.tight_layout()
    plt.savefig(f'{plot_dir}/all_params_kdes.png', dpi=300, bbox_inches='tight')
    plt.close()


if __name__ == '__main__':
    perfire_config_json_path = sys.argv[1] # data/california/recovery_maps/submission_organizer/perfire_config.json
    sensitivity_plot_path = sys.argv[2] # results/sensitivity_analyses

    with open(perfire_config_json_path, 'r') as f:
        config = yaml.safe_load(f)

    sensitivity_fires = [d['FILE_PATHS'] for d in config.values() if d['FIRE_METADATA']['SENSITIVITY_ANALYSIS']==True]
    all_dfs = []

    for fire_file_paths_d in sensitivity_fires:
        input_dir = fire_file_paths_d['OUT_MAPS_DATA_DIR_PATH']
        plot_dir = os.path.join(fire_file_paths_d['PLOTS_DIR'],'sensitivity_distr_diffs/')
        os.makedirs(plot_dir, exist_ok=True)
        print(input_dir)
        print(plot_dir)
        
        try:
            df = plot_singlefire_distr_differences(input_dir, plot_dir)
            all_dfs.append(df)

        except Exception as e:
            print(f'Failed to create distribution for {os.path.basename(input_dir)}.')
            print(e)
    
    # Group by all columns except Pixel_Count and sum the counts
    df_summed = pd.concat(all_dfs).groupby(['Difference', 'Severity', 'Param', 'Param_Val'], as_index=False)['Pixel_Count'].sum()
    plot_dir = os.path.join(sensitivity_plot_path, 'diff_distr_summary')
    print(plot_dir)
    os.makedirs(plot_dir, exist_ok=True)
    print(df_summed)
    plot_summary_distr(df_summed, plot_dir)