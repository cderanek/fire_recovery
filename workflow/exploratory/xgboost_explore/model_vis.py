import os
import seaborn as sns
from optuna.visualization import plot_contour
import matplotlib.pyplot as plt
from lifelines.utils import concordance_index
import shap
import scipy as sp
import rasterio 
import geopandas as gpd
import pandas as pd
from itertools import combinations
import xgboost as xgb

from merge_predictor_layers_info import *


#### Initial exploration of the distribution of each predictor var + correlation b/w all pairs of predictors ####
def plot_distributions(sampled_pts):
    features = sampled_pts.select_dtypes(include=['number']).columns.tolist()
    cat_features = ['aspect', 'slope', 'burn_bndy_dist_km', 'severity', 'vegetation_type', 'hot_drought_categories', '10yr_fire_count']
    # Plot distribution of all features
    for feature_name in features:
        # if distribution not already plotted
        filepath = os.path.join(output_model_dir, f'distributions/{feature_name}_distribution_sampled.png')
        if not os.path.exists(filepath):
            if feature_name in cat_features:
                plt.figure(figsize=(8, 6))
                sns.histplot(sampled_pts, x=feature_name)
                plt.tight_layout()
                plt.savefig(filepath, dpi=150)
                plt.close() 

            else:
                plt.figure(figsize=(8, 6))
                sns.kdeplot(sampled_pts, x=feature_name)
                plt.tight_layout()
                plt.savefig(filepath, dpi=150)
                plt.close()

    # Loop through all pairs of features
    for feature1, feature2 in combinations(features, 2):
        filename = f'comparisons/{feature1}_vs_{feature2}_scatter.png'
        filepath = os.path.join(output_model_dir, filename)
        
        # Skip if already exists
        if not os.path.exists(filepath):
            plt.figure(figsize=(8, 6))
            sns.scatterplot(data=sampled_pts, x=feature1, y=feature2)
            plt.tight_layout()
            plt.savefig(filepath, dpi=150)
            plt.close()  # Important: close figure to free memory


#### xgboost model results ####
def plot_model_results(study, evals_result, bst, X, X_for_shap, y_lower_bound, feature_names, output_dir):
    # Contour plot from optuna
    # Generate the contour plot
    fig = plot_contour(study)

    # Update the figure layout to change its size
    fig.update_layout(width=800, height=800).savefig(os.path.join(output_dir,'optuna_tuning_results.png'))
    fig.close()

    # Learning curve
    epochs = len(evals_result['train']['aft-nloglik'])
    x_axis = range(0, epochs)
    fig, ax = plt.subplots()
    ax.plot(x_axis, evals_result['train']['aft-nloglik'], label='Train aft-nloglink')
    ax.plot(x_axis, evals_result['eval']['aft-nloglik'], label='Eval aft-nloglik')
    ax.legend()
    plt.ylabel('aft-nloglink')
    plt.title('XGBoost aft-nloglink Learning Curve')
    plt.savefig(os.path.join(output_dir,'learning_curve.png'))
    plt.close()

    # Feature importance
    xgb.plot_importance(bst).savefig(os.path.join(output_dir, 'feature_importance.png'))

    # SHAP values per feature
    explainer = shap.TreeExplainer(bst)
    shap_values = explainer.shap_values(X_for_shap)
    shap.summary_plot(shap_values, X_for_shap, alpha=0.1).savefig(os.path.join(output_dir, 'shap_all_features.png'))

    # Separate plot of shap values for each feature
    explainer = shap.TreeExplainer(bst)
    shap_values_explanation = explainer(X_for_shap)
    for col in feature_names:
        shap.plots.scatter(shap_values_explanation[:, col], alpha=0.2, dot_size=0.5).savefig(os.path.join(output_dir, f'shap_{col}.png'))
    
    # Clustering bar plot
    clustering = shap.utils.hclust(X_for_shap, y_lower_bound)
    shap.plots.bar(shap_values_explanation, clustering=clustering, clustering_cutoff=.8).savefig(os.path.join(output_dir, f'clustering_barplot.png'))
    
    partition_tree = shap.utils.partition_tree(X_for_shap)
    plt.figure(figsize=(15, 10))
    sp.cluster.hierarchy.dendrogram(partition_tree, labels=X_for_shap.columns)
    plt.title("Hierarchical Clustering Dendrogram")
    plt.xlabel("feature")
    plt.ylabel("distance")
    plt.xticks(rotation=90) 
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f'clustering_hierarchy.png'))
    plt.close()



#### Summary stats of shapley values ####
def format_predictors_raster(bands, band_names, feature_names):
    '''
    Standardize the predictors layers to match the model input.
    Returns reformatted bands in the same order
    '''
    # Standardize units to match model input
    new_bands = []
    for feature in feature_names:
        try:
            band_index = band_names.index(feature)
            print(feature, band_index)
        except ValueError:
            print(f"'{feature}' not found in the band names.")
            print(f"Band names found are: {'\n'.join(band_names)}")
            print('Exiting with error.')
            return

        if feature=='vegetation_type':
            for bad_veg_type in [-128, 0, 7, 11]:
                bands[band_index] = np.where(bands[band_index]==bad_veg_type, -9999, bands[band_index])

        if '_anom' in feature or 'maxtempz' in feature:
            updated_vals = np.round(bands[band_index] * 10**-2) * 10**-1
            bands[band_index] = np.where(bands[band_index] == -9999, -9999, updated_vals)
            bands[band_index] = np.where(np.abs(updated_vals) <= 10, -9999, updated_vals)

        if 'pdsi' in feature:
            updated_vals = bands[band_index] * 10**-2
            bands[band_index] = np.where(bands[band_index] == -9999, -9999, updated_vals)

        if feature == 'burn_bndy_dist_km_upperbound':
            bands[band_index] = np.where(bands[band_index] < 0, -9999, bands[band_index] / 10) # convert to km (currently in 100s of meters)

        if feature in ['count_pixels_unburnedlowsev_matchveg_300mbuffer', 'count_burned_highsev_300mbuffer']:
            bands[band_index] = np.where(bands[band_index] < 0, -9999, bands[band_index] * 10) # convert to count (currently in 10s of pixels)

        if feature in ['elevation', 'aspect', 'slope']:
            bands[band_index] = np.where(bands[band_index] == -128, -9999, bands[band_index]) # values==-128 0 are invalid

        if feature in ['severity', 'wateryr_avg_pr_total']:
            bands[band_index] = np.where(bands[band_index] <= 0, -9999, bands[band_index]) # values less than= 0 are invalid

        new_bands.append(bands[band_index]) # ensure ordering matches feature_names input
    return np.array(new_bands).squeeze()



def create_flat_inputs(bands, categorical_mappings):
    '''
    Given standardized predictors layers to match the model input,
    creates flat format input dat a with missing data masked.
    Returns valid_mask, X_valid, predictions_flat
    '''
    # Create valid mask (exclude -9999 or np.nan)
    valid_mask = (~np.any(bands == -9999, axis=0)) | (~np.any(np.isnan(bands), axis=0))

    # Flatten to (pixels, bands)
    bands_2d = bands.reshape(bands.shape[0], -1).T
    valid_mask_flat = valid_mask.flatten()

    # Initialize output array
    predictions_flat = np.full(bands_2d.shape[0], -9999, dtype=np.float32)

    if valid_mask_flat.any():
        # Subset to valid pixels
        X_valid = pd.DataFrame(bands_2d[valid_mask_flat], columns=band_names)

        print(f"\n--- Valid pixels: {X_valid.shape[0]:,}")
        print(f"Feature columns: {list(X_valid.columns)}")

        # Handle categorical mappings to match original mappings
        if categorical_mappings:
            for col in categorical_mappings.keys():
                if col in X_valid.columns:
                    training_cats = list(categorical_mappings[col].values())
                    X_valid[col] = (
                                X_valid[col]
                                .astype(str)
                                .replace("nan", np.nan)
                                .replace("-9999", np.nan)
                            )
                    X_valid[col] = pd.Categorical(
                                X_valid[col],
                                categories=training_cats,
                                ordered=False
                            )

        # Return valid mask, empty list for predictions, X_valid
        return valid_mask, X_valid, predictions_flat
    
    else:
        print('ERROR: No valid pixels.')
        return None



def write_predictions_tif(predictions_flat, valid_mask_flat, height, width, profile, dmatrix, model, output_file, truey_path=None, nodata_value=-9999):
    # predict
    predictions_valid = model.predict(dmatrix)

    # Insert predictions back into full array
    predictions_flat[valid_mask_flat] = predictions_valid

    # Reshape to 2D
    predictions_2d = predictions_flat.reshape(height, width)
    
    # Write output
    if truey_path: outcount=2
    else: outcount=1
    profile.update(
        dtype=rasterio.float32,
        count=outcount,
        nodata=nodata_value,
        compress='lzw'
    )
    
    with rasterio.open(output_file, 'w', **profile) as dst:
        dst.write(predictions_2d, 1)
        
        if truey_path:
            # Also save error in predictions for recovered pixels
            survival_vals, survival_mask = rasterio.open(truey_path).read()
            predictions_diff = np.where(
                survival_mask!=1, 
                np.nan, 
                predictions_2d - survival_vals
            )

            dst.write(predictions_diff, 2)
    
    print(f"Predictions saved to {output_file}")



def calculate_save_shap(X_valid, feature_names, cat_cols, model, output_dir):
    # Create SHAP explainer
    print("Computing SHAP values...")
    X_for_shap = X_valid.copy()
    for col in cat_cols:
        if col in X_for_shap.columns:
            X_for_shap[col] = X_for_shap[col].cat.codes

    explainer = shap.TreeExplainer(model)
    
    # Compute SHAP values in batches
    batch_size = 1000
    shap_values_list = []
    
    for i in range(0, len(X_for_shap), batch_size):
        batch = X_for_shap.iloc[i:i+batch_size]
        batch_shap = explainer.shap_values(batch, check_additivity=False)
        shap_values_list.append(batch_shap)
        print(f"  Processed {min(i+batch_size, len(X_for_shap))}/{len(X_for_shap)} pixels")
    
    shap_values = np.vstack(shap_values_list)
    # Save shap values
    profile_shap = profile.copy()
    profile.update(
        dtype=rasterio.float32,
        count=1,
        nodata=-9999,
        compress='lzw'
    )

    for i, feature_name in enumerate(feature_names):
        # Reshape SHAP values for this feature
        shap_flat_output = shap_values[:, i]
        shap_flat = np.full(bands_2d.shape[0], -9999, dtype=np.float32)
        shap_flat[valid_mask_flat] = shap_flat_output
        shap_2d = shap_flat.reshape(height, width)
        
        # Save as raster
        shap_path = os.path.join(output_dir, f'shap_{feature_name}.tif')
        with rasterio.open(shap_path, 'w', **profile_shap) as dst:
            dst.write(shap_2d.astype(rasterio.float32), 1)
        
        print(f"\tSaved SHAP values for {feature_name}")
    
    # Also save base value (expected value)
    base_value = explainer.expected_value
    base_value_2d = np.full((height, width), base_value, dtype=np.float32)

    # Save as raster
    shap_path = os.path.join(output_dir, f'shap_base_value.tif')
    with rasterio.open(shap_path, 'w', **profile_shap) as dst:
        dst.write(base_value_2d.astype(rasterio.float32), 1)
        
        print(f"\tSaved SHAP values for {feature_name}")



def predict_raster(
    raster_path, 
    truey_path, 
    model, 
    feature_names, 
    categorical_mappings, 
    cat_cols=None,
    output_dir=''
    ):
    """
    Predict on predictors raster while handling missing data values and categorical consistency with training.
    """
    with rasterio.open(raster_path) as src:
        profile = src.profile
        bands = src.read()
        band_names = src.descriptions
        height, width = bands.shape[1], bands.shape[2]

        # Reformat bands to match input layers for xgboost
        bands = format_predictors_raster(bands, band_names, feature_names)

        # Create dmatrix with appropriate categorical mappings for input to xgb.predict
        valid_mask, X_valid, predictions_flat = create_flat_inputs(bands, categorical_mappings)

        # Write predictions
        dmatrix = xgb.DMatrix(X_valid, enable_categorical=True)
        output_file = os.path.join(output_dir, f'predictions_trueclimate.tif')
        write_predictions_tif(predictions_flat.copy(), valid_mask_flat, height, width,
                            profile, dmatrix, model, output_file, truey_path, nodata_value=-9999)

        # Make predictions for altered climate scenarios
        altered_scenarios = {
            '1yrpre_pdsi_avg': range(-5, 6),
            '1yrpost_pdsi_avg': range(-5, 6),
            '1yrpre_vpd_dry_anom': range(-3, 4),
            '1yrpost_vpd_dry_anom': range(-3, 4)
        }
        for var, val_range in altered_scenarios.items():
            for val in val_range:
                X_temp = X_valid.copy()
                X_temp[var] = val
                dmatrix_alt = xgb.DMatrix(X_temp, enable_categorical=True)
                output_file = os.path.join(output_dir, f'predictions_{var}_{val}.tif')
                write_predictions_tif(predictions_flat, valid_mask_flat, height, width,
                                    profile, dmatrix_alt, model, output_file, truey_path, -9999)

        # Create maps for shap values
        calculate_save_shap(X_valid, feature_names, cat_cols, model, output_dir)
        
