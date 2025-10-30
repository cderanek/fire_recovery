import os
import seaborn as sns
from optuna.visualization import plot_contour
import matplotlib.pyplot as plt
from lifelines.utils import concordance_index
import shap
import scipy as sp
import rasterio 
import geopandas as gpd
from itertools import combinations

from merge_predictor_layers_info import *


#### Initial exploration of the distribution of each predictor var + correlation b/w all pairs of predictors ####
def plot_distributions(sampled_pts):
    features = sampled_pts.select_dtypes(include=['number']).columns.tolist()
    cat_features = ['aspect', 'slope', 'burn_bndy_dist_km', 'severity', 'vegetation_type', 'hot_drought_categories', '10yr_fire_count']
    # Plot distribution of all features
    for feature_name in features:
        # if distribution not already plotted
        filepath = os.path.join(output_model_dir, f'{feature_name}_distribution_sampled.png')
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
        filename = f'{feature1}_vs_{feature2}_scatter.png'
        filepath = os.path.join(output_model_dir, filename)
        
        # Skip if already exists
        if not os.path.exists(filepath):
            plt.figure(figsize=(8, 6))
            sns.scatterplot(data=sampled_pts, x=feature1, y=feature2)
            plt.tight_layout()
            plt.savefig(filepath, dpi=150)
            plt.close()  # Important: close figure to free memory


#### Summary stats of shapley values ####



#### Shapley value maps ####

def predict_raster_with_nodata(raster_path, truey_path, model, feature_names, cat_cols=None,
                                output_path='predictions.tif', nodata_value=-9999):
    """
    Predict on raster while handling NoData values.
    """
    with rasterio.open(raster_path) as src:
        profile = src.profile
        bands = src.read()
        
        # Create mask for valid pixels (no negative vals in any band)
        # -128 and -9999 values are np.nans
        valid_mask = ~np.any(bands < 0, axis=0)
        
        # Flatten
        height, width = bands.shape[1], bands.shape[2]
        bands_2d = bands.reshape(bands.shape[0], -1).T
        valid_mask_flat = valid_mask.flatten()
        
        # Initialize output with NoData
        predictions_flat = np.full(bands_2d.shape[0], nodata_value, dtype=np.float32)
        
        # Only predict on valid pixels
        if valid_mask_flat.any():
            X_valid = pd.DataFrame(bands_2d[valid_mask_flat], columns=feature_names)
            
            # Handle categorical features
            if cat_cols:
                for col in cat_cols:
                    if col in X_valid.columns:
                        # X_valid[col] = X_valid[col].astype('category')
                        # Get the training categories in order
                        training_categories = [
                            categorical_mappings[col][i] 
                            for i in sorted(categorical_mappings[col].keys())
                        ]
            
                        # Convert to categorical with exact training categories
                        X_valid[col] = pd.Categorical(
                            X_valid[col],
                            categories=training_categories,
                            ordered=False)

                global categorical_mappings_test
                categorical_mappings_test = {}
                for col in cat_cols:
                    mapping = dict(enumerate(X_valid[col].cat.categories))
                    categorical_mappings_test[col] = mapping
                
            # Predict
            dmatrix = xgb.DMatrix(X_valid, enable_categorical=True)
            predictions_valid = model.predict(dmatrix)
            
            # Insert predictions back into full array
            predictions_flat[valid_mask_flat] = predictions_valid

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
        
        # Reshape to 2D
        predictions_2d = predictions_flat.reshape(height, width)
        
        # Write output
        profile.update(
            dtype=rasterio.float32,
            count=2,
            nodata=nodata_value,
            compress='lzw'
        )
        
        with rasterio.open(output_path, 'w', **profile) as dst:
            dst.write(predictions_2d, 1)

            # Also save error in predictions for recovered pixels
            survival_vals, survival_mask = rasterio.open(truey_path).read()
            predictions_diff = np.where(
                survival_mask!=1, 
                np.nan, 
                predictions_2d - survival_vals
            )

            dst.write(predictions_diff, 2)

        # Save shap values
        profile_shap = profile.copy()
        profile.update(
            dtype=rasterio.float32,
            count=1,
            nodata=nodata_value,
            compress='lzw'
        )

        for i, feature_name in enumerate(feature_names):
            # Reshape SHAP values for this feature
            shap_flat_output = shap_values[:, i]
            shap_flat = np.full(bands_2d.shape[0], nodata_value, dtype=np.float32)
            shap_flat[valid_mask_flat] = shap_flat_output
            shap_2d = shap_flat.reshape(height, width)
            
            # Save as raster
            shap_path = f'shap_{feature_name}.tif'
            with rasterio.open(shap_path, 'w', **profile_shap) as dst:
                dst.write(shap_2d.astype(rasterio.float32), 1)
            
            print(f"  Saved SHAP values for {feature_name}")
        
        # Also save base value (expected value)
        base_value = explainer.expected_value
        base_value_2d = np.full((height, width), base_value, dtype=np.float32)

        # Save as raster
        shap_path = f'shap_base_value.tif'
        with rasterio.open(shap_path, 'w', **profile_shap) as dst:
            dst.write(base_value_2d.astype(rasterio.float32), 1)
            
            print(f"  Saved SHAP values for {feature_name}")
    
    print(f"Predictions saved to {output_path}")