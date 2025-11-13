import numpy as np
import optuna
import pandas as pd
from sklearn.model_selection import train_test_split
import xgboost as xgb
from merge_predictor_layers_info import *
from model_vis import *
import numpy as np
np.random.seed(42)

# Use MODEL_XG venv

# Specify FEATURE_NAMES of interest:
FEATURE_NAMES = ['vegetation_type', 'severity', 'burn_bndy_dist_km_upperbound', 
    'count_pixels_unburnedlowsev_matchveg_300mbuffer', 'count_burned_highsev_300mbuffer', 
    # '1yrpre_summer_maxtempz_avg', '1yrpost_summer_maxtempz_avg', 
    # '1yrpre_winter_maxtempz_avg', '1yrpost_winter_maxtempz_avg', 
    '1yrpre_pdsi_avg', '1yrpost_pdsi_avg', 
    '1yrpre_vpd_dry_anom', '1yrpost_vpd_dry_anom',
    'elevation', 'aspect', 'slope', 'wateryr_avg_pr_total']

CATEGORICAL_VARS = ['aspect', 'vegetation_type']#, 'hot_drought_categories']

def format_survival_data(df, feature_names=FEATURE_NAMES, categorical_vars=CATEGORICAL_VARS):
    df['Survival_label_lower_bound'] = df['recovery_time_yrs'].astype('float')
    df['Survival_label_upper_bound'] = df[['recovery_time_yrs', 'recovery_status_sampled']].apply(lambda s: 
        s['recovery_time_yrs'] if s['recovery_status_sampled']==1 else np.inf,
        axis=1)
    df = df.drop(['recovery_time_yrs', 'recovery_status_sampled'], axis=1)

    for col in feature_names:
        if '_anom' in col or 'maxtempz' in col:
            df[col] = np.round(df[col] * 10**-2) * 10**-1
            df = df[np.abs(df[col]) <= 10]

        if 'pdsi' in col:
            df[col] = df[col] * 10**-2

        if col not in categorical_vars:
            df[col] = df[col].astype(np.float32)

    # Update units
    df['burn_bndy_dist_km_upperbound'] = df['burn_bndy_dist_km_upperbound'] / 10 # convert to km (currently in 100s of meters)
    df['count_pixels_unburnedlowsev_matchveg_300mbuffer'] = df['count_pixels_unburnedlowsev_matchveg_300mbuffer'] * 10 # convert to count (currently in 10s of pixels)
    df['count_burned_highsev_300mbuffer'] = df['count_burned_highsev_300mbuffer'] * 10 # convert to count (currently in 10s of pixels)

    # Drop non-vegetated veg types
    df = df[(df['vegetation_type']!=7) & (df['vegetation_type']!=11)]
    
    for col in categorical_vars:
        df[col] = df[col].astype('category')

    all_cols = feature_names + ['Survival_label_lower_bound', 'Survival_label_upper_bound', 'uid']
    df = df[all_cols].dropna(axis=1, how='all')
    return df


# Read sampled points CSV
sampled_pts = gpd.read_file(os.path.join(output_dir, 'sampled_points_min_predictors_outcomes.gpkg'))
df = format_survival_data(sampled_pts, FEATURE_NAMES, CATEGORICAL_VARS)

# Plot distributions of sampled points
plot_distributions(df)

# Train model
# Split train, test by fireid
y_lower_bound = df['Survival_label_lower_bound']
y_upper_bound = df['Survival_label_upper_bound']
X = df[FEATURE_NAMES]
print("\n--- Input dtype summary ---")
for c in X.columns:
    if str(X[c].dtype).startswith("category"):
        print(f"{c}: categorical ({len(X[c].cat.categories)} cats)")
    else:
        print(f"{c}: {X[c].dtype}")

fire_ids = np.unique(df['uid'])
train_ids, test_ids = train_test_split(fire_ids, test_size=0.2, random_state=42)
train_index = df['uid'].isin(train_ids)
valid_index = df['uid'].isin(test_ids)
dtrain = xgb.DMatrix(X[train_index], enable_categorical=True, feature_names=list(X.columns))
dtrain.set_float_info('label_lower_bound', y_lower_bound[train_index])
dtrain.set_float_info('label_upper_bound', y_upper_bound[train_index])
dvalid = xgb.DMatrix(X[valid_index], enable_categorical=True, feature_names=list(X.columns))
dvalid.set_float_info('label_lower_bound', y_lower_bound[valid_index])
dvalid.set_float_info('label_upper_bound', y_upper_bound[valid_index])

# Print model results
# Define hyperparameter search space
base_params = {'verbosity': 0,
              'objective': 'survival:aft',
              'eval_metric': 'aft-nloglik',
              'tree_method': 'hist'}  # Hyperparameters common to all trials
def objective(trial):
    params = {'learning_rate': trial.suggest_loguniform('learning_rate', 0.01, 1.0),
              'aft_loss_distribution': trial.suggest_categorical('aft_loss_distribution',
                                                                  ['normal', 'logistic', 'extreme']),
              'aft_loss_distribution_scale': trial.suggest_loguniform('aft_loss_distribution_scale', 0.01, 10.0),
              'max_depth': trial.suggest_int('max_depth', 3, 8),
              'lambda': trial.suggest_loguniform('lambda', 1e-8, 1.0),
              'alpha': trial.suggest_loguniform('alpha', 1e-8, 1.0)}  # Search space
    params.update(base_params)
    pruning_callback = optuna.integration.XGBoostPruningCallback(trial, 'valid-aft-nloglik')
    bst = xgb.train(params, dtrain, num_boost_round=10000,
                    evals=[(dtrain, 'train'), (dvalid, 'valid')],
                    early_stopping_rounds=50, verbose_eval=False, callbacks=[pruning_callback])
    if bst.best_iteration >= 25:
        return bst.best_score
    else:
        return np.inf  # Reject models with < 25 trees

# Run hyperparameter search
study = optuna.create_study(direction='minimize')
study.optimize(objective, n_trials=200)
print('Completed hyperparameter tuning with best aft-nloglik = {}.'.format(study.best_trial.value))
params = {}
params.update(base_params)
params.update(study.best_trial.params)

# Re-run training with the best hyperparameter combination
print('Re-running the best trial... params = {}'.format(params))

# Store eval results
evals_result = {}
watchlist = [(dtrain, 'train'), (dvalid, 'eval')]

bst = xgb.train(params, dtrain, num_boost_round=10000,
                evals=watchlist, evals_result=evals_result,
                early_stopping_rounds=50)
print('dtrain columns')
print(dtrain.feature_names)
# Run prediction on the validation set
df_pred = pd.DataFrame({'Label (lower bound)': y_lower_bound[valid_index],
                   'Label (upper bound)': y_upper_bound[valid_index],
                   'Predicted label': bst.predict(dvalid)})
print(df_pred)
# Show only data points with right-censored labels
print(df_pred[np.isinf(df_pred['Label (upper bound)'])])

X_for_shap = X.copy()
cat_cols = X.select_dtypes(include=['category']).columns
categorical_mappings = {}
for col in CATEGORICAL_VARS:
    if col in X.columns:
        mapping = dict(enumerate(X[col].cat.categories))
        categorical_mappings[col] = mapping
        X_for_shap[col] = X_for_shap[col].cat.codes

# Save trained model
bst.save_model(os.path.join(output_dir, 'aft_best_model.json'))

# Save concordance index
# C-index: probability model correctly orders survival times
# Higher is better (0.5 = random, 1.0 = perfect)
predictions = bst.predict(dvalid)
c_index = concordance_index(y_lower_bound[valid_index], predictions, ~np.isinf(y_upper_bound[valid_index]))
print(f"C-index: {c_index}")  # Target: > 0.7 is decent, > 0.8 is good

# Visualize results
# call helper in model_vis.py
plots_dir = os.path.join(output_dir, 'xgboost_results') 
# plot_model_results(study, evals_result, bst, X, X_for_shap, y_lower_bound, FEATURE_NAMES, output_dir)


# Apply model to full map
predictors_path = 'data/exploratory_xgboost/predictor_layers/merged_predictors_clipped.tif'
output_predictions = 'data/exploratory_xgboost/model_results'
predict_raster(raster_path=predictors_path, 
        truey_path=None, model=bst, feature_names=FEATURE_NAMES, categorical_mappings = categorical_mappings,
        cat_cols=CATEGORICAL_VARS, output_dir=output_predictions)