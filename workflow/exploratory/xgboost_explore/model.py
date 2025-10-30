import numpy as np
import optuna
import pandas as pd
from sklearn.model_selection import train_test_split
import xgboost as xgb
from merge_predictor_layers_info import *
from model_vis import *


# Read sampled points CSV
sampled_pts = gpd.read_file(os.path.join(output_dir, 'sampled_points_allpredictors_outcomes.shp'))

def format_survival_data(df, feature_names=FEATURE_NAMES):
    df['Survival_label_lower_bound'] = df['recovery_time_yrs'].astype('float')
    df['Survival_label_upper_bound'] = df[['recovery_time_yrs', 'recovery_status_sampled']].apply(lambda s: 
        s['recovery_time_yrs'] if s['recovery_status_sampled']==1 else np.inf,
        axis=1)
    df = df.drop(['recovery_time_yrs', 'recovery_status_sampled'], axis=1)
    categorical_vars = ['aspect', 'vegetation_type', 'hot_drought_categories']
    for col in categorical_vars:
        df[col] = df[col].astype('category')

    all_cols = feature_names + ['Survival_label_lower_bound', 'Survival_label_upper_bound']
    return df[all_cols]

df = format_survival_data(sampled_pts)

# Plot distributions of sampled points
plot_distributions(sampled_pts)

# Train model
# Split train, test by fireid
y_lower_bound = df['Survival_label_lower_bound']
y_upper_bound = df['Survival_label_upper_bound']
X = df[FEATURE_NAMES]
fire_ids = np.unique(df['uid'])
train_ids, test_ids = train_test_split(fire_ids, test_size=0.2, random_state=42)
train_index = df['uid'].isin(train_ids)
valid_index = df['uid'].isin(test_ids)

dtrain = xgb.DMatrix(X.iloc[train_index, :], enable_categorical=True)
dtrain.set_float_info('label_lower_bound', y_lower_bound[train_index])
dtrain.set_float_info('label_upper_bound', y_upper_bound[train_index])
dvalid = xgb.DMatrix(X.iloc[valid_index, :], enable_categorical=True)
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

# Run prediction on the validation set
df_pred = pd.DataFrame({'Label (lower bound)': y_lower_bound[valid_index],
                   'Label (upper bound)': y_upper_bound[valid_index],
                   'Predicted label': bst.predict(dvalid)})
print(df_pred)
# Show only data points with right-censored labels
print(df_pred[np.isinf(df_pred['Label (upper bound)'])])

# Save trained model
bst.save_model('aft_best_model.json')

# Visualize results
# call helper in model_vis.py
plot_model_results(study, bst, X)


# Apply model to full map
predict_raster_with_nodata(predictors_path, truey_path, bst, FEATURE_NAMES, cat_cols, output_path='predictions.tif', nodata_value=-9999)