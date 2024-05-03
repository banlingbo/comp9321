import pandas as pd
import numpy as np
from sklearn.preprocessing import PowerTransformer
import xgboost as xgb
from sklearn.feature_selection import SelectFromModel
from sklearn.metrics import mean_squared_error
from imblearn.over_sampling import SMOTE
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import f1_score, confusion_matrix, classification_report
def preprocess(df):
    df['age_of_car'] = df['age_of_car'].str.extract('(\d+) years').astype(float).fillna(0) * 12 \
                       + df['age_of_car'].str.extract('(\d+) months').astype(float).fillna(0)

    # Replace yes/no with 0/1
    bool_columns = [
        'is_esc', 'is_adjustable_steering', 'is_tpms', 'is_parking_sensors', 'is_parking_camera',
        'is_front_fog_lights', 'is_rear_window_wiper', 'is_rear_window_washer',
        'is_rear_window_defogger', 'is_brake_assist', 'is_power_door_locks', 'is_central_locking',
        'is_power_steering', 'is_driver_seat_height_adjustable', 'is_day_night_rear_view_mirror',
        'is_ecw', 'is_speed_alert'
    ]
    df[bool_columns] = df[bool_columns].apply(lambda x: x.map({'Yes': 1, 'No': 0}))

    # onehot
    categorical_cols = [
        'area_cluster', 'segment', 'model', 'engine_type', 'fuel_type',
        'transmission_type', 'steering_type', 'rear_brakes_type'
    ]
    df = pd.get_dummies(df, columns=categorical_cols)

    # Extract the values
    df[['torque_value', 'torque_rpm']] = df['max_torque'].str.extract(r'(\d+\.?\d*)Nm@(\d+)rpm').astype(float)
    df[['power_value', 'power_rpm']] = df['max_power'].str.extract(r'(\d+\.?\d*)bhp@(\d+)rpm').astype(float)
    # Remove original columns
    df.drop(['max_torque', 'max_power'], axis=1, inplace=True)
    df = df * 1
    return df

train_df = pd.read_csv('train.csv')
test_df=pd.read_csv("test.csv")
train_df=preprocess(train_df)
test_df=preprocess(test_df)

df1 = train_df.drop('Unnamed: 0', axis=1).drop(['policy_id'], axis=1).copy()
df2 = test_df.drop('Unnamed: 0', axis=1).drop(['policy_id'], axis=1).copy()

#---------------------------------------------------------------------------------#
# --------------------------------Part 1------------------------------#
#---------------------------------------------------------------------------------#

x_train1=df1.drop(['age_of_policyholder'], axis=1).copy()
y_train1=df1['age_of_policyholder']
x_test1=df2.drop(['age_of_policyholder'], axis=1).copy()
y_test1=df2['age_of_policyholder']

#use PowerTransformer
pt = PowerTransformer()
x_train_transformed = pt.fit_transform(x_train1)
x_test_transformed = pt.transform(x_test1)

#use best XGB model
best_params = {
    'subsample': 0.8,
    'n_estimators': 100,
    'min_child_weight': 1,
    'max_depth': 5,
    'learning_rate': 0.05,
    'colsample_bytree': 0.7,
    'objective': 'reg:squarederror',
    'random_state': 42
}
best_model = xgb.XGBRegressor(**best_params)
best_model.fit(x_train_transformed, y_train1)

# Extract the importance of features
importances = best_model.feature_importances_
feature_names = x_train1.columns
importance_dict = dict(zip(feature_names, importances))

# Create model selectors based on specific importance thresholds
threshold = 0.001
selector = SelectFromModel(best_model, threshold=threshold, prefit=True)

# Transform data by selector
x_train_selected = selector.transform(x_train_transformed)
x_test_selected = selector.transform(x_test_transformed)

# retrain a new model
selected_model = xgb.XGBRegressor(**best_params)
selected_model.fit(x_train_selected, y_train1)

# predict in test data
y_pred_selected = selected_model.predict(x_test_selected)
mse_selected = mean_squared_error(y_test1, y_pred_selected)
print(f"Mean Squared Error with selected features: {mse_selected:.2f}")
y_pred_int = np.round(y_pred_selected).astype(int)
output_df = pd.DataFrame({
    'policy_id': test_df['policy_id'],
    'age': y_pred_int
})

# output CSV file
output_file_name = 'zxxxxxx.PART1.output.csv'
output_df.to_csv(output_file_name, index=False)

#---------------------------------------------------------------------------------#
# --------------------------------Part 2------------------------------#
#---------------------------------------------------------------------------------#

x_train2 = df1.drop(['is_claim'], axis=1)
y_train2 = df1['is_claim']
x_test2 = df2.drop(['is_claim'], axis=1)
y_test2 = df2['is_claim']

smote = SMOTE(random_state=42)
# Apply SMOTE oversampling
x_train2_resampled, y_train2_resampled = smote.fit_resample(x_train2, y_train2)

param_dist = {
    'subsample': 0.7,
    'n_estimators': 300,
    'min_samples_split': 5,
    'min_samples_leaf': 1,
    'max_depth': 6,
    'learning_rate': 0.1
}
# Initializing a Gradient Booster Classifier
gbm_classifier = GradientBoostingClassifier(**param_dist)
# Training on an oversampled training set
gbm_classifier.fit(x_train2_resampled, y_train2_resampled)
# Predictions on the original test set
y_pred_gbm = gbm_classifier.predict(x_test2)

# Calculating Macro  F1 Scores
macro_f1 = f1_score(y_test2, y_pred_gbm, average='macro')
print(f'Macro F1 Score: {macro_f1:.2f}')
output_df = pd.DataFrame({
    'policy_id': test_df['policy_id'],
    'is_claim': y_pred_gbm
})

# output CSV file
output_file_name = 'zxxxxxx.PART2.output.csv'
output_df.to_csv(output_file_name, index=False)
