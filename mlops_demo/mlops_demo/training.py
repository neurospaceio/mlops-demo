from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix, recall_score, precision_score
import polars as pl
import dagster as dg
from modelcards import ModelCard
import datetime
import pyarrow
from codecarbon import EmissionsTracker

asset_groups = {
    "ingestion": "data_ingestion",
    "training": "model_training",
    "inference": "model_inference"
}

train_fraction = 0.7
test_fraction = 1 - train_fraction

cleaned_readings_fn = "cleaned_readings.jsonl"
cleaned_daily_statuses_fn = "cleaned_daily_statuses.jsonl"

@dg.asset(
    description="Overlap between readings and statuses",
    group_name=asset_groups["training"],
    kinds={"polars"},
)
def joined_cleaned_readings_and_statuses(context: dg.AssetExecutionContext,  historical_readings: int, historical_daily_statuses: int) -> pl.DataFrame:

    with open(cleaned_readings_fn, "r") as f:
        df_readings = pl.read_ndjson(f)
    with open(cleaned_daily_statuses_fn, "r") as f:
        df_statuses = pl.read_ndjson(f)

    df = df_readings.join(df_statuses, on="UDI", how="inner")

    context.add_output_metadata({
        "num_matches": len(df)
    })

    return df


@dg.asset(
    description="The training part of the dataset",
    group_name=asset_groups["training"],
)
def training_data(context: dg.AssetExecutionContext, joined_cleaned_readings_and_statuses: pl.DataFrame) -> pl.DataFrame:

    upper_index = int(len(joined_cleaned_readings_and_statuses)*train_fraction)
    df_train = joined_cleaned_readings_and_statuses[0:upper_index]

    num_train_rows = df_train.height
    num_failures = df_train.filter(pl.col("Target") == 1).height
    num_non_failures = df_train.filter(pl.col("Target") == 0).height
    context.add_output_metadata({
        "num_train_rows": num_train_rows,
        "num_failures": num_failures,
        "num_non_failures": num_non_failures
    })

    return df_train
    
    
@dg.asset(
    description="The test part of the dataset",
    group_name=asset_groups["training"],
)
def test_data(context: dg.AssetExecutionContext, joined_cleaned_readings_and_statuses: pl.DataFrame) -> pl.DataFrame:
    lower_index = int(len(joined_cleaned_readings_and_statuses)*train_fraction)
    df_test = joined_cleaned_readings_and_statuses[lower_index:]

    num_test_rows = df_test.height
    num_failures = df_test.filter(pl.col("Target") == 1).height
    num_non_failures = df_test.filter(pl.col("Target") == 0).height
    context.add_output_metadata({
        "num_train_rows": num_test_rows,
        "num_failures": num_failures,
        "num_non_failures": num_non_failures
    })

    return df_test


@dg.asset(
    description="The last trained model (not the production model)",
    group_name=asset_groups["training"]
)
def trained_model(context: dg.AssetExecutionContext, training_data: pl.DataFrame, test_data: pl.DataFrame) -> RandomForestClassifier:

    feature_cols = ["Air temperature [K]", "Process temperature [K]", "Rotational speed [rpm]", "Torque [Nm]"]
    label_col = "Target"

    X_train= training_data[feature_cols]
    y_train = training_data[label_col]
    X_test = test_data[feature_cols]
    y_test = test_data[label_col]

    tracker = EmissionsTracker()
    
    tracker.start_task("Train model")
    param_grid = {
        'n_estimators': [10, 30],
        'max_depth': [None, 15, 30],
        'min_samples_split': [2, 5],
        'min_samples_leaf': [1, 2],
        'max_features': ['sqrt', 'log2']
    }

    clf = RandomForestClassifier(random_state=41, verbose=1)

    grid_search = GridSearchCV(
        estimator=clf,
        param_grid=param_grid,
        cv=3,                # 5-fold cross-validation
        scoring='precision',        # or 'accuracy', 'precision', etc.
        verbose=2
    )

    grid_search.fit(X_train, y_train)

    best_model = grid_search.best_estimator_
    emissions = tracker.stop()
    co2 = emissions * 99

    # train and test accuracy
    yhat_train = best_model.predict(X_train)
    train_acc = accuracy_score(y_train, yhat_train)

    y_pred = best_model.predict(X_test)
    test_acc = accuracy_score(y_test, y_pred)

    # get test confusion matrix
    test_confm = confusion_matrix(y_test, y_pred)
    scores = {
        "accuracy": accuracy_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred),
    }

    # get markdown sample of data
    X_train_pd = X_train.to_pandas()
    y_train_pd = y_train.to_pandas()

    context.add_asset_metadata({
        "Classes": dg.MetadataValue.json(best_model.classes_.tolist()),
        "Accuracy": scores["accuracy"],
        "Recall": scores["recall"],
        "Precision": scores["precision"],
    })

     # create model cards
    model_cards_content = f"""---
language: en
license: Neurospace ApS
---
# Model Card for Predictive Maintenance on Component 7

## Model Description
This model estimates weather the given component has failed or not,
based on input data from {feature_cols}

## Development Team

| Development Information  | Text                                               |
| -------------------------|----------------------------------------------------|
| Development Team         | René Petersen                                      |
| Development Organisation | Neurospace ApS <br> Nydamsvej 17 <br> 8362 Hørning |
| Customer Team            | Answer is always 42                                |
| Customer Organisation    | 42 Data Stream Boulevard, Innova City, CA 90210    |
| Supporting e-mail        | support@abc.dk                                     |

## Intended Use

| Text                      | Information                                 |
|---------------------------|---------------------------------------------|
| Primarily Intended Use    | Predictive maintenance on component 7       |
| Primarily Intended Users  | Operators in control room                   |
| Out of Scope applications | Predictive maintenance on other components  |

## Model Details

| Model details | Text |
|---------------|------|
| Model Date | {datetime.datetime.now().date().strftime("%d/%m/%Y")} |
| Model Type | RandomForestClassifier |
| Model Version | 0.0.1 |
| Electricity used for training model | {emissions} kWh |
| Estimated CO2 esmissions | {co2} gram |
| Scheduled Retraining date | 2025/08/10 |
| Expected lifetime | 2027/10/2 |

### Model Architecture
![RandomForestArchitecture](img/Figure_1.png)


| Parameter   | Params                                                       |
|-------------|--------------------------------------------------------------|
| n_estimator | {grid_search.best_params_.get("n_estimators")}               |
| max_depth   | {grid_search.best_params_.get("max_depth")}                  |
| min_samples_split   | {grid_search.best_params_.get("min_samples_split")}  |
| min_samples_leaf   | {grid_search.best_params_.get("min_samples_leaf")}    |
| max_features   | {grid_search.best_params_.get("max_features")}            |

## Training Data
The model is trained on {X_train.shape[0]} observations, and tested on {X_test.shape[0]} observations

Model is dependent on the following features:
{X_train.columns}

### Example input data
{X_train_pd.head(10).to_markdown(index=False)}

### Example output data (0 is good, 1 is failure)
{y_train_pd.head(10).to_markdown(index=False)}

## Training/Test split

| Dataset  | Number             | Percentage of dataset                                                   |
|----------|--------------------|-------------------------------------------------------------------------|
| Training | {X_train.shape[0]} | {(X_train.shape[0] / (X_train.shape[0] + X_test.shape[0])) * 100:.2f}%  |
| Test     | {X_test.shape[0]}  | {(X_test.shape[0] / (X_train.shape[0] + X_test.shape[0])) * 100:.2f}%   |


## Number of representatives in train and test

| Dataset  | Number of normal components          | Number of failure components       |
|----------|--------------------------------------|------------------------------------|
| Training | {len(y_train.filter(y_train == 0))}  | {len(y_train.filter(y_train == 1))} |
| Test     | {len(y_test.filter(y_test == 0))}    | {len(y_test.filter(y_test == 1))}   |


### Bias in training data

| Source       | Bias |
|--------------|------|
| Input Data   | [ ]  |
| Data Leakage | [ ]  |
| Labeling     | [ ]  |
| Test Data    | [ ]  |
| Evaluation   | [ ]  |

Bias-effect is considered Neutral, as sensor data is expected to be biased with the same amount of noise.

## Validation of model
The table below presents the accuracy of the training and test data

| Dataset | Accuracy               |
|---------|------------------------|
| Train   | {train_acc * 100:.2f}% |
| Test    | {test_acc * 100:.2f}%  |

Below is presented Test Confusion Matrix


|                  | True Normal        | True Failure       |
|------------------|--------------------|--------------------|
|Predicted Normal  | {test_confm[0][0]} | {test_confm[1][0]} |
|Predicted Failure | {test_confm[0][1]} | {test_confm[1][1]} |

## Metrics and Limitations

**When component 7 is under service**


**Faulty input data**

"""

    model_card = ModelCard(content=model_cards_content)
    model_card.save("MODEL_CARD.md")
    model_card.save("docs/model_card.md")

    return best_model

train_ml_model_job = dg.define_asset_job(
    "train_ml_model",
    selection=[joined_cleaned_readings_and_statuses, training_data, test_data, trained_model]
)

train_ml_model_schedule = dg.ScheduleDefinition(
    job=train_ml_model_job,
    cron_schedule="*/10 * * * *"
)
