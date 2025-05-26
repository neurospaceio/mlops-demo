import polars as pl
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix, recall_score, precision_score

# ======================================================================
# Get data and select features and label 
# ======================================================================

df = pl.read_csv("../data/predictive_maintenance.csv")

feature_cols = ["Air temperature [K]", "Process temperature [K]", "Rotational speed [rpm]", "Torque [Nm]"]
label_col = "Target"

X = df[feature_cols]
y = df[label_col]

# ======================================================================
# Get data and select features and label 
# ======================================================================

# Define parameter grid
param_grid = {
    'n_estimators': [10, 20, 30],
    'max_depth': [None, 10, 20, 30],
    'min_samples_split': [2, 5],
    'min_samples_leaf': [1, 2],
    'max_features': ['sqrt', 'log2']
}

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3)
clf = RandomForestClassifier(random_state=41)

# Set up GridSearchCV
grid_search = GridSearchCV(
    estimator=clf,
    param_grid=param_grid,
    cv=5,                # 5-fold cross-validation
    scoring='f1',        # or 'accuracy', 'precision', etc.
    n_jobs=-1,           # use all cores
    verbose=2
)

grid_search.fit(X_train, y_train)

best_model = grid_search.best_estimator_

y_pred =best_model.predict(X_test)
print("Accuracy: ", accuracy_score(y_test, y_pred))
print("Confusion: ", confusion_matrix(y_test, y_pred))
print("Recall: ", recall_score(y_test, y_pred))
print("Precision: ", precision_score(y_test, y_pred))

if False:
    y_pred = clf.predict(X_test)
    print("Accuracy: ", accuracy_score(y_test, y_pred))
    print("Confusion: ", confusion_matrix(y_test, y_pred))
    print("Recall: ", recall_score(y_test, y_pred))
    print("Precision: ", precision_score(y_test, y_pred))