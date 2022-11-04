### Dataset
Graduate Admission 2: https://www.kaggle.com/datasets/mohansacharya/graduate-admissions
### Results
Predictions for test data - `results/admission_test_predictions.csv`
Validation results - `results/cross_val.txt`
### Usage
```
sbt
run data/admission_train.csv data/admission_test.csv results/admission_test_predictions.csv
```
