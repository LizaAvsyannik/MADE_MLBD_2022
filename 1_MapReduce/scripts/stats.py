import pandas as pd

data = pd.read_csv('data/AB_NYC_2019.csv')
assert data['price'].isna().sum() == 0

with open('results/mean_var_pandas.txt', 'w') as f:
    f.write(f"{data['price'].mean()} {data['price'].var(ddof=0)}")
