import pandas as pd
import numpy as np

data = pd.read_csv("../clean/macro/master_macro_cleaned_naics.csv",encoding='utf-8',dtype=str)

reduced_columns = data[['CASE_SUBMITTED', 'EMPLOYER_STATE', 'WORKSITE_STATE', 'NAICS_CLASSIFICATION', 'TOTAL_WORKERS']]

reduced_columns['CASE_SUBMITTED']=pd.to_datetime(reduced_columns['CASE_SUBMITTED'],format='%Y-%m-%d')

reduced_years = reduced_columns[(reduced_columns.CASE_SUBMITTED >= '2009-07-01')]

reduced_years = reduced_years.dropna()

reduced_years.to_csv("../clean/macro/post2009_naics.csv",index=False, encoding='utf-8')

print(reduced_years.shape)