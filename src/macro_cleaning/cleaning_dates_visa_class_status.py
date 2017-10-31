import numpy as np
import pandas as pd
from datetime import date
import os

print("Reading clean data")
data = pd.read_csv('../clean/all_clean_data.csv', encoding='utf-8', dtype=str)


if not os.path.exists('../clean/macro'):
    os.mkdir('../clean/macro')

def update_visa_class(x):
    if pd.to_datetime(x).year<2007:
        return 'H-1B'
    else:
        return np.nan

print("Basic fixes to VISA_CLASS")
#Fix remaining VISA_CLASS issues (remove Australian, make uppercase)
data['VISA_CLASS']=data.VISA_CLASS.str.upper().str.replace("AUSTRALIAN","").str.strip()

print("Updating CASE_STATUS")
#Fix inconsistent CASE_STATUSes (someone forgot to add WITHDRAWN)
data["CASE_STATUS"]=data.apply(lambda x: x.CASE_STATUS+"-WITHDRAWN" if x.WITHDRAWN=="Y" else x.CASE_STATUS, axis=1)

print("Updating CASE_SUBMITTED")
#Update CASE_SUBMITTED with DECISION_DATE if empty
data["CASE_SUBMITTED"]=data.apply(lambda x: x.DECISION_DATE if pd.isnull(x.CASE_SUBMITTED) and not pd.isnull(x.DECISION_DATE) else x.CASE_SUBMITTED, axis=1)

print("Updating VISA_CLASS")
#All Pre-2007 VISA_CLASS is H-1B
data["VISA_CLASS"]=data.apply(lambda x: update_visa_class(x.DECISION_DATE) if pd.isnull(x.VISA_CLASS) else x.VISA_CLASS, axis=1)

print("Filtering and De-Duping Data")
#Get only certified data
certified_data = data.query('CASE_STATUS=="CERTIFIED"')

#Sort by dates
certified_sorted_data = certified_data.sort_values(["CASE_SUBMITTED","DECISION_DATE"],ascending=True)

#Dedupe, keeping only the latest date
certified_sorted_deduped_data = certified_sorted_data.drop_duplicates(["CASE_NUMBER","CASE_STATUS","EMPLOYER_NAME","TOTAL_WORKERS","WAGE_RATE_OF_PAY"], keep='last')

print("Writing deduped, filtered, clean data!")
#Write to file
certified_sorted_deduped_data.to_csv("../clean/macro/cleaned_data_fixed_visa_dates_status.csv",index=False, encoding='utf-8')
