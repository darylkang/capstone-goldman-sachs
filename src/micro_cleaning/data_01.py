import numpy as np
import pandas as pd
import os
import time
import sys
sys.path.insert(0, 'clean')

from datetime import datetime
from zipfile import ZipFile

import warnings
warnings.filterwarnings('ignore')

tic = time.time()
print('Wait...')

with open('data_01.csv', 'w+') as f:
    HEADER = ','.join([
        'CASE_NUMBER', # C_num
        'CASE_STATUS', # CertCode
        'EMPLOYER_AREA_CODE', # ReturnFax (Derived)
        'EMPLOYER_NAME', # EmpName
        'EMPLOYER_CITY', # EmpCity
        'EMPLOYER_ADDRESS', # EmpAddy1
        'EMPLOYER_STATE', # EmpState
        'EMPLOYER_POSTAL_CODE', # EmpZip
        'WageRateFrom',
        'WageRateTo',
        'WAGE_UNIT_OF_PAY', # RatePer
        'FULL_TIME_POSITION', # PartTime
        'DOT_CODE', # JobCode
        'TOTAL_WORKERS', # NumImmigrants
        'JOB_TITLE', # JobTitle
        'WORKSITE_CITY', # WorkCity_1
        'WORKSITE_STATE', # WorkState_1
        'PREVAILING_WAGE', # PrevWage_1
        'PW_UNIT_OF_PAY', # PrevWagePer_1
        'CASE_SUBMITTED', # DateSigned (2004 Only)
        'DECISION_DATE', # ProcessDate
        
        # NA Columns
        'VISA_CLASS',
        'SOC_CODE',
        'SOC_NAME',
        'NAIC_CODE',
        'WORKSITE_POSTAL_CODE',
        'WAGE_RATE_OF_PAY_CALCULATED',
        'PREVAILING_WAGE_CALCULATED',
        'WITHDRAWN' # Delete
    ])
    f.write(HEADER + '\n')
    for i in range(1, 6):
        with ZipFile('/'.join(['raw_data', 'H1B_fax_FY200{}_text.zip'.format(i)])) as ZIP:
            if i != 4:
                file = '_'.join(['H1B_FAX' if i == 2 else 'H1B_Fax', 'FY200{}_Download.txt'])
            else:
                file = 'H1B_fax_FY0{}.txt'
            with ZIP.open(file.format(i)) as file:
                data = pd.read_csv(file, dtype=str)
            if i != 4:
                data['DateSigned'] = np.nan
            data = pd.concat([
                data.iloc[:,  0:6 ],
                data.iloc[:,  7:13],
                data.iloc[:, 15:22],
                data['DateSigned'],
                data.iloc[:, 35],
                pd.DataFrame(columns=list(range(8)))
            ], axis=1)
#             data = data.loc[data['CertCode'] == 'Certified']
            data.to_csv(f, header=False, index=False)

data = pd.read_csv('data_01.csv', dtype=str)

x = [
    'CASE_STATUS',
    'JOB_TITLE',
    'EMPLOYER_ADDRESS',
    'EMPLOYER_CITY',
    'EMPLOYER_STATE',
    'WORKSITE_CITY',
    'WORKSITE_STATE'
]
for i in x:
    data[i] = data[i].str.replace(r'[^\w\s]', '').str.upper()

data['EMPLOYER_AREA_CODE'] = [
    str(i)[:3] if len(str(i)) == 10 else np.nan for i in data['EMPLOYER_AREA_CODE']
]

data['EMPLOYER_POSTAL_CODE'] = [
    i if len(str(i)) == 5 else np.nan for i in data['EMPLOYER_POSTAL_CODE'].str.replace('\s', '')
]

x = [
    'WageRateFrom',
    'WageRateTo'
]
for i in x:
    data[i] = pd.to_numeric(data[i], errors='coerce')
z = []
for i in range(len(data)):
    W0 = data['WageRateFrom'][i]
    W1 = data['WageRateTo'][i]
    if np.isnan(W0) or np.isnan(W1) or W1 < W0:
        z.append(W0)
    else:
        z.append(np.mean([W0, W1]))
data['WAGE_RATE_OF_PAY'] = z
for i in x:
    data.drop(i, axis=1, inplace=True)

print('Keep waiting...')

data['DOT_CODE'] = [
    i if len(str(i)) == 3 else np.nan for i in data['DOT_CODE']
]

data['FULL_TIME_POSITION'] = (data['FULL_TIME_POSITION'] == 'N') + 0

data['PW_UNIT_OF_PAY'].str.upper()
data['PW_UNIT_OF_PAY'] = [
    i if i in ['Y', 'M', 'B', 'W', 'H'] else np.nan for i in data['PW_UNIT_OF_PAY']
]

data['CASE_SUBMITTED'] = pd.to_datetime(data['CASE_SUBMITTED'], errors='coerce').dt.strftime('%Y-%m-%d')
data['CASE_SUBMITTED'][data['CASE_SUBMITTED'] == 'NaT'] = np.nan

data['DECISION_DATE'] = data['DECISION_DATE'].str.replace(' 0:00:00', '')
data['DECISION_DATE'] = pd.to_datetime(data['DECISION_DATE'], errors='coerce').dt.strftime('%Y-%m-%d')
data['DECISION_DATE'][data['DECISION_DATE'] == 'NaT'] = np.nan

data.drop('WITHDRAWN', axis=1, inplace=True)
data['DOT_NAME'] = np.nan

data = data.reindex_axis(sorted(data.columns), axis=1)

if not os.path.exists('../clean'):
    os.mkdir('../clean')
# data.to_csv('data_01.csv', index=False)
data.iloc[0:288392].to_csv('../clean/2001_fax.csv', index=False)
data.iloc[288392:435139].to_csv('../clean/2002_fax.csv', index=False)
data.iloc[435139:482513].to_csv('../clean/2003_fax.csv', index=False)
data.iloc[482513:514010].to_csv('../clean/2004_fax.csv', index=False)
data.iloc[514010:523945].to_csv('../clean/2005_fax.csv', index=False)
os.remove('data_01.csv')

print("K, it's done...")
toc = time.time()
print("You've waited:", round((toc - tic) / 60, 2), 'minutes')