import numpy as np
import pandas as pd
import time
from os import path
import glob
from datetime import datetime
from zipfile import ZipFile

import warnings
warnings.filterwarnings('ignore')

tic = time.time()
print("write data to csv....")
with open('data_02.csv', 'w+', encoding = 'utf-8') as f:
    HEADER = ','.join([
        'CASE_SUBMITTED', # Submitted_Data
        'CASE_NUMBER', # Case_No
        'EMPLOYER_NAME', # Name
        'EMPLOYER_ADDRESS', # Address
        'EMPLOYER_CITY', # City
        'EMPLOYER_STATE', # State
        'EMPLOYER_POSTAL_CODE', # Postal_Code
        'TOTAL_WORKERS', # Nbr_Immigrants
        'JOB_TITLE', # Job_Title
        'DECISION_DATE', # Dol_Decision_Date
        'DOT_CODE', # Job_Code
        'CASE_STATUS', # Approval_Status
        'WAGE_UNIT_OF_PAY', # Rate_Per_1
        'WAGE_RATE_OF_PAY', # Max_Rate_1
        'FULL_TIME_POSITION', # Part_Time_1
        'WORKSITE_CITY', # City_1
        'WORKSITE_STATE', # State_1
        'PREVAILING_WAGE', # Prevailing_Wage_1
        'VISA_CLASS', # Program (2007)
        'WITHDRAWN', # Withdrawn (2007)

        # NA Columns
        'EMPLOYER_AREA_CODE',
        'PW_UNIT_OF_PAY',
        'SOC_CODE',
        'SOC_NAME',
        'NAIC_CODE',
        'WORKSITE_POSTAL_CODE',
        'WAGE_RATE_OF_PAY_CALCULATED',
        'PREVAILING_WAGE_CALCULATED',
    ])

    f.write(HEADER + '\n')
    # Write txt files to csv
    file_root = 'raw_data/'
    files = glob.glob(file_root + '/*.zip')
    efiles = [file for file in files if 'efile' in file]
    efiles = efiles[:-2]
    num_data = []
    for efile in efiles:
        with ZipFile(efile) as ZIP:
            file_names = ZIP.namelist()
            data_len = 0
            for file_name in file_names:
                if file_name.endswith('.txt') and file_name.startswith('H1B'):
                    with ZIP.open(file_name) as file:
                        if file_name.endswith('Efile.txt'):
                            data = pd.read_csv(
                                file,
                                dtype=str,
                                encoding="ISO-8859-1",
                                header=None)
                        else:
                            data = pd.read_csv(
                                file,
                                dtype=str,
                                encoding="ISO-8859-1")
                        data['PROGRAM'] = np.nan
                        data['WITHDRAWN'] = np.nan
                        data = pd.concat([
                            data.iloc[:, 0:4],
                            data.iloc[:, 5:9],
                            data.iloc[:, 11:13],
                            data.iloc[:, 15:17],
                            data.iloc[:, 18:24],
                            data['PROGRAM'],
                            data['WITHDRAWN'],
                            pd.DataFrame(columns=list(range(8)))
                        ], axis=1)
                        data.to_csv(f, header=False, index=False, encoding='utf-8')
                        data_len += len(data)
        print('total data for {}: {}'.format(efile, data_len))
        num_data.append(data_len)

data = pd.read_csv('data_02.csv', dtype=str)

print('clean data...')
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

'''
data['EMPLOYER_AREA_CODE'] = [
    str(i)[:3] if len(str(i)) == 10 else np.nan for i in data['EMPLOYER_AREA_CODE']
]
'''

data['EMPLOYER_POSTAL_CODE'] = [
    i if len(str(i)) == 5 else np.nan for i in data['EMPLOYER_POSTAL_CODE'].str.replace('\s', '')
]

print("change wage rate of pay to numeric...")
data['WAGE_RATE_OF_PAY'] = pd.to_numeric(data['WAGE_RATE_OF_PAY'], errors='coerce')

print("change prevailing wage to numeric...")
data['PREVAILING_WAGE'] = pd.to_numeric(data['PREVAILING_WAGE'], errors='coerce')

print('change worksite state to string...')
data[u'WORKSITE_STATE'] = pd.to_numeric(data['WORKSITE_STATE'], errors='coerce')


print('clean dot code...')
data['DOT_CODE'] = [
    i if (not np.isnan(int(i))) and int(i) <= 198 else np.nan for i in data['DOT_CODE']
]

## Original daataset is for part time, so switch values
print('clean full time position...')
data.FULL_TIME_POSITION.replace(to_replace = dict(N = 'YY', Y = 'NN'), inplace = True)
data.FULL_TIME_POSITION.replace(to_replace = dict(YY = 'Y', NN = 'N'), inplace = True)

'''
data['PW_UNIT_OF_PAY'].str.upper()
data['PW_UNIT_OF_PAY'] = [
    i if i in ['Y', 'M', 'B', 'W', 'H'] else np.nan for i in data['PW_UNIT_OF_PAY']
]
'''

print('clean case submitted...')
data['CASE_SUBMITTED'] = data['CASE_SUBMITTED'].str.replace(' 0:00:00', '')
data['CASE_SUBMITTED'] = pd.to_datetime(data['CASE_SUBMITTED'], errors='coerce').dt.strftime('%Y-%m-%d')
data['CASE_SUBMITTED'][data['CASE_SUBMITTED'] == 'NaT'] = np.nan

print('clean decision date...')
data['DECISION_DATE'] = data['DECISION_DATE'].str.replace(' 0:00:00', '')
data['DECISION_DATE'] = pd.to_datetime(data['DECISION_DATE'], errors='coerce').dt.strftime('%Y-%m-%d')
data['DECISION_DATE'][data['DECISION_DATE'] == 'NaT'] = np.nan

print('sort columns...')
data = data.reindex_axis(sorted(data.columns), axis=1)

#print('drop withdrawn column')
#data.drop('WITHDRAWN', axis=1, inplace=True)

data['DOT_NAME'] = np.nan
idx_from = 0
idx_to = 0
for i in range(len(num_data)):
    idx_to += num_data[i]
    print('write data from {} to {}...'.format(idx_from, idx_to))
    data.iloc[idx_from:idx_to].to_csv('../clean/200{}_efile.csv'.format(i+2), index=False, encoding='utf-8')
    idx_from = idx_to
print("done")
toc = time.time()
print("process time:", round((toc - tic) / 60, 2), 'minutes')
