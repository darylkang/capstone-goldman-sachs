import numpy as np
import pandas as pd
import progressbar as pb
import sys

columns = [
    'PREVAILING_WAGE',
    'PW_UNIT_OF_PAY',
    'PREVAILING_WAGE_CALCULATED',
    'WAGE_RATE_OF_PAY',
    'WAGE_UNIT_OF_PAY',
    'WAGE_RATE_OF_PAY_CALCULATED'
]

print('Loading Data...')
data = pd.read_csv('clean/all_clean_data.csv', usecols=columns, dtype=str)

data['WAGE_RATE_OF_PAY'] = pd.to_numeric(data['WAGE_RATE_OF_PAY'], errors='coerce')
data['PREVAILING_WAGE'] = pd.to_numeric(data['PREVAILING_WAGE'], errors='coerce')

print('Parsing Wages:')
sys.stdout.flush()
bar = pb.ProgressBar()
for i in bar(range(len(data))):
    x = data.at[i, 'WAGE_UNIT_OF_PAY']
    if x in ['H', 'W', 'B', 'M', 'Y']:
        pass
    else:
        data.at[i, 'WAGE_UNIT_OF_PAY'] = np.nan
        if x in ['Hour']:
            data.at[i, 'WAGE_UNIT_OF_PAY'] = 'H'
        if x in ['Week']:
            data.at[i, 'WAGE_UNIT_OF_PAY'] = 'W'
        if x in ['2 weeks']:
            data.at[i, 'WAGE_UNIT_OF_PAY'] = 'B'
        if x in ['Month']:
            data.at[i, 'WAGE_UNIT_OF_PAY'] = 'M'
        if x in ['Year', 'y']:
            data.at[i, 'WAGE_UNIT_OF_PAY'] = 'Y'
            
    x = data.at[i, 'WAGE_UNIT_OF_PAY']
    if x == 'H':
        data.at[i, 'WAGE_RATE_OF_PAY_CALCULATED'] = data.at[i, 'WAGE_RATE_OF_PAY'] * 12 * 4 * 40 * 8
    if x == 'W':
        data.at[i, 'WAGE_RATE_OF_PAY_CALCULATED'] = data.at[i, 'WAGE_RATE_OF_PAY'] * 12 * 4
    if x == 'B':
        data.at[i, 'WAGE_RATE_OF_PAY_CALCULATED'] = data.at[i, 'WAGE_RATE_OF_PAY'] * 12 * 2
    if x == 'M':
        data.at[i, 'WAGE_RATE_OF_PAY_CALCULATED'] = data.at[i, 'WAGE_RATE_OF_PAY'] * 12
    if x == 'Y':
        data.at[i, 'WAGE_RATE_OF_PAY_CALCULATED'] = data.at[i, 'WAGE_RATE_OF_PAY']
        
    x = data.at[i, 'PW_UNIT_OF_PAY']
    if x == 'H':
        data.at[i, 'PREVAILING_WAGE_CALCULATED'] = data.at[i, 'PREVAILING_WAGE'] * 12 * 4 * 40
    if x == 'W':
        data.at[i, 'PREVAILING_WAGE_CALCULATED'] = data.at[i, 'PREVAILING_WAGE'] * 12 * 4
    if x == 'B':
        data.at[i, 'PREVAILING_WAGE_CALCULATED'] = data.at[i, 'PREVAILING_WAGE'] * 12 * 2
    if x == 'M':
        data.at[i, 'PREVAILING_WAGE_CALCULATED'] = data.at[i, 'PREVAILING_WAGE'] * 12
    if x == 'Y':
        data.at[i, 'PREVAILING_WAGE_CALCULATED'] = data.at[i, 'PREVAILING_WAGE']

print('Updating Master CSV...')
original_data = pd.read_csv('clean/all_clean_data.csv', dtype=str)
original_data.drop(columns, axis=1, inplace=True)
original_data['WAGE_RATE_OF_PAY_CALCULATED'] = data['WAGE_RATE_OF_PAY_CALCULATED']
original_data['PREVAILING_WAGE_CALCULATED'] = data['PREVAILING_WAGE_CALCULATED']
original_data = original_data.reindex_axis(sorted(original_data.columns), axis=1)
original_data.to_csv('clean/all_clean_data.csv', index=False)
print('Done')
