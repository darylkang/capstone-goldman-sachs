import pandas as pd

WAGES = [
    'MAX_RATE', # All NA's
    'MAX_RATE_2',
    'PREVAILING_WAGE',
    'PREVAILING_WAGE_2',
    'PW_SOURCE_OTHER', # Further Discussion
    'PW_SOURCE_YEAR', # Further Discussion
    'PW_SOURCE_YEAR_2', # Possibly Wrong Column
    'PW_UNIT_OF_PAY', # Further Discussion
    'PW_UNIT_OF_PAY_2', # Further Discussion
    'PW_WAGE_LEVEL',
    'PW_WAGE_SOURCE', # Possibly Wrong Column
    'PW_WAGE_SOURCE_2', # Possibly Wrong Column
    'PW_WAGE_SOURCE_OTHER_2', # Further Discussion
    'WAGE_RATE_OF_PAY', # Split into WAGE_RATE_OF_PAY_A and WAGE_RATE_OF_PAY_B
    'WAGE_RATE_OF_PAY_2',
    'WAGE_RATE_OF_PAY_FROM',
    'WAGE_RATE_OF_PAY_TO',
    'WAGE_UNIT_OF_PAY', # Further Discussion
    'WAGE_UNIT_OF_PAY_2' # Possibly Wrong Column
]

data = pd.read_csv('H1B_Full_Data.csv', usecols=WAGES, dtype=str)

WAGE_RATE_OF_PAY_A = []
WAGE_RATE_OF_PAY_B = []

for line in data['WAGE_RATE_OF_PAY']:
    try:
        a, b = line.split('-')
        WAGE_RATE_OF_PAY_A.append(a)
        WAGE_RATE_OF_PAY_B.append(b)
    except:
        WAGE_RATE_OF_PAY_A.append('NaN')
        WAGE_RATE_OF_PAY_B.append('NaN')

data.drop('WAGE_RATE_OF_PAY', axis=1, inplace=True)

data['WAGE_RATE_OF_PAY_A'] = WAGE_RATE_OF_PAY_A
data['WAGE_RATE_OF_PAY_B'] = WAGE_RATE_OF_PAY_B

NO_ISSUE = [
    'MAX_RATE_2',
    'PREVAILING_WAGE',
    'PREVAILING_WAGE_2',
    'WAGE_RATE_OF_PAY_A',
    'WAGE_RATE_OF_PAY_B',
    'WAGE_RATE_OF_PAY_2',
    'WAGE_RATE_OF_PAY_FROM',
    'WAGE_RATE_OF_PAY_TO'
]

print('Columns with no issues:')
for i in NO_ISSUE:
    print(i)

for column in NO_ISSUE:
    data[column] = data[column].str.replace(r'[^\d\.]', '')
    data[column] = pd.to_numeric(data[column], errors='coerce')

data.to_csv('wages.csv', index=False)

ISSUE = [
    'PW_SOURCE_OTHER', # Further Discussion
    'PW_SOURCE_YEAR', # Further Discussion
    'PW_SOURCE_YEAR_2', # Possibly Wrong Column
    'PW_UNIT_OF_PAY', # Further Discussion
    'PW_UNIT_OF_PAY_2', # Further Discussion
    'PW_WAGE_SOURCE', # Possibly Wrong Column
    'PW_WAGE_SOURCE_2', # Possibly Wrong Column
    'PW_WAGE_SOURCE_OTHER_2', # Further Discussion
    'WAGE_UNIT_OF_PAY', # Further Discussion
    'WAGE_UNIT_OF_PAY_2' # Possibly Wrong Column
]

print('Columns for further discussion:')
for i in ISSUE:
    print(i)