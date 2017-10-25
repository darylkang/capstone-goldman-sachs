import pandas as pd
import numpy as np
import os

import utils #File in current directory

files = [('yr2009_new', 'Icert_%20LCA_%20FY2009.xlsx'), 
         ('yr2011', 'H-1B_iCert_LCA_FY2011_Q4.xlsx'), 
         ('yr2012', 'LCA_FY2012_Q4.xlsx'), 
         ('yr2013', 'LCA_FY2013.xlsx'), 
         ('yr2014', 'H-1B_FY14_Q4.xlsx')]

if not os.path.exists('clean'):
        os.mkdir('clean')

originalDfs = []

for clean, inputFn in files:
    print('Loading ', clean)
    data = pd.read_excel('data/' + inputFn)
    data.columns = ['CASE_NUMBER', 'CASE_STATUS','CASE_SUBMITTED','DECISION_DATE', 'VISA_CLASS', 
    'EMPLOYMENT_START_DATE', 'EMPLOYMENT_END_DATE', 'EMPLOYER_NAME','EMPLOYER_ADDRESS','EMPLOYER_CITY','EMPLOYER_STATE','EMPLOYER_POSTAL_CODE',
    'SOC_CODE','SOC_NAME','JOB_TITLE','WAGE_RATE_OF_PAY_FROM','WAGE_RATE_OF_PAY_TO','WAGE_UNIT_OF_PAY','FULL_TIME_POSITION',
    'TOTAL_WORKERS','WORKSITE_CITY','WORKSITE_STATE','PREVAILING_WAGE','PW_UNIT_OF_PAY','PW_WAGE_SOURCE','PW_SOURCE_OTHER','PW_SOURCE_YEAR',
    'WORKSITE_CITY_2','WORKSITE_STATE_2','PREVAILING_WAGE_2','PW_UNIT_OF_PAY_2','PW_WAGE_SOURCE_2','PW_WAGE_SOURCE_OTHER_2','PW_SOURCE_YEAR_2',
    'NAIC_CODE']

    #Format dates
    data['CASE_SUBMITTED'] = data['CASE_SUBMITTED'].dt.strftime('%Y-%m-%d')
    data['DECISION_DATE'] = data['DECISION_DATE'].dt.strftime('%Y-%m-%d')

    #Assume 1 or 2 entries without proper visa class are H1Bs
    data['VISA_CLASS'] = data['VISA_CLASS'].replace('Select Visa Classification', 'H-1B')
    data['FULL_TIME_POSITION'] = np.where(data['FULL_TIME_POSITION'].isin(['Y']), 1, 0)

    #Apply util functions
    data['EMPLOYER_POSTAL_CODE'] = data['EMPLOYER_POSTAL_CODE'].apply(utils.fix_zip)
    data['SOC_CODE'] = data['SOC_CODE'].apply(utils.fix_socCode)
    data['WAGE_UNIT_OF_PAY'] = data['WAGE_UNIT_OF_PAY'].apply(utils.fix_unitOfPay)
    data['PW_UNIT_OF_PAY'] = data['PW_UNIT_OF_PAY'].apply(utils.fix_unitOfPay)

    #Average Wage Rate of Pay range for valid values
    data['WAGE_RATE_OF_PAY_FROM'] = data['WAGE_RATE_OF_PAY_FROM'].replace(0, np.nan)
    data['WAGE_RATE_OF_PAY_TO'] = data['WAGE_RATE_OF_PAY_TO'].replace(0, np.nan)
    data['WAGE_RATE_OF_PAY'] = data[['WAGE_RATE_OF_PAY_FROM', 'WAGE_RATE_OF_PAY_TO']].mean(axis=1)

    data['WITHDRAWN'] = 'N'
    data.loc[data['CASE_STATUS'].str.match('WITHDRAWN'), 'WITHDRAWN'] = 'Y'

    data['EMPLOYER_AREA_CODE'] = None
    data['WORKSITE_POSTAL_CODE'] = None
    data['DOT_CODE'] = None

    data = data[['CASE_SUBMITTED', 'CASE_NUMBER', 'CASE_STATUS', 'DECISION_DATE', 'VISA_CLASS',
    'JOB_TITLE', 'DOT_CODE', 'SOC_CODE', 'SOC_NAME', 'FULL_TIME_POSITION',
    'EMPLOYER_NAME', 'EMPLOYER_ADDRESS', 'EMPLOYER_CITY', 'EMPLOYER_STATE', 'EMPLOYER_POSTAL_CODE',
    'EMPLOYER_AREA_CODE', 'NAIC_CODE', 'TOTAL_WORKERS', 
    'WORKSITE_CITY', 'WORKSITE_STATE', 'WORKSITE_POSTAL_CODE', 
    'WAGE_RATE_OF_PAY', 'WAGE_UNIT_OF_PAY', 'PREVAILING_WAGE', 'PW_UNIT_OF_PAY',
    'WITHDRAWN']]

    print("Creating clean CSV: ",clean)
    data.to_csv('clean/' + clean + '.csv', encoding='utf-8', index = False)


#-------------------
#Clean 2009 old file
print('Loading yr2009_old')
data = pd.read_excel('data/H-1B_Case_Data_FY2009.xlsx')
data.columns = ['CASE_SUBMITTED','CASE_NUMBER','VISA_CLASS','EMPLOYER_NAME','EMPLOYER_ADDRESS1','EMPLOYER_ADDRESS2',
'EMPLOYER_CITY','EMPLOYER_STATE','EMPLOYER_COUNTY','EMPLOYER_POSTAL_CODE','TOTAL_WORKERS','EMPLOYMENT_START_DATE','EMPLOYMENT_END_DATE',
'JOB_TITLE','DECISION_DATE','DOT_CODE','DOT_NAME','CASE_STATUS','WAGE_RATE_OF_PAY_FROM','WAGE_UNIT_OF_PAY','WAGE_RATE_OF_PAY_TO','PART_TIME',
'WORKSITE_CITY','WORKSITE_STATE','PREVAILING_WAGE','PW_WAGE_SOURCE','PW_SOURCE_YEAR','PW_SOURCE_OTHER','WAGE_RATE_OF_PAY_2','WAGE_UNIT_OF_PAY_2',
'MAX_RATE_2','PART_TIME_2','WORKSITE_CITY_2','WORKSITE_STATE_2','PREVAILING_WAGE_2','PW_WAGE_SOURCE_2','PW_SOURCE_YEAR_2','PW_WAGE_SOURCE_OTHER_2',
'WITHDRAWN']

data['CASE_SUBMITTED'] = data['CASE_SUBMITTED'].dt.strftime('%Y-%m-%d')
data['DECISION_DATE'] = data['DECISION_DATE'].dt.strftime('%Y-%m-%d')
data['WAGE_RATE_OF_PAY_FROM'] = data['WAGE_RATE_OF_PAY_FROM'].apply(float)

data['EMPLOYER_ADDRESS'] = (data['EMPLOYER_ADDRESS1'].map(str) + ' ' + data['EMPLOYER_ADDRESS2'].map(str)).str.replace('nan', '').str.upper().str.strip()

data['VISA_CLASS'] = data['VISA_CLASS'].replace('R', 'H-1B')
data['VISA_CLASS'] = data['VISA_CLASS'].replace('A', 'E-3 Australian')
data['VISA_CLASS'] = data['VISA_CLASS'].replace('C', 'H-1B1 Chile')
data['VISA_CLASS'] = data['VISA_CLASS'].replace('S', 'H-1B1 Singapore')

data['FULL_TIME_POSITION'] = 1
data.loc[data.PART_TIME == 'Y', 'FULL_TIME_POSITION'] = 0

data['SOC_CODE'] = None
data['SOC_NAME'] = None
data['EMPLOYER_AREA_CODE'] = None
data['NAIC_CODE'] = None
data['WORKSITE_POSTAL_CODE'] = None

data['WAGE_UNIT_OF_PAY'] = data['WAGE_UNIT_OF_PAY'].apply(utils.fix_unitOfPay)
data['PW_UNIT_OF_PAY'] = data['WAGE_UNIT_OF_PAY']

data['WAGE_RATE_OF_PAY_FROM'] = data['WAGE_RATE_OF_PAY_FROM'].replace(0, np.nan)
data['WAGE_RATE_OF_PAY_TO'] = data['WAGE_RATE_OF_PAY_TO'].replace(0, np.nan)
data['WAGE_RATE_OF_PAY'] = data[['WAGE_RATE_OF_PAY_FROM', 'WAGE_RATE_OF_PAY_TO']].mean(axis=1)

data = data[['CASE_SUBMITTED', 'CASE_NUMBER', 'CASE_STATUS', 'DECISION_DATE', 'VISA_CLASS',
'JOB_TITLE', 'DOT_CODE', 'SOC_CODE', 'SOC_NAME', 'FULL_TIME_POSITION',
'EMPLOYER_NAME', 'EMPLOYER_ADDRESS', 'EMPLOYER_CITY', 'EMPLOYER_STATE', 'EMPLOYER_POSTAL_CODE',
'EMPLOYER_AREA_CODE', 'NAIC_CODE', 'TOTAL_WORKERS', 
'WORKSITE_CITY', 'WORKSITE_STATE', 'WORKSITE_POSTAL_CODE', 
'WAGE_RATE_OF_PAY', 'WAGE_UNIT_OF_PAY', 'PREVAILING_WAGE', 'PW_UNIT_OF_PAY', 'WITHDRAWN']]

print("Creating clean CSV: yr2009_old")
data.to_csv('clean/yr2009_old.csv', encoding='utf-8', index= False)


#-------------------
#Clean 2010 file
print('Loading yr2010')
data = pd.read_excel('data/H-1B_FY2010.xlsx')
data.columns = ['CASE_NUMBER', 'CASE_STATUS','CASE_SUBMITTED','DECISION_DATE', 'EMPLOYMENT_START_DATE', 'EMPLOYMENT_END_DATE', 
'EMPLOYER_NAME','EMPLOYER_ADDRESS1','EMPLOYER_ADDRESS2', 'EMPLOYER_CITY','EMPLOYER_STATE','EMPLOYER_POSTAL_CODE',
'SOC_CODE','SOC_NAME','JOB_TITLE','WAGE_RATE_OF_PAY_FROM','WAGE_RATE_OF_PAY_TO','TOTAL_WORKERS','WORKSITE_CITY','WORKSITE_STATE',
'PREVAILING_WAGE','PW_UNIT_OF_PAY','PW_WAGE_SOURCE','PW_SOURCE_OTHER','PW_SOURCE_YEAR','WORKSITE_CITY_2','WORKSITE_STATE_2',
'PREVAILING_WAGE_2','PW_UNIT_OF_PAY_2','PW_WAGE_SOURCE_2','PW_WAGE_SOURCE_OTHER_2','PW_SOURCE_YEAR_2','NAIC_CODE']

data['CASE_SUBMITTED'] = data['CASE_SUBMITTED'].dt.strftime('%Y-%m-%d')
data['DECISION_DATE'] = data['DECISION_DATE'].dt.strftime('%Y-%m-%d')

data['VISA_CLASS'] = data['CASE_NUMBER'].str.slice(2,5)
data['VISA_CLASS'] = data['VISA_CLASS'].replace('200', 'H-1B')
data['VISA_CLASS'] = data['VISA_CLASS'].replace('201', 'H-1B1 Chile')
data['VISA_CLASS'] = data['VISA_CLASS'].replace('202', 'H-1B1 Singapore')
data['VISA_CLASS'] = data['VISA_CLASS'].replace('203', 'E-3 Australian')

data['EMPLOYER_ADDRESS'] = (data['EMPLOYER_ADDRESS1'].map(str) + ' ' + data['EMPLOYER_ADDRESS2'].map(str)).str.replace('nan', '').str.upper().str.strip()

 #Apply util functions
data['EMPLOYER_POSTAL_CODE'] = data['EMPLOYER_POSTAL_CODE'].apply(str).apply(utils.fix_zip)
data['SOC_CODE'] = data['SOC_CODE'].apply(utils.fix_socCode)
data['PW_UNIT_OF_PAY'] = data['PW_UNIT_OF_PAY'].apply(utils.fix_unitOfPay)

#Average Wage Rate of Pay range for valid values
data['WAGE_RATE_OF_PAY_FROM'] = data['WAGE_RATE_OF_PAY_FROM'].replace(0, np.nan)
data['WAGE_RATE_OF_PAY_TO'] = data['WAGE_RATE_OF_PAY_TO'].replace(0, np.nan)
data['WAGE_RATE_OF_PAY'] = data[['WAGE_RATE_OF_PAY_FROM', 'WAGE_RATE_OF_PAY_TO']].mean(axis=1)

data['WITHDRAWN'] = 'N'
data.loc[data['CASE_STATUS'].str.match('WITHDRAWN'), 'WITHDRAWN'] = 'Y'

data['FULL_TIME_POSITION'] = None
data['WAGE_UNIT_OF_PAY'] = None
data['EMPLOYER_AREA_CODE'] = None
data['WORKSITE_POSTAL_CODE'] = None
data['DOT_CODE'] = None

data = data[['CASE_SUBMITTED', 'CASE_NUMBER', 'CASE_STATUS', 'DECISION_DATE', 'VISA_CLASS',
'JOB_TITLE', 'DOT_CODE', 'SOC_CODE', 'SOC_NAME', 'FULL_TIME_POSITION',
'EMPLOYER_NAME', 'EMPLOYER_ADDRESS', 'EMPLOYER_CITY', 'EMPLOYER_STATE', 'EMPLOYER_POSTAL_CODE',
'EMPLOYER_AREA_CODE', 'NAIC_CODE', 'TOTAL_WORKERS', 
'WORKSITE_CITY', 'WORKSITE_STATE', 'WORKSITE_POSTAL_CODE', 
'WAGE_RATE_OF_PAY', 'WAGE_UNIT_OF_PAY', 'PREVAILING_WAGE', 'PW_UNIT_OF_PAY','WITHDRAWN']]

print("Creating clean CSV: yr2010")
data.to_csv('clean/yr2010.csv', encoding='utf-8', index = False)


