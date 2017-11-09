import numpy as np
import pandas as pd
from datetime import date
import os
import zipcode
import re
import sys
import progressbar as pb


#Set up global variables
loc = ['CASE_STATUS', 'CASE_SUBMITTED', 'EMPLOYER_CITY', 'EMPLOYER_STATE', 'EMPLOYER_POSTAL_CODE', 'WORKSITE_CITY', 'WORKSITE_STATE', 'WORKSITE_POSTAL_CODE']
state_values = ['TX', 'MA', 'MI', 'CA', 'VA', 'NJ', 'NY', 'PA', 'FL', 'MN', 'IL',
       'MD', 'CT', 'WA', 'IA', 'CO', 'AZ', 'GA', 'OK', 'LA', 'WI', 'ND',
       'UT', 'IN', 'OH', 'KY', 'NC', 'NH', 'MO', 'TN', 'ID', 'VT', 'DC',
       'SD', 'AL', 'OR', 'AR', 'NM', 'SC', 'NE', 'DE', 'WY', 'HI', 'KS',
       'WV', 'ME', 'RI', 'NV', 'MS', 'AK','MT','PR','GU','VI']
columns = [
    'PREVAILING_WAGE',
    'PW_UNIT_OF_PAY',
    'PREVAILING_WAGE_CALCULATED',
    'WAGE_RATE_OF_PAY',
    'WAGE_UNIT_OF_PAY',
    'WAGE_RATE_OF_PAY_CALCULATED'
]

#Set up cleaning functions

def update_visa_class(x):
    if pd.to_datetime(x).year<2007:
        return 'H-1B'
    else:
        return np.nan

def update_fix_states(x):
    if pd.isnull(x['EMPLOYER_STATE']):
        if x.EMPLOYER_POSTAL_CODE:
            newzip = zipcode.isequal(x.EMPLOYER_POSTAL_CODE)
            if newzip is not None:
                print("Old state was",x.EMPLOYER_STATE,"and new state is",newzip.state)
                return newzip.state
            else:
                return x.EMPLOYER_STATE
    elif x['EMPLOYER_STATE'] not in state_values and x.EMPLOYER_POSTAL_CODE.isdigit():
        newzip = zipcode.isequal(x.EMPLOYER_POSTAL_CODE)
        if newzip is not None:
            print("Old state was",x.EMPLOYER_STATE,"and new state is",newzip.state)
            return newzip.state
        else:
            return x.EMPLOYER_STATE
    else:
        return x.EMPLOYER_STATE

def worksite_fix_states(x):
    if pd.isnull(x['WORKSITE_STATE']):
        if x.WORKSITE_POSTAL_CODE:
            newzip = zipcode.isequal(x.WORKSITE_POSTAL_CODE)
            if newzip is not None:
                print("Old state was",x.WORKSITE_STATE,"and new state is",newzip.state)
                return newzip.state
            else:
                return x.WORKSITE_STATE
    elif x['WORKSITE_STATE'] not in state_values and x.WORKSITE_POSTAL_CODE.isdigit():
        newzip = zipcode.isequal(x.WORKSITE_POSTAL_CODE)
        if newzip is not None:
            print("Old state was",x.WORKSITE_STATE,"and new state is",newzip.state)
            return newzip.state
        else:
            return x.WORKSITE_STATE
    else:
        return x.WORKSITE_STATE

def createIndustryMapping(x):
	if pd.isnull(x):
		return np.nan
	naic = str(x)
	if(naic[0:4] == '6113'):
	    industry = 'Colleges & Universities'
	elif(naic[0:2] in ['61', '92']):
	    industry = 'Other Educational, Public Affairs'
	elif(naic[0:2] in ['42', '44', '45', '48', '49']):
	    industry = 'Trade, Transportation, Warehousing'
	elif(naic[0:2] in ['62']):
	    industry = 'Healthcare'
	elif(naic[0:3] in ['334', '335']):
	    industry = 'Manufacturing - Other'
	elif(naic[0:2] in ['31', '32', '33']):
	    industry = 'Manufacturing - Computers & Electronics'
	elif(naic[0:4] in ['5112', '5415']):
	    industry = 'Software Publishers, Computer Services'
	elif(naic[0:4] in ['5413', '5417']):
	    industry = 'Engineering & Scientific R&D Services'
	elif(naic[0:4] == '5416' or naic[0:2] == '55'):
	    industry = 'Management, Consulting & Technical Services'
	elif(naic[0:4] in ['5411', '5412']):
	    industry = 'Legal & Accounting Services'
	elif(naic[0:4] in ['5414', '5418'] or naic[0:2] == '51'):
	    industry = 'Media, Advertising, Telecommunications'
	elif(naic[0:2] == '54' or naic[0:3] == '561'):
	    industry = 'Other Professional & Administrative Services'
	elif(naic[0:2] in ['52', '53']):
	    industry = 'Finance, Insurance, Real Estate'
	else:
	    #Commodities, Energy, Utilities, Construction, Arts & Entertainment, Accomodation, Other Services, & Unknown
	    industry = 'Other'
	return industry

# READ IN DATA

print("Reading micro-cleaned data")
data = pd.read_csv('../clean/all_clean_data.csv', encoding='utf-8', dtype=str)

if not os.path.exists('../clean/macro'):
    os.mkdir('../clean/macro')

# START CLEANING UPDATES
print("Starting macro cleaning. Get ready, this will take a while.")
print("Updating CASE_STATUS. Might as well grab a coffee.")
#Fix inconsistent CASE_STATUSes (someone forgot to add WITHDRAWN)
data["CASE_STATUS"]=data.apply(lambda x: x.CASE_STATUS+"-WITHDRAWN" if x.WITHDRAWN=="Y" else x.CASE_STATUS, axis=1)

print("Updating CASE_SUBMITTED. Hope you at least have some tea.")
#Update CASE_SUBMITTED with DECISION_DATE if empty
data["CASE_SUBMITTED"]=data.apply(lambda x: x.DECISION_DATE if pd.isnull(x.CASE_SUBMITTED) and not pd.isnull(x.DECISION_DATE) else x.CASE_SUBMITTED, axis=1)

# DATA DEDUPING AND FILTERING

print("Filtering and De-Duping Data! This should make stuff go faster.")
#Get only certified data
data.query('CASE_STATUS=="CERTIFIED"', inplace=True)

#Sort by dates
data.sort_values(["CASE_SUBMITTED","DECISION_DATE"],ascending=True, inplace=True)

#Dedupe, keeping only the latest date
data.drop_duplicates(["CASE_NUMBER","CASE_STATUS","EMPLOYER_NAME","TOTAL_WORKERS","WAGE_RATE_OF_PAY"], keep='last', inplace=True)

# CONTINUE CLEANING UPDATES

print("Applying basic fixes to VISA_CLASS")
#Fix remaining VISA_CLASS issues (remove Australian, make uppercase)
data['VISA_CLASS']=data.VISA_CLASS.str.upper().str.replace("AUSTRALIAN","").str.strip()

print("Updating VISA_CLASS for earlier years")
#All Pre-2007 VISA_CLASS is H-1B
data["VISA_CLASS"]=data.apply(lambda x: update_visa_class(x.DECISION_DATE) if pd.isnull(x.VISA_CLASS) else x.VISA_CLASS, axis=1)

# START LOCATION CLEANING - TEJAS

print("Writing temporary file")
# TEMPORARY FILE WRITING
data.to_csv("../clean/macro/master_macro_cleaned_1.csv",index=False, encoding='utf-8')

print("Creating industry classifications")
data['NAICS_CLASSIFICATION'] = data.NAIC_CODE.apply(createIndustryMapping)

print("Working on Location Cleaning")

print("Working on employer states")
data.EMPLOYER_POSTAL_CODE = data.EMPLOYER_POSTAL_CODE.fillna('')
data.EMPLOYER_STATE = data.apply(update_fix_states,axis =1)

val = ['TX', 'MA', 'MI', 'CA', 'VA', 'NJ', 'NY', 'PA', 'FL', 'MN', 'IL',
       'MD', 'CT', 'WA', 'IA', 'CO', 'AZ', 'GA', 'OK', 'LA', 'WI', 'ND',
       'UT', 'IN', 'OH', 'KY', 'NC', 'NH', 'MO', 'TN', 'ID', 'VT', 'DC',
       'SD', 'AL', 'OR', 'AR', 'NM', 'SC', 'NE', 'DE', 'WY', 'HI', 'KS',
       'WV', 'ME', 'RI', 'NV', 'MS', 'AK','MT', 'PR', 'GU', 'VI',  'MP', 'AS']

data['EMPLOYER_STATE'] = [
    i if i in val else np.nan for i in data['EMPLOYER_STATE']
]
print("Working on Worksite states")
data.WORKSITE_POSTAL_CODE = data.WORKSITE_POSTAL_CODE.fillna('')
data.WORKSITE_STATE = data.apply(worksite_fix_states,axis =1)


data['WORKSITE_STATE'] = [
    i if i in val else np.nan for i in data['WORKSITE_STATE']
]

print("Writing temporary file")
data.to_csv("../clean/macro/master_macro_cleaned_2.csv",index=False, encoding='utf-8')

print('Loading Data...')
data = pd.read_csv('../clean/macro/master_macro_cleaned_2.csv', usecols=columns, dtype=str)

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
original_data = pd.read_csv('../clean/macro/master_macro_cleaned_2.csv', dtype=str)
original_data.drop(columns, axis=1, inplace=True)
original_data['WAGE_RATE_OF_PAY_CALCULATED'] = data['WAGE_RATE_OF_PAY_CALCULATED']
original_data['PREVAILING_WAGE_CALCULATED'] = data['PREVAILING_WAGE_CALCULATED']
original_data = original_data.reindex_axis(sorted(original_data.columns), axis=1)

print("Writing deduped, filtered, clean data!")
#Write to file
original_data.to_csv("../clean/macro/master_macro_cleaned.csv",index=False, encoding='utf-8')

data2 = pd.read_csv("../clean/macro/master_macro_cleaned.csv", dtype=str, encoding='utf-8')


data2.to_csv("../clean/macro/master_macro_cleaned_naics.csv",index=False, encoding='utf-8')