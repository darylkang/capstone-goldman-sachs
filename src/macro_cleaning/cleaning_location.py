import pandas as pd
import re
import numpy as np
import zipcode
import os

loc = ['CASE_STATUS', 'CASE_SUBMITTED', 'EMPLOYER_CITY', 'EMPLOYER_STATE', 'EMPLOYER_POSTAL_CODE', 'WORKSITE_CITY', 'WORKSITE_STATE', 'WORKSITE_POSTAL_CODE']

data = pd.read_csv('../clean/all_clean_data.csv',usecols = loc, dtype = str)

if not os.path.exists('../clean/macro'):
    os.mkdir('../clean/macro')

state_values = ['TX', 'MA', 'MI', 'CA', 'VA', 'NJ', 'NY', 'PA', 'FL', 'MN', 'IL',
       'MD', 'CT', 'WA', 'IA', 'CO', 'AZ', 'GA', 'OK', 'LA', 'WI', 'ND',
       'UT', 'IN', 'OH', 'KY', 'NC', 'NH', 'MO', 'TN', 'ID', 'VT', 'DC',
       'SD', 'AL', 'OR', 'AR', 'NM', 'SC', 'NE', 'DE', 'WY', 'HI', 'KS',
       'WV', 'ME', 'RI', 'NV', 'MS', 'AK','MT','PR','GU','VI']

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

data.WORKSITE_POSTAL_CODE = data.WORKSITE_POSTAL_CODE.fillna('')
data.WORKSITE_STATE = data.apply(worksite_fix_states,axis =1)


data['WORKSITE_STATE'] = [
    i if i in val else np.nan for i in data['WORKSITE_STATE']
]

data['CASE_SUBMITTED'] = [
    str(i)[:4] if i else np.nan for i in data['CASE_SUBMITTED']
]

ndata = data[['CASE_STATUS', 'CASE_SUBMITTED', 'EMPLOYER_STATE', 'WORKSITE_STATE']]

ndata.to_csv('../clean/macro/location_state.csv', index = False, encoding ='utf-8')
