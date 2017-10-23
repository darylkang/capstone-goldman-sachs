import pandas as pd
import numpy as np
import re
import os
import utils 
import string

data_dir = "data/"

files = ["H-1B_Disclosure_Data_FY16.xlsx", 
            "H-1B_Disclosure_Data_FY15_Q4.xlsx", 
            "H-1B_FY14_Q4.xlsx", 
            "LCA_FY2013.xlsx"]

data = pd.read_excel(data_dir + files[1])

data.columns = ['CASE_NUMBER', 'CASE_STATUS', 'CASE_SUBMITTED', 'DECISION_DATE', 'VISA_CLASS', 'EMPLOYMENT_START_DATE', 'EMPLOYMENT_END_DATE', 'EMPLOYER_NAME', 'EMPLOYER_ADDRESS1', 'EMPLOYER_ADDRESS2', 'EMPLOYER_CITY', 'EMPLOYER_STATE', 'EMPLOYER_POSTAL_CODE', 'EMPLOYER_COUNTRY', 'EMPLOYER_PROVINCE', 'EMPLOYER_PHONE', 'EMPLOYER_PHONE_EXT', 'AGENT_ATTORNEY_NAME', 'AGENT_ATTORNEY_CITY', 'AGENT_ATTORNEY_STATE', 'JOB_TITLE', 'SOC_CODE', 'SOC_NAME', 'NAIC_CODE', 'TOTAL_WORKERS', 'FULL_TIME_POSITION', 'PREVAILING_WAGE', 'PW_UNIT_OF_PAY', 'PW_WAGE_LEVEL', 'PW_WAGE_SOURCE', 'PW_WAGE_SOURCE_YEAR', 'PW_WAGE_SOURCE_OTHER', 'WAGE_RATE_OF_PAY', 'WAGE_UNIT_OF_PAY', 'H-1B_DEPENDENT', 'WILLFUL VIOLATOR', 'WORKSITE_CITY', 'WORKSITE_COUNTY', 'WORKSITE_STATE', 'WORKSITE_POSTAL_CODE']

data['CASE_SUBMITTED'] = data['CASE_SUBMITTED'].dt.strftime('%Y-%m-%d')
data['DECISION_DATE'] = data['DECISION_DATE'].dt.strftime('%Y-%m-%d')

data.EMPLOYER_ADDRESS = (data.EMPLOYER_ADDRESS1.map(str) +" "+ data.EMPLOYER_ADDRESS2.map(str)).str.replace('nan','').str.upper().str.strip()

data['EMPLOYER_ADDRESS'] = data.EMPLOYER_ADDRESS.apply(utils.emp_address)

data.EMPLOYER_POSTAL_CODE = data['EMPLOYER_POSTAL_CODE'].apply(utils.update_fix_zip)

data.WORKSITE_POSTAL_CODE = data['WORKSITE_POSTAL_CODE'].apply(utils.update_fix_zip)


data["EMPLOYER_CITY"]=data.EMPLOYER_CITY.apply(utils.uppercase_nopunct)
data["EMPLOYER_STATE"]=data.EMPLOYER_STATE.apply(utils.uppercase_nopunct)
data["WORKSITE_CITY"]=data.WORKSITE_CITY.apply(utils.uppercase_nopunct)
data["WORKSITE_STATE"]=data.WORKSITE_STATE.apply(utils.uppercase_nopunct)

data.WORKSITE_STATE = data.WORKSITE_STATE.replace('PW', 'PA')

data.EMPLOYER_STATE = data[['CASE_NUMBER','EMPLOYER_STATE', 'EMPLOYER_POSTAL_CODE']].apply(utils.update_fix_states, axis =1)
data.WORKSITE_STATE = data[['CASE_NUMBER','WORKSITE_STATE', 'WORKSITE_POSTAL_CODE']].apply(utils.worksite_fix_states, axis =1)

data.JOB_TITLE = data['JOB_TITLE'].apply(utils.emp_address)
data['DOT_CODE'] = None

data.SOC_CODE = data.SOC_CODE.apply(utils.update_fix_socCode)


data.EMPLOYER_PHONE = data.EMPLOYER_PHONE.apply(utils.emp_address)
data['EMPLOYER_AREA_CODE'] = [
    str(i)[:3] if len(str(i)) == 11 else np.nan for i in data['EMPLOYER_PHONE']
]

data['WAGE_UNIT_OF_PAY'] = data['WAGE_UNIT_OF_PAY'].apply(utils.fix_unitOfPay)
data['PW_UNIT_OF_PAY'] = data['PW_UNIT_OF_PAY'].apply(utils.fix_unitOfPay)

a = data['WAGE_RATE_OF_PAY'].map(str).str.split("-").tolist()
a.remove(['22.69', '61.55 ', ' 46.02'])
b = pd.DataFrame(a,columns="WAGE_RATE_OF_PAY_FROM WAGE_RATE_OF_PAY_TO".split())
b['WAGE_RATE_OF_PAY_FROM'] = b['WAGE_RATE_OF_PAY_FROM'].str.strip()
b['WAGE_RATE_OF_PAY_TO'] = b['WAGE_RATE_OF_PAY_TO'].str.strip()
b = b.apply(pd.to_numeric,errors='coerce')

b['WAGE_RATE_OF_PAY_FROM'] = b['WAGE_RATE_OF_PAY_FROM'].replace(0, np.nan)
b['WAGE_RATE_OF_PAY_TO'] = b['WAGE_RATE_OF_PAY_TO'].replace(0, np.nan)
data['WAGE_RATE_OF_PAY'] = b[['WAGE_RATE_OF_PAY_FROM', 'WAGE_RATE_OF_PAY_TO']].apply(utils.calc_wage, axis =1)



cdata = data[['CASE_SUBMITTED', 'CASE_NUMBER', 'CASE_STATUS', 'DECISION_DATE', 'VISA_CLASS',
    'JOB_TITLE', 'DOT_CODE', 'SOC_CODE', 'SOC_NAME', 'FULL_TIME_POSITION',
    'EMPLOYER_NAME', 'EMPLOYER_ADDRESS', 'EMPLOYER_CITY', 'EMPLOYER_STATE', 'EMPLOYER_POSTAL_CODE',
    'EMPLOYER_AREA_CODE', 'NAIC_CODE', 'TOTAL_WORKERS', 
    'WORKSITE_CITY', 'WORKSITE_STATE', 'WORKSITE_POSTAL_CODE', 
    'WAGE_RATE_OF_PAY', 'WAGE_UNIT_OF_PAY', 'PREVAILING_WAGE', 'PW_UNIT_OF_PAY']]


if not os.path.exists('clean'):
        os.mkdir('clean')
        
new_filename="2015.csv"
cdata.to_csv("clean/{}".format(new_filename), encoding='utf-8', index = False)
