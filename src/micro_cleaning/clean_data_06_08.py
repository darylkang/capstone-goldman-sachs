import pandas as pd
import numpy as np
from zipfile import ZipFile
from cleanco import cleanco
import re
import os
import zipcode
from pyzipcode import ZipCodeDatabase

#State formats
state_format_extrastuff = '^[a-zA-Z]{2} '
state_format_right = '^[A-Z]{2}$'

#Set up standardization for date fields
date_right_pattern = '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
date_long_format = '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$'
date_long_format_no_space = '^[0-9]{4}-[0-9]{2}-[0-9]{2}[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$'
date_slash_format = '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
date_slash_format_5='^[0-9]{1,2}/[0-9]{1,2}/[0-9]{5}$'
date_slash_format_2 = '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}$'
date_slash_format_long = '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}$'

#Set up standardization for zipcodes
right_zip = '^[0-9]{5}$'
long_zip = '^[0-9]{5}-[0-9]{4}$'

zip_dict = {'77098':'TX','49855':'MI', '48075':'MI','48334':'MI', '48034':'MI', '48335':'MI','95014':'CA','92833':'CA','92834':'CA','10962':'NY','98117':'WA','98765':'WA','20008':'DC','20002':'DC','21704':'MD','20814':'MD','21208':'MD','22222':'VA'}

state_values = ['TX', 'MA', 'MI', 'CA', 'VA', 'NJ', 'NY', 'PA', 'FL', 'MN', 'IL',
       'MD', 'CT', 'WA', 'IA', 'CO', 'AZ', 'GA', 'OK', 'LA', 'WI', 'ND',
       'UT', 'IN', 'OH', 'KY', 'NC', 'NH', 'MO', 'TN', 'ID', 'VT', 'DC',
       'SD', 'AL', 'OR', 'AR', 'NM', 'SC', 'NE', 'DE', 'WY', 'HI', 'KS',
       'WV', 'ME', 'RI', 'NV', 'MS', 'AK','MT','PR','GU','VI']

def get_file_from_zip(zipname, filename):
    with ZipFile('/'.join(['raw_data', zipname])) as zip_file:
        with zip_file.open(filename) as file:
            return pd.read_csv(file, dtype=str, encoding='Latin-1')

def count_wrong_formats(df):
	has_submitted = "CASE_SUBMITTED" in df.columns
	has_decision = "DECISION_DATE" in df.columns
	wrong_submitted_date_format = 0
	wrong_decision_date_format=0
	wrong_employer_state_format = 0
	wrong_worksite_state_format = 0
	wrong_zip_format = 0
	for index, row in df.iterrows():
		if has_submitted and (pd.isnull(row['CASE_SUBMITTED']) or not re.match(date_right_pattern,row['CASE_SUBMITTED'])):
			wrong_submitted_date_format+=1
		if has_decision and (pd.isnull(row['DECISION_DATE']) or not re.match(date_right_pattern,row['DECISION_DATE'])):
			wrong_decision_date_format+=1
		if row['EMPLOYER_STATE'] not in state_values:
			wrong_employer_state_format+=1
		if row['WORKSITE_STATE'] not in state_values:
			wrong_worksite_state_format+=1
		if pd.isnull(row['EMPLOYER_POSTAL_CODE']) or not re.match(right_zip, row['EMPLOYER_POSTAL_CODE']):
			wrong_zip_format+=1
	if has_submitted:
		print(wrong_submitted_date_format,"bad CASE_SUBMITTED fields")
	if has_decision:
		print(wrong_decision_date_format,"bad DECISION_DATE fields")
	print(wrong_employer_state_format, "bad EMPLOYER_STATE fields")
	print(wrong_worksite_state_format, "bad WORKSITE_STATE fields")
	print(wrong_zip_format, "bad EMPLOYER_POSTAL_CODE fields")


def address_concatenate(data):
	data["EMPLOYER_ADDRESS"] = (data["EMPLOYER_ADDRESS1"].map(str) +" "+ data["EMPLOYER_ADDRESS2"].map(str)).str.replace('nan','').str.upper().str.strip()

	#Set up cleaning functions
def employer_name_uppercase_cleanco(x):
    if pd.isnull(x):
        return x
    else:
        return cleanco(str(x).upper()).clean_name()

def uppercase_nopunct(x): 
    if pd.isnull(x):
        return x
    else:
        return str(x).upper().replace('[^\w\s]','').replace('/',' ').replace('"','').replace('  ',' ').strip()

def clean_states(x):
    if pd.isnull(x):
        return x
    if str(x).strip()=='':
        return np.nan
    elif re.match(state_format_right, x):
        return x
    elif re.match(state_format_extrastuff, x) and x[:2] in state_values:
        return x[:2]
    elif x=="MARYLAND":
        return "MD"
    elif x=="NEW YORK":
        return "NY"
    else:
        print("\t\tState Error, ",x)
        return x

def case_status_withdrawn(x):
    if x.WITHDRAWN=="Y" or x.WITHDRAWN=="y":
        return x.CASE_STATUS+"-WITHDRAWN"
    else:
        return x.CASE_STATUS

def dot_code_format(x):
	if pd.isnull(x):
		return x
	else:
		return str(x).zfill(3)

def check_apply_date_pattern(x):
    if pd.isnull(x):
        return x
    else:
        x=str(x).replace(" ",'')
    if re.match(date_right_pattern,x):
        return x
    elif re.match(date_long_format, x):
        return x[:10].strip()
    elif re.match(date_long_format_no_space,x):
    	return x[:10].strip()
    elif re.match(date_slash_format, x):
        month = x[:x.index('/')]
        day = x[x.index('/')+1:x.index('/',x.index('/')+1)]
        year = x[x.index('/',x.index('/')+1)+1:]
        return "{}-{}-{}".format(year.strip(), month.zfill(2).strip(), day.zfill(2).strip())
    elif re.match(date_slash_format_5, x):
        month = x[:x.index('/')]
        day = x[x.index('/')+1:x.index('/',x.index('/')+1)]
        year = x[x.index('/',x.index('/')+1)+1:x.index('/',x.index('/')+1)+5]
        return "{}-{}-{}".format(year.strip(), month.zfill(2).strip(), day.zfill(2).strip())
    elif re.match(date_slash_format_2, x):
        month = x[:x.index('/')]
        day = x[x.index('/')+1:x.index('/',x.index('/')+1)]
        year = x[x.index('/',x.index('/')+1)+1:]
        return "20{}-{}-{}".format(year.strip(), month.zfill(2).strip(), day.zfill(2).strip())
    elif re.match(date_slash_format_long, x):
        month = x[:x.index('/')]
        day = x[x.index('/')+1:x.index('/',x.index('/')+1)]
        year = x[x.index('/',x.index('/')+1)+1:x.index('/',x.index('/')+1)+5]
        return "{}-{}-{}".format(year.strip(), month.zfill(2).strip(), day.zfill(2).strip())
    else:
        print("\t\tDATE ERROR: x is",x,"returning None")
        return None

def fix_zip(x):
    if pd.isnull(x):
        return x
    x=str(x).strip()
    if x.isnumeric():
    	x=x.zfill(5)
    if re.match(right_zip,x):
        return x
    elif re.match(long_zip,x):
        return x[:5]
    else:
        print("\t\tError in zip,",x)
        return x

def fix_visa_class(x):
    if pd.isnull(x):
        return x
    x=str(x).strip()
    valid = ["H-1B","E-3","H-1B1 CHILE","H-1B1 SINGAPORE"]
    if x in valid:
        return x
    elif x=="R":
        return "H-1B"
    elif x=="A":
        return "E-3"
    elif x=="C":
        return "H-1B1 CHILE"
    elif x=="S":
        return "H-1B1 SINGAPORE"
    else:
        print("\t\tError in visa class, ",x)
        return x

def fix_employer_states(x):
    if pd.isnull(x['EMPLOYER_STATE']):
        return x.EMPLOYER_STATE
    elif x['EMPLOYER_STATE'] not in state_values and x.EMPLOYER_POSTAL_CODE.isdigit():
        if x.EMPLOYER_POSTAL_CODE in zip_dict.keys():
            return zip_dict[x.EMPLOYER_POSTAL_CODE]
        else:
            newzip = zipcode.isequal(x.EMPLOYER_POSTAL_CODE)
            if newzip is not None:
                return newzip.state
            else:
                pyzip = findpyzipcode(x)
                if pyzip:
                    return pyzip
                print("\t\tCouldnt find",x.EMPLOYER_POSTAL_CODE,"in either zip package")
                return x.EMPLOYER_STATE
    elif x['EMPLOYER_STATE'] not in state_values and re.match(state_format_right, x.EMPLOYER_POSTAL_CODE.upper()):
        #print("Employer state found in postal code, shifting",x.EMPLOYER_POSTAL_CODE.upper())
        x.EMPLOYER_CITY = x.EMPLOYER_STATE
        return x.EMPLOYER_POSTAL_CODE.upper()
    else:
        return x.EMPLOYER_STATE

def findpyzipcode(x):
    zcdb = ZipCodeDatabase()
    try: 
        value = zcdb[x]
        return value.state
    except IndexError:
        return None

def fix_worksite_states(x):
    if pd.isnull(x['WORKSITE_STATE']):
        return x.WORKSITE_STATE
    elif x['WORKSITE_STATE'] not in state_values and x['EMPLOYER_STATE'] in state_values and x.WORKSITE_CITY == x.EMPLOYER_CITY:
        #print("Cities match, returning",x.EMPLOYER_STATE,"to fix",x.WORKSITE_STATE)
        return x.EMPLOYER_STATE
    else:
        return x.WORKSITE_STATE

def check_states(x):
    if x.CASE_STATUS=="CERTIFIED":
        if pd.isnull(x.EMPLOYER_STATE):
            print("Null Employer State: {}, Status - {}".format(x.EMPLOYER_NAME, x.CASE_STATUS))
        elif x['EMPLOYER_STATE'] not in state_values:
            print("Wrong Employer State: Name - {}, City - {}, State - {}, Zip - {}, Status - {}".format(x.EMPLOYER_NAME, x.EMPLOYER_CITY, x.EMPLOYER_STATE, x.EMPLOYER_POSTAL_CODE, x.CASE_STATUS))
        if pd.isnull(x['WORKSITE_STATE']):
            print("Null Worksite State: {}, Status - {}".format(x.EMPLOYER_NAME, x.CASE_STATUS))
        elif x['WORKSITE_STATE'] not in state_values:
            print("Wrong Worksite State: Name - {}, City - {}, State - {}, Zip - {}, Status - {}".format(x.EMPLOYER_NAME, x.WORKSITE_CITY, x.WORKSITE_STATE, x.EMPLOYER_POSTAL_CODE, x.CASE_STATUS))

# Set up function to standardize unit of pay
def standard_unit(x):
    if(pd.isnull(x)):
        return x
    elif x in ['Y','H','M','W','B']:
        return x
    elif x=='Year' or x=='yr':
        return 'Y'
    elif x=='Hour' or x=='hr':
        return 'H'
    elif x=='Month' or x=='mth':
        return 'M'
    elif x=='Week' or x=='wk':
        return 'W'
    elif x=='2 weeks' or x=='bi':
        return 'B'
    else:
        print("\t\tError in unit of pay,",x)
        return x

#Set up function to go from part time to full time
def part_to_full(x):
    if pd.isnull(x):
        return x
    elif x=="N":
        return int(1)
    elif x=="Y":
        return int(0)
    else:
        print("\t\tError in part time,",x)
        return np.nan

def format_full_time(x):
	if x=="N" or x=="n":
		return int(0)
	if x=="Y" or x=="y":
		return int(1)

#Create function to clean up WAGE_RATE_OF_PAY
def calc_wage_rate_of_pay(x):
	if x.WAGE_RATE_OF_PAY_FROM=='.':
		x.WAGE_RATE_OF_PAY_FROM = np.nan
	if x.WAGE_RATE_OF_PAY_TO=='.':
		x.WAGE_RATE_OF_PAY_TO = np.nan
	if pd.isnull(x.WAGE_RATE_OF_PAY_FROM) and pd.isnull(x.WAGE_RATE_OF_PAY_TO):
		return np.nan
	wage_from = float(str(x.WAGE_RATE_OF_PAY_FROM).replace('$','').replace(',','').strip())
	if pd.isnull(x.WAGE_RATE_OF_PAY_TO):
		return wage_from
	wage_to = float(str(x.WAGE_RATE_OF_PAY_TO).replace('$','').replace(',','').strip())
	if wage_from <=wage_to:
		return (wage_from + wage_to)/2
	else:
		return wage_from

def write_to_csv(data, filename):
	print("Writing data to file",filename)
	if not os.path.exists('../clean'):
		os.mkdir('../clean')
	data.to_csv("../clean/{}".format(filename), encoding='utf-8', index=False)

def clean_all_data(data):
	cols = data.columns

	print("Cleaning addresses...")
	if 'EMPLOYER_ADDRESS1' in cols:
		print("\t Concatenating ADDRESS1 and ADDRESS2")
		address_concatenate(data)
	print("\t Uppercasing and removing punctuation")
	data["EMPLOYER_ADDRESS"] = data.EMPLOYER_ADDRESS.apply(uppercase_nopunct)

	if 'CASE_SUBMITTED' not in cols and 'DATE_SIGNED' in cols:
		print("Creating CASE_SUBMITTED column and filling with DATE_SIGNED")
		data["CASE_SUBMITTED"]=data.DATE_SIGNED.apply(check_apply_date_pattern)
	elif 'CASE_SUBMITTED' in cols:
		print("Cleaning CASE_SUBMITTED")
		data["CASE_SUBMITTED"]=data.CASE_SUBMITTED.apply(check_apply_date_pattern)

	if 'VISA_CLASS' in cols:
		print("Cleaning and fixing VISA_CLASS")
		data["VISA_CLASS"]=data.VISA_CLASS.apply(fix_visa_class)

	print("Cleaning EMPLOYER_NAME")
	data["EMPLOYER_NAME"]=data.EMPLOYER_NAME.apply(employer_name_uppercase_cleanco)

	print("Cleaning EMPLOYER_CITY")
	data["EMPLOYER_CITY"]=data.EMPLOYER_CITY.apply(uppercase_nopunct)

	print("Cleaning EMPLOYER_POSTAL_CODE")
	data["EMPLOYER_POSTAL_CODE"]=data.EMPLOYER_POSTAL_CODE.apply(fix_zip)
	
	print("Trying to fix EMPLOYER_STATE")
	print("\t Uppercasing and removing punctuation")
	data["EMPLOYER_STATE"]=data.EMPLOYER_STATE.apply(uppercase_nopunct)
	print("\t Manually trying to fix simple State mistakes")
	data["EMPLOYER_STATE"]=data.EMPLOYER_STATE.apply(clean_states)
	print("\t Trying to fix states by using available zip codes")
	data["EMPLOYER_STATE"] = data.apply(fix_employer_states, axis=1)

	print("Cleaning WORKSITE_CITY")
	data["WORKSITE_CITY"]=data.WORKSITE_CITY.apply(uppercase_nopunct)
	print("Trying to fix WORKSITE_STATE")
	print("\t Uppercasing and removing punctuation")
	data["WORKSITE_STATE"]=data.WORKSITE_STATE.apply(uppercase_nopunct)
	print("\t Manually trying to fix simple State mistakes")
	data["WORKSITE_STATE"]=data.WORKSITE_STATE.apply(clean_states)
	print("\t Trying to fix states by using EMPLOYER_CITY information")
	data["WORKSITE_STATE"] = data.apply(fix_worksite_states, axis=1)

	print("Fixing and cleaning CASE_STATUS")
	data["CASE_STATUS"]=data.CASE_STATUS.apply(uppercase_nopunct)

	if 'WITHDRAWN' in cols:
		print("Adding WITHDRAWN to CASE_STATUS")
		data["CASE_STATUS"]=data.apply(case_status_withdrawn, axis=1)

	print("Cleaning JOB_TITLE")
	data["JOB_TITLE"]=data.JOB_TITLE.apply(uppercase_nopunct)

	if 'DECISION_DATE' in cols:
		print("Formatting DECSION_DATE")
		data["DECISION_DATE"]=data.DECISION_DATE.apply(check_apply_date_pattern)
	
	if 'WAGE_RATE_OF_PAY_TO' in cols and 'WAGE_RATE_OF_PAY_FROM' in cols:
		print("Calculating WAGE_RATE_OF_PAY")
		data["WAGE_RATE_OF_PAY"]=data.apply(calc_wage_rate_of_pay, axis=1)

	if 'WAGE_UNIT_OF_PAY' in cols:
		print("Standardizing WAGE_UNIT_OF_PAY")
		data["WAGE_UNIT_OF_PAY"]=data.WAGE_UNIT_OF_PAY.apply(standard_unit)
	
	if 'PW_UNIT_OF_PAY' in cols:
		print("Standardizing PW_UNIT_OF_PAY")
		data["PW_UNIT_OF_PAY"]=data.PW_UNIT_OF_PAY.apply(standard_unit)

	if 'FULL_TIME_POSITION' in cols:
		print("Making FULL_TIME_POSITION binary")
		data["FULL_TIME_POSITION"]=data.FULL_TIME_POSITION.apply(format_full_time)
	
	if 'PART_TIME' in cols:
		print("Creatung FULL_TIME_POSITION based on PART_TIME")
		data["FULL_TIME_POSITION"]=data.PART_TIME.apply(part_to_full)

	if 'DOT_CODE' in cols:
		print("Formatting DOT_CODE")
		data["DOT_CODE"]=data.DOT_CODE.apply(dot_code_format)

	if 'DOT_NAME' in cols:
		print("Cleaning DOT_NAME")
		data["DOT_NAME"]=data.DOT_CODE.apply(uppercase_nopunct)

	return data

def main():
	#Get data for 2006 FAX
	print("Getting data from 2006 FAX")
	zipname = 'H1B_fax_FY2006_text.zip'
	filename = 'H1B_Fax_FY2006_External_Web.txt'
	data06fax = get_file_from_zip(zipname, filename)
	columns = ['CASE_NUMBER','CASE_STATUS','RETURN_FAX','EMPLOYER_NAME','EMPLOYER_CITY','EMPLOYER_ADDRESS1','EMPLOYER_ADDRESS2','EMPLOYER_STATE','EMPLOYER_POSTAL_CODE','WAGE_RATE_OF_PAY_FROM','WAGE_RATE_OF_PAY_TO','WAGE_UNIT_OF_PAY','PART_TIME','EMPLOYMENT_START_DATE','EMPLOYMENT_END_DATE','DOT_CODE','TOTAL_WORKERS','JOB_TITLE','WORKSITE_CITY','WORKSITE_STATE','PREVAILING_WAGE','PW_UNIT_OF_PAY','PW_WAGE_SOURCE','PW_SOURCE_YEAR','PW_SOURCE_OTHER','WORKSITE_CITY_2','WORKSITE_STATE_2','PREVAILING_WAGE_2','PW_UNIT_OF_PAY_2','PW_WAGE_SOURCE_2','PW_SOURCE_YEAR_2','PW_WAGE_SOURCE_OTHER_2','DATE_SIGNED','CERTIFICATION_START_DATE','CERTIFICATION_END_DATE','DECISION_DATE','PROCESS_DATE']
	data06fax.columns=columns

	data06fax = clean_all_data(data06fax)

	cleaned_data=data06fax[["CASE_SUBMITTED","CASE_NUMBER","CASE_STATUS","DECISION_DATE","JOB_TITLE","DOT_CODE","FULL_TIME_POSITION","EMPLOYER_NAME","EMPLOYER_ADDRESS","EMPLOYER_CITY","EMPLOYER_STATE","EMPLOYER_POSTAL_CODE","TOTAL_WORKERS","WORKSITE_CITY","WORKSITE_STATE","WAGE_RATE_OF_PAY","WAGE_UNIT_OF_PAY","PREVAILING_WAGE","PW_UNIT_OF_PAY","DATE_SIGNED"]]
	write_to_csv(cleaned_data,"fax_2006.csv")

	#Get data for 2006 E-File
	print("Getting data from 2006 E-File")
	zipname = 'H1B_efile_FY06_text.zip'
	filename = 'H1B_efile_FY06.txt'
	data06ef = get_file_from_zip(zipname, filename)
	columns = ['CASE_SUBMITTED','CASE_NUMBER','EMPLOYER_NAME','EMPLOYER_ADDRESS1','EMPLOYER_ADDRESS2','EMPLOYER_CITY','EMPLOYER_STATE','EMPLOYER_POSTAL_CODE','TOTAL_WORKERS','EMPLOYMENT_START_DATE','EMPLOYMENT_END_DATE','JOB_TITLE','DECISION_DATE','CERTIFICATION_START_DATE','CERTIFICATION_END_DATE','DOT_CODE','CASE_STATUS','WAGE_RATE_OF_PAY_FROM','WAGE_UNIT_OF_PAY','WAGE_RATE_OF_PAY_TO','PART_TIME','WORKSITE_CITY','WORKSITE_STATE','PREVAILING_WAGE','PW_WAGE_SOURCE','PW_SOURCE_YEAR','PW_SOURCE_OTHER','WAGE_RATE_OF_PAY_2','WAGE_UNIT_OF_PAY_2','MAX_RATE_2','PART_TIME_2','WORKSITE_CITY_2','WORKSITE_STATE_2','PREVAILING_WAGE_2','PW_WAGE_SOURCE_2','PW_SOURCE_YEAR_2','PW_WAGE_SOURCE_OTHER_2']
	data06ef.columns=columns

	data06ef = clean_all_data(data06ef)

	cleaned_data=data06ef[["CASE_SUBMITTED","CASE_NUMBER","CASE_STATUS","DECISION_DATE","JOB_TITLE","DOT_CODE","FULL_TIME_POSITION","EMPLOYER_NAME","EMPLOYER_ADDRESS","EMPLOYER_CITY","EMPLOYER_STATE","EMPLOYER_POSTAL_CODE","TOTAL_WORKERS","WORKSITE_CITY","WORKSITE_STATE","WAGE_RATE_OF_PAY","WAGE_UNIT_OF_PAY","PREVAILING_WAGE"]]
	write_to_csv(cleaned_data, "efile_2006.csv")

	#Get data for 2007 E-File
	print("Getting data from 2007 E-File")
	zipname = 'H1B_efile_FY07_text.zip'
	filename = 'EFILE_FY2007.txt'
	data07 = get_file_from_zip(zipname, filename)
	columns = ['CASE_SUBMITTED','CASE_NUMBER','VISA_CLASS','EMPLOYER_NAME','EMPLOYER_ADDRESS1','EMPLOYER_ADDRESS2','EMPLOYER_CITY','EMPLOYER_STATE','EMPLOYER_POSTAL_CODE','TOTAL_WORKERS','EMPLOYMENT_START_DATE','EMPLOYMENT_END_DATE','JOB_TITLE','DECISION_DATE','CERTIFICATION_START_DATE','CERTIFICATION_END_DATE','DOT_CODE','CASE_STATUS','WAGE_RATE_OF_PAY_FROM','WAGE_UNIT_OF_PAY','WAGE_RATE_OF_PAY_TO','PART_TIME','WORKSITE_CITY','WORKSITE_STATE','PREVAILING_WAGE','PW_WAGE_SOURCE','PW_SOURCE_YEAR','PW_SOURCE_OTHER','WAGE_RATE_OF_PAY_2','WAGE_UNIT_OF_PAY_2','MAX_RATE_2','PART_TIME_2','WORKSITE_CITY_2','WORKSITE_STATE_2','PREVAILING_WAGE_2','PW_WAGE_SOURCE_2','PW_SOURCE_YEAR_2','PW_WAGE_SOURCE_OTHER_2','WITHDRAWN']
	data07.columns=columns

	data07 = clean_all_data(data07)

	cleaned_data=data07[["CASE_SUBMITTED","CASE_NUMBER","VISA_CLASS","CASE_STATUS","DECISION_DATE","JOB_TITLE","DOT_CODE","FULL_TIME_POSITION","EMPLOYER_NAME","EMPLOYER_ADDRESS","EMPLOYER_CITY","EMPLOYER_STATE","EMPLOYER_POSTAL_CODE","TOTAL_WORKERS","WORKSITE_CITY","WORKSITE_STATE","WAGE_RATE_OF_PAY","WAGE_UNIT_OF_PAY","PREVAILING_WAGE"]]
	write_to_csv(cleaned_data, "efile_2007.csv")

	#Get data for 2008 E-File
	print("Getting data from 2008 E-File")
	H1B_2008_columns=['CASE_SUBMITTED','CASE_NUMBER','VISA_CLASS','EMPLOYER_NAME','EMPLOYER_ADDRESS1','EMPLOYER_ADDRESS2','EMPLOYER_CITY','EMPLOYER_STATE','EMPLOYER_POSTAL_CODE','TOTAL_WORKERS','EMPLOYMENT_START_DATE','EMPLOYMENT_END_DATE','JOB_TITLE','DECISION_DATE','CERTIFICATION_START_DATE','CERTIFICATION_END_DATE','DOT_CODE','CASE_STATUS','WAGE_RATE_OF_PAY_FROM','WAGE_UNIT_OF_PAY','WAGE_RATE_OF_PAY_TO','PART_TIME','WORKSITE_CITY','WORKSITE_STATE','PREVAILING_WAGE','PW_WAGE_SOURCE','PW_SOURCE_YEAR','PW_SOURCE_OTHER','WAGE_RATE_OF_PAY_2','WAGE_UNIT_OF_PAY_2','MAX_RATE_2','PART_TIME_2','WORKSITE_CITY_2','WORKSITE_STATE_2','PREVAILING_WAGE_2','PW_WAGE_SOURCE_2','PW_SOURCE_YEAR_2','PW_WAGE_SOURCE_OTHER_2','WITHDRAWN','DOT_NAME']
	H1B_2008 = pd.read_excel('data/H-1B_Case_Data_FY2008.xlsx')
	H1B_2008.columns=H1B_2008_columns

	data = clean_all_data(H1B_2008)

	cleaned_data=data[["CASE_SUBMITTED","CASE_NUMBER","CASE_STATUS","VISA_CLASS","DECISION_DATE","JOB_TITLE","DOT_CODE","DOT_NAME","FULL_TIME_POSITION","EMPLOYER_NAME","EMPLOYER_ADDRESS","EMPLOYER_CITY","EMPLOYER_STATE","EMPLOYER_POSTAL_CODE","TOTAL_WORKERS","WORKSITE_CITY","WORKSITE_STATE","WAGE_RATE_OF_PAY","WAGE_UNIT_OF_PAY","PREVAILING_WAGE"]]
	write_to_csv(cleaned_data, "efile_2008.csv")

if __name__ == '__main__':
    main()