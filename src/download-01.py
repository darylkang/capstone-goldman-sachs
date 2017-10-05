import numpy as np
import pandas as pd
import os
import requests
import shutil

from zipfile import ZipFile

def download(URL):
    file = URL.split('/')[-1]
    r = requests.get(URL, stream=True)
    r.raise_for_status()
    with open('/'.join(['temp', file]), 'w+b') as f:
        for chunk in r.iter_content(chunk_size=2048):
            if chunk:
                f.write(chunk)

def main():
    if not os.path.exists('temp'):
        os.mkdir('temp')
    print('Downloading Data...')
    for i in range(1, 8):
        try:
            download('http://www.flcdatacenter.com/download/H1B_efile_FY0{}_text.zip'.format(i))
        except:
            pass
        try:
            download('http://www.flcdatacenter.com/download/H1B_fax_FY200{}_text.zip'.format(i))
        except:
            pass
    print('Download Complete')
    print('Combining Datasets...')
    with open('data_01.csv', 'a') as f:
        HEADER = ','.join([
            'Submitted_Date',
            'Case_No',
            'Program',
            'Name',
            'Address',
            'Address2',
            'City',
            'State',
            'Postal_Code',
            'Nbr_Immigrants',
            'Begin_Date',
            'End_Date',
            'Job_Title',
            'Dol_Decision_Date',
            'Certified_Begin_Date',
            'Certified_End_Date',
            'Job_Code',
            'Approval_Status',
            'Wage_Rate_1',
            'Rate_Per_1',
            'Max_Rate_1',
            'Part_Time_1',
            'City_1',
            'State_1',
            'Prevailing_Wage_1',
            'Wage_Source_1',
            'Yr_Source_Pub_1',
            'Other_Wage_Source_1',
            'Wage_Rate_2',
            'Rate_Per_2',
            'Max_Rate_2',
            'Part_Time_2',
            'City_2',
            'State_2',
            'Prevailing_Wage_2',
            'Wage_Source_2',
            'Yr_Source_Pub_2',
            'Other_Wage_Source_2',
            'Withdrawn',
            'C_num',
            'CertCode',
            'ReturnFax',
            'EmpName',
            'EmpCity',
            'EmpAddy1',
            'EmpAddy2',
            'EmpState',
            'EmpZip',
            'WageRateFrom',
            'WageRateTo',
            'RatePer',
            'PartTime',
            'BeginDate',
            'EndDate',
            'JobCode',
            'NumImmigrants',
            'JobTitle',
            'WorkCity_1',
            'WorkState_1',
            'PrevWage_1',
            'PrevWagePer_1',
            'WageSource_1',
            'WorkYear1',
            'OtherWageSource1',
            'WorkCity2',
            'WorkState2',
            'PrevWage2',
            'PrevWagePer_2',
            'WageSource_2',
            'WorkYear_2',
            'OtherWageSource2',
            'CertStart',
            'CertEnd',
            'Det_Date',
            'ProcessDate'
        ])
        f.write(HEADER + '\n')
        for i in range(1, 8):
            if i < 7:
                with ZipFile('/'.join(['temp', 'H1B_fax_FY200{}_text.zip'.format(i)])) as zip_file:
                    if i in [1, 3, 5]:
                        with zip_file.open('H1B_Fax_FY200{}_Download.txt'.format(i)) as file:
                            data = pd.read_csv(file, dtype=str, encoding='Latin-1')
                            data = pd.concat([pd.DataFrame(columns=list(range(39))), data], axis=1)
                            data.to_csv(f, header=False, index=False)
                    elif i == 2:
                        with zip_file.open('H1B_FAX_FY200{}_Download.txt'.format(i)) as file:
                            data = pd.read_csv(file, dtype=str, encoding='Latin-1')
                            data = pd.concat([pd.DataFrame(columns=list(range(39))), data], axis=1)
                            data.to_csv(f, header=False, index=False)
                    elif i == 4:
                        with zip_file.open('H1B_fax_FY0{}.txt'.format(i)) as file:
                            data = pd.read_csv(file, dtype=str, encoding='Latin-1')
                            data.drop(data.columns[list(range(32, 38))], axis=1, inplace=True)
                            data = pd.concat([pd.DataFrame(columns=list(range(39))), data], axis=1)
                            data.to_csv(f, header=False, index=False)
                    elif i == 6:
                        with zip_file.open('H1B_Fax_FY200{}_External_Web.txt'.format(i)) as file:
                            data = pd.read_csv(file, dtype=str, encoding='Latin-1')
                            data.drop('DateSigned', axis=1, inplace=True)
                            data = pd.concat([pd.DataFrame(columns=list(range(39))), data], axis=1)
                            data.to_csv(f, header=False, index=False)
            if i > 1:
                with ZipFile('/'.join(['temp', 'H1B_efile_FY0{}_text.zip'.format(i)])) as zip_file:
                    if i in [2, 3, 4, 6]:
                        with zip_file.open('H1B_efile_FY0{}.txt'.format(i)) as file:
                            data = pd.read_csv(file, dtype=str, encoding='Latin-1')
                            data.insert(2, 'Program', np.nan)
                            data.insert(38, 'Withdrawn', np.nan)
                            data = pd.concat([data, pd.DataFrame(columns=list(range(36)))], axis=1)
                            data.to_csv(f, header=False, index=False)
                        if i == 4:
                            with zip_file.open('H1B_efile_FY0{}.txt'.format(i + 1)) as file:
                                data = pd.read_csv(file, dtype=str, encoding='Latin-1')
                                data.insert(2, 'Program', np.nan)
                                data.insert(38, 'Withdrawn', np.nan)
                                data = pd.concat([data, pd.DataFrame(columns=list(range(36)))], axis=1)
                                data.to_csv(f, header=False, index=False)
                    elif i == 5:
                        with zip_file.open('H1B_FY0{}_Efile.txt'.format(i)) as file:
                            data = pd.read_csv(file, dtype=str, encoding='Latin-1')
                            data.drop(data.columns[list(range(16, 21))], axis=1, inplace=True)
                            data.insert(2, 'Program', np.nan)
                            data.insert(38, 'Withdrawn', np.nan)
                            data = pd.concat([data, pd.DataFrame(columns=list(range(36)))], axis=1)
                            data.to_csv(f, header=False, index=False)
                    elif i == 7:
                        with zip_file.open('EFILE_FY200{}.txt'.format(i)) as file:
                            data = pd.read_csv(file, dtype=str, encoding='Latin-1')
                            data = pd.concat([data, pd.DataFrame(columns=list(range(36)))], axis=1)
                            data.to_csv(f, header=False, index=False)
    shutil.rmtree('temp')
    print('Process Complete')

if __name__ == '__main__':
    main()
