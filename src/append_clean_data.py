import pandas as pd
import numpy as np
import os

master_data = pd.DataFrame()


filenames=['efile_2006.csv','efile_2007.csv','efile_2008.csv','fax_2006.csv','2015.csv','2016.csv','2017.csv']

for file in os.listdir('./clean/'):
	print("Appending",file)
	data = pd.read_csv('clean/{}'.format(file),encoding='utf-8',index_col=False, dtype=str)
	data['SOURCE_FILE']=file
	master_data = master_data.append(data, ignore_index=True)
	print("Master data now has",len(master_data),"rows")

print("Writing master to csv (this may take a while)")
master_data.to_csv('clean/all_clean_data.csv',encoding='utf-8', index=False)
