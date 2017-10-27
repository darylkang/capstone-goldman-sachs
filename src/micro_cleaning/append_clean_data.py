import pandas as pd
import numpy as np
import os

master_data = pd.DataFrame()

for file in os.listdir('../clean/'):
	if(file == 'macro' or file == 'all_clean_data.csv'):
		print("Not a file to be appended.")
		continue
	print("Appending",file)
	data = pd.read_csv('../clean/{}'.format(file),encoding='utf-8',index_col=False, dtype=str)
	data['SOURCE_FILE']=file
	master_data = master_data.append(data, ignore_index=True)
	print("Master data now has",len(master_data),"rows")

print("Writing master to csv (this may take a while)")
master_data.to_csv('../clean/all_clean_data.csv',encoding='utf-8', index=False)
