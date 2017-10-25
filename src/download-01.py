import numpy as np
import pandas as pd
import os
import requests

from zipfile import ZipFile

def download(URL):
    file = URL.split('/')[-1]
    r = requests.get(URL, stream=True)
    r.raise_for_status()
    with open('/'.join(['raw_data', file]), 'w+b') as f:
        for chunk in r.iter_content(chunk_size=2048):
            if chunk:
                f.write(chunk)

def main():
    if not os.path.exists('raw_data'):
        os.mkdir('raw_data')
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

if __name__ == '__main__':
    main()
