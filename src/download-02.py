import os
import requests

"""
    Update new data resource here
"""


resources = ["pdf/PerformanceData/2017/H-1B_Disclosure_Data_FY17.xlsx",
    "docs/Performance_Data/Disclosure/FY15-FY16/H-1B_Disclosure_Data_FY16.xlsx", 
    "docs/py2015q4/H-1B_Disclosure_Data_FY15_Q4.xlsx",
    "docs/py2014q4/H-1B_FY14_Q4.xlsx",
    "docs/lca/LCA_FY2013.xlsx",
    "docs/py2012_q4/LCA_FY2012_Q4.xlsx",
    "docs/lca/H-1B_iCert_LCA_FY2011_Q4.xlsx",
    "docs/lca/H-1B_FY2010.xlsx",
    "docs/lca/Icert_%20LCA_%20FY2009.xlsx",
    "docs/lca/H-1B_Case_Data_FY2009.xlsx",
    "docs/lca/H-1B_Case_Data_FY2008.xlsx"]

def download(resource):
    url = "https://www.foreignlaborcert.doleta.gov/"
    link = url + resource
    print(link)
    name = link.split('/')[-1]
    resp = requests.get(link)
    if not os.path.exists('data'):
        os.mkdir('data')
    with open('data/' + name, 'wb') as fp:
        fp.write(resp.content)

for i in resources:
    download(i)


