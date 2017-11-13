import pandas as pd
import numpy as np
import re
from fuzzywuzzy import fuzz
from cleanco import cleanco #Altered version of cleanco v1.3


numberOfEmployersToCluster = 100
testThresholds = [100, 99, 98, 95, 90, 85, 80, 75, 70, 65, 60, 55, 50]


print("Loading data...")
data = pd.read_csv('../../clean/all_clean_data.csv', encoding="utf-8", dtype=str)
print("Data loading complete. Dataframe shape is ", data.shape)

data = data[data['CASE_STATUS'] == 'CERTIFIED']
data['SOURCE_FILE'] = data['SOURCE_FILE'].str.replace('.csv', ''
	).str.replace('yr', '').str.replace('efile_2006', '2006_efile').str.replace('efile_2007', '2007_efile'
	).str.replace('efile_2008', '2008_efile').str.replace('fax_2006', '2006_fax'
	).str.replace('2009_old', '2009_efile').str.replace('2009_new', '2009')

data['NAIC_CODE'] = data['NAIC_CODE'].apply(lambda x: np.nan if pd.isnull(x) else str(x).replace('.0',''))
data['EMPLOYER_AREA_CODE'] = data['EMPLOYER_AREA_CODE'].apply(lambda x: np.nan if pd.isnull(x) else str(x))

data = data.loc[:,['EMPLOYER_NAME', 'NAIC_CODE',  
	'EMPLOYER_ADDRESS', 'EMPLOYER_AREA_CODE', 'EMPLOYER_CITY', 'EMPLOYER_STATE', 'EMPLOYER_POSTAL_CODE', 
	'SOURCE_FILE']]

data = data.astype(str).applymap(lambda x: x.strip(' ').upper())

e = data.groupby( ['EMPLOYER_NAME', 'NAIC_CODE',  
	'EMPLOYER_ADDRESS', 'EMPLOYER_AREA_CODE', 'EMPLOYER_CITY', 'EMPLOYER_STATE', 'EMPLOYER_POSTAL_CODE']
	)['SOURCE_FILE'].value_counts().unstack(fill_value=0).reset_index()

e = e.replace('NAN', np.nan)

e['TOTAL'] = e.sum(axis=1)

e = e.sort_values(['EMPLOYER_NAME', 'EMPLOYER_STATE', 'EMPLOYER_CITY', 
	'EMPLOYER_POSTAL_CODE', 'EMPLOYER_ADDRESS', 'NAIC_CODE', 'EMPLOYER_AREA_CODE'])

print("Employers shape is ", e.shape)


e['name'] = None
e['co_type'] = None
e['subsidiaryOf'] = None
e['alias'] = None
e['formerly'] = None
e['other'] = None

def createIndustryMapping(x):
    if(pd.isnull(x)):
        industry = None
    else:
        naic = str(x)
        if(naic[0:4] == '6113'):
            industry = 'Colleges & Universities'
        elif(naic[0:2] in ['61', '92']):
            industry = 'Other Educational, Public Affairs'
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
        elif(naic[0:3] == '335'):
            industry = 'Manufacturing - Computers & Electronics'
        elif(naic[0:2] in ['31', '32', '33']):
            industry = 'Manufacturing - Other'
        elif(naic[0:2] in ['52', '53']):
            industry = 'Finance, Insurance, Real Estate'
        elif(naic[0:2] in ['42', '44', '45', '48', '49']):
            industry = 'Trade, Transportation, Warehousing'
        elif(naic[0:2] in ['62']):
            industry = 'Healthcare'
        else:
            #Commodities, Energy, Utilities, Construction, Arts & Entertainment, Accomodation, Other Services, & Unknown
            industry = 'Other'
    return industry

e['industry'] = e['NAIC_CODE'].apply(createIndustryMapping) 

#Begin employer name cleaning
phraseLookup = {
        'subsidiaryOf': [' A SUBSIDIARY OF ', ' SUBSIDIARY OF ', ' A SUB OF ', ' SUB OF ', 
                         ' A DIVISION OF ', ' DIVISION OF ', ' A PART OF ', ' PART OF '],
        'alias': [' AKA ', ' A/K/A ', ' DBA ', ' D/B/A '],
        'formerly': [' PREVIOUSLY KNOWN AS ', ' PREVIOUSLY KNOWN ', ' PREVIOUSLY KNOW AS ' , ' PREVIOUSLY ', 
                     ' FORMERLY KNOWN AS ', ' FORMERLY KNOW AS ', ' FORMERLY ', ' FKA ']
    }

regex = {}
for k,v in phraseLookup.items():
    regex[k] = re.compile('|'.join(v))

def cleanEmployerName(x):
    name = x['EMPLOYER_NAME']
    
    if(not pd.isnull(name)):   
        name = re.sub('["\']' , '' , re.sub(r'[;:-=~]+', ',', name))
        
        #Get secondary names
        if(re.search(', A ', name)):
            main = re.sub(', A .*', '', name)
            secondary = re.sub('.*, ', '', name)
        elif(re.search('\(', name)):
            main = re.sub('\(.*', '', name)
            after = re.sub('.*\(', ' ', name)
            secondary = re.sub('\).*', '', after)
            if(re.sub('\)', '', after) != after):
                main += re.sub('.*\)', '', after)
        else:
            main = name.strip()
            secondary = None
    
        secondTypes = {'subsidiaryOf':None, 'alias':None, 'formerly':None, 'other':None}
        
        for k,v in regex.items():
            match = regex[k].search(main)
            if(match):
                secondTypes[k] = main[match.end():]
                main = main[:match.start()]
            elif(secondary):
                match = regex[k].search(secondary)
                if(match):
                    secondTypes[k] = secondary[match.end():]
                    secondary = secondary[:match.start()]

        if(secondary):
            secondTypes['other'] = secondary.strip()
        
        co = cleanco(main)
        cotype = co.type()
        if(cotype):
            main = co.clean_name()
            x['co_type'] = cotype[0]
        
        for k,v in secondTypes.items() :
            if(v):
                co = cleanco(v)
                secondTypes[k] = co.clean_name()
                if(not cotype):
                    cotype = co.type()
                    if(cotype):
                        x['co_type'] = cotype[0]
        
        x['name'] = re.sub('[ ]+', ' ', re.sub('[^\w ]', '', re.sub('([^\w ][ ]|[ ][^\w ])', ' ', ' ' + main + ' '))).strip()
        for k,v in secondTypes.items():
            if(v):
                x[k] = re.sub('[ ]+', ' ', re.sub('[^\w ]', '', re.sub('([^\w ][ ]|[ ][^\w ])', ' ', ' ' + v + ' '))).strip()
                
    return x
    
e = e.apply(cleanEmployerName, axis = 1)
print('Employer name cleaning complete.')

#Create similarity score for any two employers x & y
def compareEmployers(x, y):
    #define weights
    w = {'employer_name': 25,
         'name': 50,
         'subsidiaryOf': 10,
         'alias': 10,
         'formerly': 10,
         'other': 5,
         'nameWithSubsidiaryOf': 25, #best of name, name with subsidiaryOf
         'nameWithAlias': 25, #best of name, name with alias
         'nameWithFormerly': 25, #best of name, name with formerly
         'nameWithOther': 5, #best of name, name with other
         'naic': 15,
         'industry': 15, 
         'address': 30, 
         'city': 15, 
         'state': 5, 
         'zipcode': 15, 
         'areacode': 10}
      
    employer_name = max([fuzz.token_set_ratio(e['EMPLOYER_NAME'][x], e['EMPLOYER_NAME'][y]),
                         fuzz.partial_ratio(e['EMPLOYER_NAME'][x], e['EMPLOYER_NAME'][y]) ])
    
    name = max([fuzz.token_set_ratio(e['name'][x], e['name'][y]),
                fuzz.partial_ratio(e['name'][x], e['name'][y]) ])
    
    if(pd.isnull(e['subsidiaryOf'][x]) or pd.isnull(e['subsidiaryOf'][y])):
        subsidiaryOf = 0
        w['subsidiaryOf'] = 0
    else:
        subsidiaryOf = max([fuzz.token_set_ratio(e['subsidiaryOf'][x], e['subsidiaryOf'][y]),
                            fuzz.partial_ratio(e['subsidiaryOf'][x], e['subsidiaryOf'][y]) ])
    
    if(pd.isnull(e['alias'][x]) or pd.isnull(e['alias'][y])):
        alias = 0
        w['alias'] = 0
    else:
        alias = max([fuzz.token_set_ratio(e['alias'][x], e['alias'][y]),
                     fuzz.partial_ratio(e['alias'][x], e['alias'][y]) ])

    if(pd.isnull(e['formerly'][x]) or pd.isnull(e['formerly'][y])):
        formerly = 0
        w['formerly'] = 0
    else:
        formerly = max([fuzz.token_set_ratio(e['formerly'][x], e['formerly'][y]),
                        fuzz.partial_ratio(e['formerly'][x], e['formerly'][y]) ])

    if(pd.isnull(e['other'][x]) or pd.isnull(e['other'][y])):
        other = 0
        w['other'] = 0
    else:
        other = max([fuzz.token_set_ratio(e['other'][x], e['other'][y]),
                     fuzz.partial_ratio(e['other'][x], e['other'][y]) ])
    
    
    nameWithSubsidiaryOf = max(
        name,
        0 if pd.isnull(e['subsidiaryOf'][x]) else max(
            fuzz.token_set_ratio(e['subsidiaryOf'][x], e['name'][y]),
            fuzz.partial_ratio(e['subsidiaryOf'][x], e['name'][y])        
        ),
        0 if pd.isnull(e['subsidiaryOf'][y]) else max(
            fuzz.token_set_ratio(e['name'][x], e['subsidiaryOf'][y]),
            fuzz.partial_ratio(e['name'][x], e['subsidiaryOf'][y])        
        )
    )
    
    nameWithAlias = max(
        name,
        0 if pd.isnull(e['alias'][x]) else max(
            fuzz.token_set_ratio(e['alias'][x], e['name'][y]),
            fuzz.partial_ratio(e['alias'][x], e['name'][y])        
        ),
        0 if pd.isnull(e['alias'][y]) else max(
            fuzz.token_set_ratio(e['name'][x], e['alias'][y]),
            fuzz.partial_ratio(e['name'][x], e['alias'][y])        
        )
    )
    
    nameWithFormerly = max(
        name,
        0 if pd.isnull(e['formerly'][x]) else max(
            fuzz.token_set_ratio(e['formerly'][x], e['name'][y]),
            fuzz.partial_ratio(e['formerly'][x], e['name'][y])        
        ),
        0 if pd.isnull(e['formerly'][y]) else max(
            fuzz.token_set_ratio(e['name'][x], e['formerly'][y]),
            fuzz.partial_ratio(e['name'][x], e['formerly'][y])        
        )
    )    
    
    nameWithOther = max(
        name,
        0 if pd.isnull(e['other'][x]) else max(
            fuzz.token_set_ratio(e['other'][x], e['name'][y]),
            fuzz.partial_ratio(e['other'][x], e['name'][y])        
        ),
        0 if pd.isnull(e['other'][y]) else max(
            fuzz.token_set_ratio(e['name'][x], e['other'][y]),
            fuzz.partial_ratio(e['name'][x], e['other'][y])        
        )
    )
    
    
    if(pd.isnull(e['NAIC_CODE'][x]) or pd.isnull(e['NAIC_CODE'][y])):
        naic = 0
        w['naic'] = 0
    else:
        if(e['NAIC_CODE'][x] == e['NAIC_CODE'][y]):
            naic = 100
        elif(e['NAIC_CODE'][x][0:6] == e['NAIC_CODE'][y][0:6]):
            naic = 98
        elif(e['NAIC_CODE'][x][0:4] == e['NAIC_CODE'][y][0:4]):
            naic = 90
        elif(e['NAIC_CODE'][x][0:2] == e['NAIC_CODE'][y][0:2]):
            naic = 75
        else:
            naic = 0
    
    if(pd.isnull(e['industry'][x]) or pd.isnull(e['industry'][y])):
        industry = 0
        w['industry'] = 0
    else:
        if(e['industry'][x] == e['industry'][y]):
            industry = 100
        else:
            industry = 0
    
    if(pd.isnull(e['EMPLOYER_ADDRESS'][x]) or pd.isnull(e['EMPLOYER_ADDRESS'][y])):
        address = 0
        w['address'] = 0
    else:
        address = max([fuzz.token_set_ratio(e['EMPLOYER_ADDRESS'][x], e['EMPLOYER_ADDRESS'][y]),
                       fuzz.partial_ratio(e['EMPLOYER_ADDRESS'][x], e['EMPLOYER_ADDRESS'][y]) ])
    
    if(pd.isnull(e['EMPLOYER_CITY'][x]) or pd.isnull(e['EMPLOYER_CITY'][y])):
        city = 0
        w['city'] = 0
    else:
        city = max([fuzz.token_set_ratio(e['EMPLOYER_CITY'][x], e['EMPLOYER_CITY'][y]),
                    fuzz.partial_ratio(e['EMPLOYER_CITY'][x], e['EMPLOYER_CITY'][y]) ])
    
    if(pd.isnull(e['EMPLOYER_STATE'][x]) or pd.isnull(e['EMPLOYER_STATE'][y])):
        state = 0
        w['state'] = 0
    else:
        state = fuzz.ratio(e['EMPLOYER_STATE'][x], e['EMPLOYER_STATE'][y])
    
    if(pd.isnull(e['EMPLOYER_POSTAL_CODE'][x]) or pd.isnull(e['EMPLOYER_POSTAL_CODE'][y])):
        zipcode = 0
        w['zipcode'] = 0
    else:
        zipcode = fuzz.ratio(e['EMPLOYER_POSTAL_CODE'][x], e['EMPLOYER_POSTAL_CODE'][y])
    
    if(pd.isnull(e['EMPLOYER_AREA_CODE'][x]) or pd.isnull(e['EMPLOYER_AREA_CODE'][y])):
        areacode = 0
        w['areacode'] = 0
    elif(e['EMPLOYER_AREA_CODE'][x] == e['EMPLOYER_AREA_CODE'][y]):
        areacode = 1
    else:
        areacode = 0
    
    return (employer_name*w['employer_name'] + name*w['name'] + 
            subsidiaryOf*w['subsidiaryOf'] + alias*w['alias'] + formerly*w['formerly'] + other*w['other'] + 
            nameWithSubsidiaryOf*w['nameWithSubsidiaryOf'] + nameWithAlias*w['nameWithAlias'] + 
            nameWithFormerly*w['nameWithFormerly'] + nameWithOther*w['nameWithOther'] +
            naic*w['naic'] + industry*w['industry'] + 
            address*w['address'] + city*w['city'] + 
            state*w['state'] + zipcode*w['zipcode'] + areacode*w['areacode']) / sum(w.values())


#Create Distance Matrix (ints to save space)
eDist = np.zeros((numberOfEmployersToCluster, numberOfEmployersToCluster), dtype=int)
print("Calculating Employer distance matrix for ", str(len(eDist))," employers.")

for x in range(0, eDist.shape[0]):
    for y in range(0, x):
        eDist[x, y] = compareEmployers(x,y)

print("Distance matrix calculated.")

#np.savetxt("eDist.csv", eDist, delimiter=',')

#Create unique employers based on similarity score above given threshold
def clusterEmployers(threshold):
    indices = pd.DataFrame({'entityId': np.empty(len(eDist)), 'numLinksStart': np.zeros(len(eDist))})
    
    links = []    
    for i,j in zip(*np.where(eDist >= threshold)):
        links.append((i,j))
        indices.loc[i, 'numLinksStart'] += 1
        indices.loc[j, 'numLinksStart'] += 1
        
    indices['numLinksRemaining'] = indices['numLinksStart'] 
    entityCount = 0
    
    while indices['numLinksRemaining'].sum() > 0:
        entityCount += 1
        entityIndices = [indices.sort_values(by = 'numLinksRemaining', ascending = False).iloc[0].name]
        newIndicesToCheckNext = entityIndices #prime loop
        linksToRemove = []
        
        while len(newIndicesToCheckNext) > 0:
            indicesToCheck = newIndicesToCheckNext
            newIndicesToCheckNext = []
            for link in links:
                if link[0] in indicesToCheck:
                    if link[1] not in newIndicesToCheckNext:
                        newIndicesToCheckNext.append(link[1])
                        entityIndices.append(link[1])
                    linksToRemove.append(link)
                if link[1] in indicesToCheck:
                    if link[0] not in newIndicesToCheckNext:
                        newIndicesToCheckNext.append(link[0])
                        entityIndices.append(link[0])

            #Remove linksToRemove from links
            links = [link for link in links if link not in linksToRemove]
    
        indices.loc[entityIndices, 'entityId'] = entityCount
        
        indices['numLinksRemaining'] = 0
        for i,j in links:
            indices.loc[i, 'numLinksRemaining'] += 1
            indices.loc[j, 'numLinksRemaining'] += 1
    
    for i in indices.index[indices['numLinksStart'] == 0].tolist():
        entityCount +=1
        indices.loc[i, 'entityId'] = entityCount
        
    return indices['entityId']



uniqueEntities = {}

eEntity = e.copy()

for threshold in testThresholds:
    eEntity[('entityThreshold' + str(threshold))] = clusterEmployers(threshold)
    uniqueEntities[threshold] = len(eEntity[('entityThreshold' + str(threshold))].unique())

print("Number of Unique Entities at various thresholds: ", uniqueEntities)







