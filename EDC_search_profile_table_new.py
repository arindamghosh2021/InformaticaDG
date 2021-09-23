# -*- coding: utf-8 -*-
"""
Created on Mon Jul 26 13:34:55 2021

@author: sjain
"""

import pandas as pd
import requests
 
requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning)

search_url='https://inedclab0101.informatica.com:9085/access/2/catalog/data/search'
username='Administrator'
password='Administrator'
#param_q={'core.classType':'*.Table','core.resourceName':'sales_sachin'/Oracle_DD}
payload={
          'q':'core.classType:*.Table AND core.resourceName:"Oracle_DD"',
          'pageSize':100
         # 'attributeId': 'com.infa.ldm.profiling.lastProfileTime'
        }
        
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json',
  'Authorization': 'Basic QWRtaW5pc3RyYXRvcjpBZG1pbmlzdHJhdG9y'
 #'Authorization': 'Basic QWRtaW5pc3RyYXRvcjpBZG1pbmlzdHJhdG9y'
}
#headers  = {"Content-Type": "application/json"}
from requests.auth import HTTPBasicAuth
get_response = requests.get(search_url,auth=HTTPBasicAuth(username, password),params = payload, headers=headers,verify=False)
#print(get_response.text)

pr_getvalueJson = get_response.json()

get_list=pr_getvalueJson ['hits']

#logger.info ("Getlist values")
df35=pd.DataFrame(columns = ['TableName', 'Profile','Last Profile Time'])
df36=pd.DataFrame(columns = ['TableName', 'Profile','Last Profile Time'])
df=pd.DataFrame(get_list)
for indices, row in df.iterrows():
    #print(row["values"])
    df32=row["values"]
    df34=pd.DataFrame(df32)
    if 'com.infa.ldm.profiling.lastProfileTime' in df34.attributeId.values:
        #print("yes")
        t=df34[df34['attributeId']=='core.name'].index.values
        j=df34[df34['label']=='Last Profile Time'].index.values
        #print(df34['value'][t])
        t1=df34['value'][t]
        #print(t1)
        j1=df34['value'][j]
        #print (j1)
        df35['TableName']=pd.Series(t1).values
        df35['Profile']='y'
        df35['Last Profile Time']=pd.Series(j1).values        
        df36=df36.append(df35,ignore_index = True)
         
    else:
        t=df34[df34['attributeId']=='core.name'].index.values
        t1=df34['value'][t]
        df35['TableName']=pd.Series(t1).values
        df35['Profile']='N'
        df35['Last Profile Time']='null'        
        df36=df36.append(df35,ignore_index = True)
        pass

df36.to_csv("ProfileTables11.csv", index=False)   
  

        
        



 