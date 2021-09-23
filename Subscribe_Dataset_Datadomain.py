# -*- coding: utf-8 -*-
import pandas as pd
import requests
import json
#import os as os
#import numpy as np
import logging  

logger=logging.getLogger()
logger.setLevel(logging.DEBUG)
#import requests
requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning)

search_url='https://<HOSTNAME>:<PORT>/access/2/catalog/data/search'
username='<USERNAME>'
password='<PASSWORD>'
#param_q={'core.classType':'*.Table','core.resourceName':'sales_sachin'}
payload={
          'q':'com.infa.ldm.profiling.dataDomainsAccepted:Customer_ID'
         # 'attributeId': 'com.infa.ldm.profiling.lastProfileTime'
        }
        
headers = {
  'Content-Type': 'application/json',
  'Accept': 'application/json',
  'Authorization': 'Basic QWRtaW5pc3RyYXRvcjpBZG1pbmlzdHJhdG9y'
 #'Authorization': 'Basic QWRtaW5pc3RyYXRvcjpBZG1pbmlzdHJhdG9y'
}
from requests.auth import HTTPBasicAuth
get_response = requests.get(search_url,auth=HTTPBasicAuth(username, password),params = payload, headers=headers,verify=False)

pr_getvalueJson = get_response.json()
get_list=pr_getvalueJson ['hits']

df=pd.DataFrame(get_list)
df=df.drop(['href','nativeId','values','subHits'], axis = 1)

df['Table_name'] = df.id.str.extract(r'\b(\w+)$',expand = True)

df['bar'] = df.apply(lambda row : row['id'].replace(str(row['Table_name']), ''), axis=1)
df['bar'] = df.bar.str[:-1]

def subscribe(row):
    print("name of the table")
    #print (row['bar'])
    URL_subscr= 'https://<HOSTNAME>:<PORT>/access/2/catalog/data/subscriptions'
    
    payload_subs = {
            #{
         "items": [
         {
      "changeTypes": [
        "SOURCE",
        "INFERENCES_ANNOTATIONS",
        "COLLABORATION"
      ],
      #"objectId": "sales_sachin://PDB01/SJAINA/ORC_TGT"
      "objectId":row['bar']
      }
    ]
   }
       
    #print (json.dumps(payload_subs))
    requests.put(URL_subscr, auth=HTTPBasicAuth(username, password),headers=headers, data=json.dumps(payload_subs),verify=False)
    #print (a)
 
countFlag=0
#print (df.bar.count())
for index, row in df.iterrows():
    if (df.bar.count())>0:
        result=subscribe(row)
        #print(result)
    else:
        pass
#    
