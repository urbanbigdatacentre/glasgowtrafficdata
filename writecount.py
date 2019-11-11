#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 16 17:34:15 2019

@author: chauman.fung@glasgow.ac.uk
"""
import pandas as pd
import schedule
import time
import psycopg2
from pandas.io.json import json_normalize
import requests
from sqlalchemy import create_engine
import io
import datetime

headers = {
    'Ocp-Apim-Subscription-Key': '{xxxxxxxxxxxxxxxxxxxxxxxxxxxxx}',
}

url="https://gcc.azure-api.net/traffic/movement?format=json"


def job():
    try:    
        content = requests.get(url,headers).json()  
        content0 = content['d2LogicalModel']['payloadPublication']['publicationTime']
        content1 = json_normalize(content['d2LogicalModel']['payloadPublication']['siteMeasurements'])
        content1['id'] = range(1, len(content1) + 1)
        df4 = pd.DataFrame()
        for i in range(0,len(content1)):
            df1= json_normalize(content1['measuredValue'][i][0])
            df2= json_normalize(content1['measuredValue'][i][1])
            df3= pd.concat([df1, df2],axis=1)
            df4= df4.append(df3,ignore_index=True)
        df4['id'] = range(1, len(df4) + 1)
        df5 = pd.DataFrame()
        df5 = pd.merge(df4,content1, on='id')
        df5 = df5.drop('@index', 1)  
        df5 = df5.drop('basicDataValue.@xsi$type', 1)  
        df5["basicDataValue.vehicleFlow"] = pd.to_numeric(df5["basicDataValue.vehicleFlow"])
        df5["basicDataValue.concentration"] = pd.to_numeric(df5["basicDataValue.concentration"])
        df5['publicationTime']=content0
                
        engine = create_engine('postgresql+psycopg2://USERNAME:PASSWORD@ADDRESS:PORT/NAMEDB')
        df5.head(0).to_sql('count1', engine,if_exists='append',index=False) 
        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        df5.to_csv(output, sep='\t',header=False,index=False)
        output.seek(0)
        
        cur.copy_from(output, 'count1', null="") # null values become ''
        conn.commit()
        cur.close()
        conn.close()
        print(" Successfully written to database ")
        now = datetime.datetime.now()
        print ("Current date and time : ")
        print (now.strftime("%Y-%m-%d %H:%M:%S"))

    except (Exception, psycopg2.DatabaseError) as error :
        print ("Error in writing to database:", error)

# schedule this task every 5 minutes (traffic movement is updated every 5 mins)
schedule.every(5).minutes.do(job)


while True:
    schedule.run_pending()
    time.sleep(1)                   
