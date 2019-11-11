# -*- coding: utf-8 -*-
"""
Created on Mon Nov 11 11:26:31 2019

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

url="https://gcc.azure-api.net/traffic/events?format=json"


def job():
    try:    
        content = requests.get(url,headers).json()  
        content1 = json_normalize(content['d2lm$d2LogicalModel']['d2lm$payloadPublication']['d2lm$situation'])
        content1.columns = ['id','confidentiality','informationStatus','recordId','xsiType','CreationTime',
                            'version','versionTime','firstSupplierVersionTime','probOfOccurrence','validityStatus',
                            'overallStartTime','overallEndTime','droplang','dropmessage','lang','message',
                            'tpegPointLocation','tpegPointLocationXsiType','tpegDirection','tpegPointLocationType',
                            'tpegPointLocationPointXsiType','latitude','longitude','tpegOtherPointDescriptor',
                            'disturbanceActivityType','networkManagementType','effectOnRoadLayout','roadManagement']
        content1 = content1.drop('droplang', 1)    
        content1 = content1.drop('dropmessage', 1)     
        content1 = content1.drop('tpegOtherPointDescriptor',1)
        content1 = content1.fillna('.')                     
        engine = create_engine('postgresql+psycopg2://USERNAME:PASSWORD@ADDRESS:PORT/NAMEDB') #USERNAME:PASSWORD@ADDRESS:PORT/NAMEDB
        content1.head(0).to_sql('events', engine,if_exists='append',index=False) 
        conn = engine.raw_connection()
        cur = conn.cursor()
        output = io.StringIO()
        content1.to_csv(output, sep='\t',header=False,index=False)
        output.seek(0)
        cur.copy_from(output,'events', null="") # null values become ''
        conn.commit()
        cur.close()
        conn.close()
        print(" Successfully written to database ")
        now = datetime.datetime.now()
        print ("Current date and time : ")
        print (now.strftime("%Y-%m-%d %H:%M:%S"))

    except (Exception, psycopg2.DatabaseError) as error :
        print ("Error in writing to database:", error)

# schedule this task every ? minutes 
schedule.every(?).minutes.do(job)


while True:
    schedule.run_pending()
    time.sleep(1)                   
