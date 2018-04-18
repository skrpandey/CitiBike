
# coding: utf-8

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from time import localtime, strftime
from pyspark.sql import SQLContext
from pyspark.sql.types import StructType,StructField,StringType,DateType,IntegerType,LongType

import urllib
import json
import requests
import pandas as pd

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext(appName="CitiBike_download")
sqlContext = SQLContext(sc)

fileNameList = ['station_information','station_status','system_alerts','system_regions']  
cleanedData = []
strData = ''

# function to clean the Json
def jSonCleaner(strData):
    strData = r.text[r.text.find("["):-2]
    return strData


for nfile in fileNameList:
    #removing unwanted string from the starting of the json file and from starting    
    #reading the json file using requests
    r = requests.get("https://gbfs.citibikenyc.com/gbfs/en/" +nfile + ".json")
    # correct json after cleaning
    cleanedData.append(jSonCleaner(r.text)) 


# ##  Getting JSON  List from formatted string
station_information = json.loads(cleanedData[0])
station_status = json.loads(cleanedData[1])
system_alerts = json.loads(cleanedData[2])
system_regions = json.loads(cleanedData[3])

#Changing Python List to Pandas DF
pdf_station_info= pd.DataFrame(station_information)
pdf_station_status= pd.DataFrame(station_status)
pdf_system_alerts= pd.DataFrame(system_alerts)
pdf_system_regions= pd.DataFrame(system_regions)


# Datatype change : station_ids -> List to station_id string and renaming the column too
columns = ['alert_id','last_updated', 'station_id','summary','type']
new_pdf_system_alerts = pd.DataFrame(columns=columns)
for index, row in pdf_system_alerts.iterrows():
    if len(row.station_ids)==1:
        new_pdf_system_alerts.loc[len(new_pdf_system_alerts)]=[row['alert_id'],row['last_updated'],row['station_ids'][0],row['summary'],row['type']] 
    else:
        for val in row.station_ids[:]:
            new_pdf_system_alerts.loc[len(new_pdf_system_alerts)]=[row['alert_id'],row['last_updated'],val,row['summary'],row['type']]

schema_system_alerts = StructType([StructField("alert_id", LongType(), True), StructField("last_updated", LongType(), False), StructField("station_id", StringType(), False), StructField("summary", StringType(), False), StructField("type", StringType(), False)])            
sqldf_system_alerts = sqlContext.createDataFrame([],schema_system_alerts)

pdf_station_info=pdf_station_info.drop(['eightd_has_key_dispenser','rental_methods',
                                        'eightd_station_services','rental_url'], axis=1)
pdf_station_status=pdf_station_status.drop(['eightd_active_station_services'], axis=1)

#Changing Pandas DF to SQL for aggregation 
sqldf_station_info=sqlContext.createDataFrame(pdf_station_info)   
sqldf_station_status=sqlContext.createDataFrame(pdf_station_status)
if len(pdf_system_alerts>1):
    sqldf_system_alerts=sqlContext.createDataFrame(new_pdf_system_alerts)  
if len(pdf_system_regions):
    sqldf_system_regions=sqlContext.createDataFrame(pdf_system_regions)




joined_info_status = sqldf_station_info.join(sqldf_station_status,['station_id'],'outer')
join_info_status_alert = joined_info_status.join(sqldf_system_alerts,['station_id'],'outer')
join_info_sta_alt_reg = join_info_status_alert.join(sqldf_system_regions,['region_id'],'outer')


pd_df=join_info_sta_alt_reg.toPandas()
pd_df['Date']=strftime("%Y/%m/%d %H:%M:%S", localtime())


from time import gmtime, strftime
timestr = strftime("%Y%m%d_%H%M%S", localtime())
pd_df.to_csv('/citibike/output/'+timestr+'.csv',index=False)

sc.stop()

