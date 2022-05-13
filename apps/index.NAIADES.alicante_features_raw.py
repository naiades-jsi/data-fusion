from src.fusion.stream_fusion import streamFusion, bachFusion

import pandas as pd
import numpy as np
import json
import copy
import time
import datetime
import schedule

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))

measurements_conductivity = [
    'salinity_EA002_26_conductivity',
    'salinity_EA003_36_conductivity',
    'salinity_EA007_36_conductivity',
    'salinity_EA008_36_conductivity'
    ]


#start = 14 #1
stop = 9   #0

template = {
            "aggregate":"mean",
            "measurement":"alicante",
            "fields":["value"],
            "tags":{None: None},
            "window":"1m",
            "when":"-0h"
            }
            
fusions = {}


for m in measurements_conductivity:
    fusion = []
    template['measurement'] = m
    temp = copy.deepcopy(template)
    fusion.append(temp)
    fusions[m] = copy.deepcopy(fusion)

def RunBatchFusionOnce():
    for location in measurements_conductivity:
      today = datetime.datetime.today()
  
      config = {
          "token":"k_TK7JanSGbx9k7QClaPjarlhJSsh8oApCyQrs9GqfsyO3-GIDf_tJ79ckwrcA-K536Gvz8bxQhMXKuKYjDsgw==",
          "url": "http://localhost:8086",
          "organisation": "naiades",
          "bucket": "alicante",
          "startTime":"2021-07-07T00:00:00",
          "stopTime":"2021-07-13T00:00:00",
          "every":"1m",
          "fusion": fusions[location]
      }
  
      today = datetime.datetime.today()
      folder = 'features_data'
  
      config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:00")
      #config['startTime'] = datetime.datetime.utcfromtimestamp((today - datetime.datetime(1970, 1, 2 + start)).total_seconds()).strftime("%Y-%m-%dT%H:00:00")
      
      #print(config['stopTime'] )
      with open(f'{folder}/features_alicante_{location}_raw.json', 'w+')as file_json:
        try:
          lines = file_json.readlines()
          last_line = lines[-1]
          tss = int(json.loads(last_line)['timestamp']/1000 + 60*60)
        except:
          tss = 1649000000
  
      config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:%M:00")
  
      #print(config['startTime'])
  
      file_json = open(f'alicante_salinity_{location}_raw_config.json', 'w')
      file_json.write(json.dumps(config, indent=4, sort_keys=True) )
      file_json.close()
  
      sf2 = bachFusion(config)
  
  
      update_outputs = True
      try:
        fv, t = sf2.buildFeatureVectors()
      except:
        print('Feature vector generation failed')
        update_outputs = False
        
      if(update_outputs):
        with open(f'{folder}/features_alicante_{location}_raw.json', 'a+') as file_json:
          for j in range(t.shape[0]):
              fv_line = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}
              if((all(isinstance(x, (float, int)) for x in fv[j])) and (not np.isnan(fv[j]).any())):
                file_json.write((json.dumps(fv_line) + '\n' ))
          file_json.close()
    
    
        for j in range(t.shape[0]):
            output = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}
            output_topic = f"features_alicante_{location}"
           # Start Kafka producer
            if((all(isinstance(x, (float, int)) for x in fv[j])) and (not np.isnan(fv[j]).any())):
              future = producer.send(output_topic, output)
    
            try:
                record_metadata = future.get(timeout=10)
            except Exception as e:
                print('Producer error: ' + str(e))

#Do batch fusion once per day
schedule.every().hour.do(RunBatchFusionOnce)
print(schedule.get_jobs())
now = datetime.datetime.now()

current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)

RunBatchFusionOnce()
while True:
    
    schedule.run_pending()
    time.sleep(1)