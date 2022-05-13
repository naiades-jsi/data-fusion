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

measurements_analog = [
    'pressure5770',
    'pressure5771',
    'pressure5772',
    'pressure5773'
    ]

#start = 14 #1
stop = 9   #0

template = {
            "aggregate":"mean",
            "measurement":"braila",
            "fields":["value"],
            "tags":{None: None},
            "window":"5m",
            "when":"-0h"
            }


fusions = {}
for m in measurements_analog:
    fusion = []
    template['measurement'] = m
    for i in range(1):
        temp = copy.deepcopy(template)
        temp['when'] = f'-0h'
        fusion.append(temp)
    fusions[m] = copy.deepcopy(fusion)
        



#while (True):
wait = True
once = True

def RunBatchFusionOnce():
    for location in measurements_analog:
      today = datetime.datetime.today()
  
      config = {
          "token":"k_TK7JanSGbx9k7QClaPjarlhJSsh8oApCyQrs9GqfsyO3-GIDf_tJ79ckwrcA-K536Gvz8bxQhMXKuKYjDsgw==",
          "url": "http://localhost:8086",
          "organisation": "naiades",
          "bucket": "braila",
          "startTime":"2021-07-07T00:00:00",
          "stopTime":"2021-07-13T00:00:00",
          "every":"5m",
          "fusion": fusions[location]
      }
  
      today = datetime.datetime.today()
      folder = 'features_data'
  
      config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
      
      print(config['stopTime'] )
  
      file_json = open(f'{folder}/features_braila_{location}_anomaly.json', 'r')
  
      lines = file_json.readlines()
      last_line = lines[-1]
      tss = int(json.loads(last_line)['timestamp']/1000 + 5*60)
  
      config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:%M:%S")
  
      print(config['startTime'])
  
      file_json = open(f'braila_{location}_anomaly_config.json', 'w')
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
        
        for j in range(t.shape[0]):
            if(not any(v is None for v in fv[j])):
                if(not np.isnan(fv[j]).any()):
                  fv_line = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}
            
              #data is uploaded at different times - this ensures that FV's won't be sent if data hasn't been uploaded for one or more of the sensors
                  with open(f'{folder}/features_braila_{location}_anomaly.json', 'a') as file_json:
                    file_json.write((json.dumps(fv_line) + '\n' ))
  
  
        for j in range(t.shape[0]):
            if(not any(v is None for v in fv[j])):
                if(not np.isnan(fv[j]).any()):
                  output = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}
                  
                  output_topic = f'features_braila_{location}_anomaly'
                  
                  future = producer.send(output_topic, output)
          
                  try:
                      record_metadata = future.get(timeout=10)
                  except Exception as e:
                      print('Producer error: ' + str(e))

#Do batch fusion once per day
schedule.every().hour.do(RunBatchFusionOnce)
  
  
#print(schedule.get_jobs())
now = datetime.datetime.now()

current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)

RunBatchFusionOnce()
while True:
    
    schedule.run_pending()
    time.sleep(1)