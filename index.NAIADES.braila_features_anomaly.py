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
    'flow211106H360',
    'flow211206H360', 
    'flow211306H360', 
    'flow318505H498'
    ]

#start = 14 #1
stop = 9   #0

template = {
            "aggregate":"mean",
            "measurement":"braila",
            "fields":["flow_rate_value"],
            "tags":{None: None},
            "window":"5m",
            "when":"-0h"
            }


fusions = {}
for m in measurements_analog:
    fusion = []
    template['measurement'] = m
    temp = copy.deepcopy(template)
    temp['when'] = "-0h"
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
  
      #print(json.dumps(config, indent=4, sort_keys=True))
  
      today = datetime.datetime.today()
      folder = 'features_data'
  
      config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:00:00")
      #config['startTime'] = datetime.datetime.utcfromtimestamp((today - datetime.datetime(1970, 1, 2 + start)).total_seconds()).strftime("%Y-%m-%dT%H:00:00")
      
      print(config['stopTime'] )
  
      file_json = open(f'{folder}/features_braila_{location}_anomaly.json', 'r')
  
      lines = file_json.readlines()
      last_line = lines[-1]
      tss = int(json.loads(last_line)['timestamp']/1000 + 5*60)
      
      #print(last_line)
      #print(tss)
      #print(datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00"))
  
      config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00")
  
      print(config['startTime'])
  
      file_json = open(f'braila_{location}_night_anomaly_config.json', 'w')
      file_json.write(json.dumps(config, indent=4, sort_keys=True) )
      file_json.close()
  
      #sf2 = bachFusion(config)
      sf2 = bachFusion(config)
  
  
      update_outputs = True
      try:
        fv, t = sf2.buildFeatureVectors()
      except:
        print('Feature vector generation failed')
        update_outputs = False
        
      if(update_outputs):
        
        for j in range(t.shape[0]):
            if(not np.isnan(fv[j]).any()):
              fv_line = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}
            
              #data is uploaded at different times - this ensures that FV's won't be sent if data hasn't been uploaded for one or more of the sensors
              with open(f'{folder}/features_braila_{location}_anomaly.json', 'a') as file_json:
                file_json.write((json.dumps(fv_line) + '\n' ))
  
  
        for j in range(t.shape[0]):
            if(not np.isnan(fv[j]).any()):
              output = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}
              output_topic = f'features_braila_{location}_anomaly'
              future = producer.send(output_topic, output)
      
              try:
                  record_metadata = future.get(timeout=10)
              except Exception as e:
                  print('Producer error: ' + str(e))

#Do batch fusion once per hour
schedule.every().hour.do(RunBatchFusionOnce)
  
  
#print(schedule.get_jobs())
now = datetime.datetime.now()

current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)

RunBatchFusionOnce()
while True:
    
    schedule.run_pending()
    time.sleep(1)