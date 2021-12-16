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


template = {
            "aggregate":"mean",
            "measurement":"salinity_EA001_36_level",
            "fields":["value"],
            "tags":{None: None},
            "window":"4m",
            "when":"-0h"
            }

fusion = []




# Fusion contains 5m mean of level value
numSamples = 80
for i in range(numSamples):
    temp = copy.deepcopy(template)
    temp['when'] = f'-{4*i}m'
    fusion.append(temp)




def RunBatchFusionOnce():
    today = datetime.datetime.today()

    config = {
        "token":"k_TK7JanSGbx9k7QClaPjarlhJSsh8oApCyQrs9GqfsyO3-GIDf_tJ79ckwrcA-K536Gvz8bxQhMXKuKYjDsgw==",
        "url": "http://localhost:8086",
        "organisation": "naiades",
        "bucket": "alicante",
        "startTime":"2021-07-07T00:00:00",
        "stopTime":"2021-07-13T00:00:00",
        "every":f"{numSamples*4}m",
        "fusion": fusion
    }


    today = datetime.datetime.today()
    folder = 'features_data'

    config['stopTime'] =datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    
    print(config['stopTime'] )

    file_json = open(f'{folder}/features_alicante_salinity_EA001_36_level_duration.json', 'r')

    lines = file_json.readlines()
    last_line = lines[-1]
    tss = int(json.loads(last_line)['timestamp'] + numSamples*5*60)
    

    config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:%M:%S")

    print(config['startTime'])

    file_json = open(f'config_alicante_salinity_EA001_36_level_duration.json', 'w')
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
      
      #duration, time
      tosend = []
      for vector, time in zip(fv, t):
        if((all(isinstance(x, (float, int)) for x in vector))):
          Append = False
          series = vector[::-1].copy()
          ctr = 0
          for i in range(2, len(series)):
              ctr +=1
              if(series[i]<series[i-1]):
                  if(series[i-1]<series[i-2]):
                      pass
                  else:
                      duration = ctr*4*60
                      ctr = 0
                      timestamp = time.astype('uint64')/1000000000
                      print(timestamp)
                      if(Append):
                        tosend.append([duration, timestamp - (numSamples - i)*4*60])
                      else:
                        Append = True
      tosend = np.array(tosend)             
      print(tosend)
    
    
    
      file_json = open(f'{folder}/features_alicante_salinity_EA001_36_level_duration.json', 'a')
      for j in range(tosend.shape[0]):
          if((all(isinstance(x, (float, int)) for x in tosend[j])) and (not np.isnan(tosend[j]).any())):
            fv_line = {"timestamp":int(tosend[j][1]), "ftr_vector":[tosend[j][0]]}
            file_json.write((json.dumps(fv_line) + '\n' ))
  
      file_json.close()
      
      
      for j in range(tosend.shape[0]):
          output = {"timestamp":int(tosend[j][1]), "ftr_vector":[tosend[j][0]]}
          output_topic = "features_alicante_salinity_EA001_36_level_duration"
          # Start Kafka producer
          
          if((all(isinstance(x, (float, int)) for x in tosend[j]))):
            future = producer.send(output_topic, output)
  
          try:
              record_metadata = future.get(timeout=10)
          except Exception as e:
              print('Producer error: ' + str(e))  



#Do batch fusion once per day
schedule.every(320).minutes.do(RunBatchFusionOnce)
print(schedule.get_jobs())
now = datetime.datetime.now()

current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)

RunBatchFusionOnce()
while True:
    schedule.run_pending()
    time.sleep(1)
    
    
    
    
    
"""   
"""