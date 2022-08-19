from src.fusion.stream_fusion import streamFusion, bachFusion

import pandas as pd
import numpy as np
import json
import copy
import time
import datetime
import schedule
from scipy.fft import fft

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))


template = {
            "aggregate":"mean",
            "measurement":"salinity_EA001_36_level",
            "fields":["value"],
            "tags":{None: None},
            "window":"2m",
            "when":"-0h"
            }
            
fusion = []

for i in range(100):
    temp = copy.deepcopy(template)
    temp['when'] = f'-{200 - 2*i}m'
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
        "every":"5m",
        "fusion": fusion
    }

    today = datetime.datetime.today()
    folder = 'features_data'

    config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:00:00")

    with open(f'{folder}/features_alicante_level_frequency.json', 'r')as file_json:
        try:
          lines = file_json.readlines()
          last_line = lines[-1]
          print(last_line)
          frequency = json.loads(last_line)['ftr_vector'][0]
          tss = int(json.loads(last_line)['timestamp']/1000 + 2*60)
        except:
          pass
    
    config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00")
    print(config['startTime'])
    print(config['stopTime'])

    file_json = open(f'alicante_level_frequency.json', 'w')
    file_json.write(json.dumps(config, indent=4, sort_keys=True) )
    file_json.close()

    sf2 = bachFusion(config)


    update_outputs = True
    try:
      fv, t = sf2.buildFeatureVectors()
      print(f'{len(fv)}/{len(t)} feature vectors generated', flush = True)
    except:
      print('Feature vector generation failed')
      update_outputs = False
      
    
      
    if(update_outputs):

      for j, vec in enumerate(fv):
        if(isinstance(frequency, (float, int))and (not np.isnan(frequency))):
          try:
            arr = np.array(vec)
            f = np.argmax(np.abs(fft(vec - 0.5*(np.max(vec) + np.min(vec)))[1:int(len(vec)/2)]+1))
            frequency = 0.90*frequency + 0.1*f
          except:
            frequency = None
    
    
          if((all(isinstance(x, (float, int)) for x in vec)) and (not np.isnan(vec).any())):
            with open(f'{folder}/features_alicante_level_frequency.json', 'a+') as file_json:
                fv_line = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list([frequency])}
                
                if(isinstance(frequency, (float, int))and (not np.isnan(frequency))):
                  file_json.write((json.dumps(fv_line) + '\n' ))
    
            output = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list([frequency])}
            output_topic = "features_alicante_level_frequency"
            
            future = producer.send(output_topic, output)
      
            try:
                  record_metadata = future.get(timeout=10)
            except Exception as e:
                  print('Producer error: ' + str(e))



#Do batch fusion once per hour
schedule.every().hour.do(RunBatchFusionOnce)


RunBatchFusionOnce()
while True:
    
    schedule.run_pending()
    time.sleep(1)