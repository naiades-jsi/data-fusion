from src.fusion.stream_fusion import streamFusion, batchFusion

import pandas as pd
import numpy as np
import json
import copy
import time
import datetime
import schedule

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))

locations = [
    'alipark',
    'autobuses',
    'benalua',
    'diputacion',
    'mercado',
    'montaneta',
    'rambla'
    ]

#start = 14 #1
stop = 9   #0

fusions = {}


for m in locations:
    fusion = []
    template = {
            "aggregate":"mean",
            "measurement":"alicante",
            "fields":["value"],
            "tags":{None: None},
            "window":"30m",
            "when":"-0h"
            }
    template['measurement'] = f'{m}_flow'
    for i in range(24):
        temp = copy.deepcopy(template)
        temp['fields'] = ["value"]
        temp['when'] = f'-{(23-i)*30}m'
        fusion.append(temp)


    template['measurement'] = 'weather_observed1'
    template['window'] = "1h"
            
    for i in range(12):
        temp = copy.deepcopy(template)
        temp['fields'] = ["pressure"]
        temp['when'] = f'-{(11-i)}h'
        fusion.append(temp)

        temp = copy.deepcopy(template)
        temp['fields'] = ["humidity"]
        temp['when'] = f'-{(11-i)}h'
        fusion.append(temp)

        temp = copy.deepcopy(template)
        temp['fields'] = ["temperature"]
        temp['when'] = f'-{(11-i)}h'
        fusion.append(temp)

        temp = copy.deepcopy(template)
        temp['fields'] = ["wind_bearing"]
        temp['when'] = f'-{(11-i)}h'
        fusion.append(temp)

        temp = copy.deepcopy(template)
        temp['fields'] = ["wind_speed"]
        temp['when'] = f'-{(11-i)}h'
        fusion.append(temp)
    fusions[m] = copy.deepcopy(fusion)





#while (True):
wait = True
once = True

def RunBatchFusionOnce():
    for location in locations:
      today = datetime.datetime.today()

      config = {
          "token":"k_TK7JanSGbx9k7QClaPjarlhJSsh8oApCyQrs9GqfsyO3-GIDf_tJ79ckwrcA-K536Gvz8bxQhMXKuKYjDsgw==",
          "url": "http://localhost:8086",
          "organisation": "naiades",
          "bucket": "alicante",
          "startTime":"2021-07-07T00:00:00",
          "stopTime":"2021-07-13T00:00:00",
          "every":"1h",
          "fusion": fusions[location]
      }

      #print(json.dumps(config, indent=4, sort_keys=True))

      today = datetime.datetime.today()
      folder = 'features_data'

      config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:00:00")
      #config['startTime'] = datetime.datetime.utcfromtimestamp((today - datetime.datetime(1970, 1, 2 + start)).total_seconds()).strftime("%Y-%m-%dT%H:00:00")

      #print(config['stopTime'] )

      file_json = open(f'{folder}/features_alicante_{location}_forecasting_w.json', 'r')

      lines = file_json.readlines()
      last_line = lines[-1]
      tss = int(json.loads(last_line)['timestamp']/1000 + 30*60)

      #print(last_line)
      #print(tss)
      #print(datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00"))

      config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00")

      #print(config['startTime'])

      file_json = open(f'alicante_{location}_forecasting_w_config.json', 'w')
      file_json.write(json.dumps(config, indent=4, sort_keys=True) )
      file_json.close()

      #sf2 = batchFusion(config)
      sf2 = batchFusion(config)


      update_outputs = True
      try:
        fv, t = sf2.buildFeatureVectors()
      except:
        print('Feature vector generation failed')
        update_outputs = False

      if(update_outputs):

        consumption_tosend = []
        for i in range(len(t)):
            
            Flow = fv[i, :24]
            Weather = fv[i, 24:]
            weather_ext = np.zeros(len(Weather)*2)
            weather_ext[::2] = Weather
            weather_ext[1::2] = Weather

            vec = np.concatenate([Flow, weather_ext])
            consumption_tosend.append(vec)
            last_values = vec[23::24]
          
            if(not pd.isna(last_values).any()):
                fv_line = {"timestamp":int(t[i].astype('uint64')/1000000), "ftr_vector":list(consumption_tosend[i])}
                
                #data is uploaded at different times - this ensures that FV's won't be sent if data hasn't been uploaded for one or more of the sensors
                with open(f'{folder}/features_alicante_{location}_forecasting_w.json', 'a') as file_json:
                  file_json.write((json.dumps(fv_line) + '\n' ))
                
                file_json.close()
                
                output = {"timestamp":int(t[i].astype('uint64')/1000000), "ftr_vector":list(consumption_tosend[i])}
                output_topic = f'features_alicante_{location}_forecasting_w'
      
                future = producer.send(output_topic, output)
        
                try:
                    record_metadata = future.get(timeout=10)
                except Exception as e:
                    print('Producer error: ' + str(e))
                      

#Do batch fusion once per hour


schedule.every().hour.do(RunBatchFusionOnce)

now = datetime.datetime.now()

current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)

RunBatchFusionOnce()
print('Component started successfully.')
while True:

    schedule.run_pending()
    time.sleep(1)