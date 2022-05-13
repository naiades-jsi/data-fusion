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


template = {
            "aggregate":"mean",
            "measurement":"salinity_EA001_36_level",
            "fields":["value"],
            "tags":{None: None},
            "window":"1m",
            "when":"-0h"
            }

fusion = []



# Fusion contains 5m mean of level value

temp = copy.deepcopy(template)
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
        "every":"1m",
        "fusion": fusion
    }


    today = datetime.datetime.today()
    folder = 'features_data'

    config['stopTime'] =datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    print(config['stopTime'] )

    file_json = open(f'{folder}/features_alicante_salinity_EA001_36_level.json', 'r')

    lines = file_json.readlines()
    last_line = lines[-1]
    tss = int(json.loads(last_line)['timestamp']/1000 + 5*60)


    config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:%M:%S")

    print(config['startTime'])

    file_json = open(f'config_alicante_salinity_EA001_36_level.json', 'w')
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
      file_json = open(f'{folder}/features_alicante_salinity_EA001_36_level.json', 'a')
      for j in range(t.shape[0]):
          fv_line = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}
          if((all(isinstance(x, (float, int)) for x in fv[j])) and (not np.isnan(fv[j]).any())):
            file_json.write((json.dumps(fv_line) + '\n' ))

      file_json.close()

      for j in range(t.shape[0]):
          output = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}
          output_topic = "features_alicante_salinity_EA001_36_level"
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