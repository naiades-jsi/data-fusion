# includes
from src.fusion.stream_fusion import streamFusion, batchFusion

import pandas as pd
import numpy as np
import json
import copy
import time
import datetime
import schedule

from kafka import KafkaProducer

import logging

LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", level=logging.INFO)

producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))

Device_names = [
    "device_1f0d",  #1
    "device_1f08",  #2
    "device_1f10",  #3
    "device_1f06",  #4
    "device_1efd",  #5
    "device_1eff",  #6
    "device_1f02",  #7
    "device_1efe"   #8
]

Fusions = []
for idx in range(8):
    fusion = []

    template = {
        "aggregate":"mean",
        "measurement": Device_names[idx],
        "fields":["value"],
        "tags":{None: None},
        "window":"1h",
        "when":"-0h"
    }



    temp = copy.deepcopy(template)
    fusion.append(temp)

    temp = copy.deepcopy(template)
    temp["when"] = "-1h"
    fusion.append(temp)

    temp = copy.deepcopy(template)
    temp["measurement"] = "environmental_station"
    temp["fields"] = ["relativeHumidity"]
    temp["window"] = "1h"
    temp["when"] = "-0h"
    fusion.append(temp)

    temp = copy.deepcopy(template)
    temp["measurement"] = "environmental_station"
    temp["fields"] = ["soil"]
    temp["window"] = "1h"
    temp["when"] = "-0h"
    fusion.append(temp)
    temp["when"] = "-1h"
    fusion.append(temp)

    temp = copy.deepcopy(template)
    temp["measurement"] = "environmental_station"
    temp["fields"] = ["temperature"]
    temp["window"] = "1h"
    temp["when"] = "-0h"
    fusion.append(temp)

    temp = copy.deepcopy(template)
    temp["measurement"] = "environmental_station"
    temp["fields"] = ["temperature"]
    temp["window"] = "1h"
    temp["when"] = "-0h"
    fusion.append(temp)
    
    #temp = copy.deepcopy(template)
    #temp["measurement"] = 'flower_bed_' + str(idx + 1)
    #temp["fields"] = ['soilTemperature']
    
    #fusion.append(temp)
    
    temp = copy.deepcopy(template)
    Fusions.append(fusion)


def RunBatchFusionOnce():
    for idx in range(8):
      today = datetime.datetime.today()

      config = {
          "token":"k_TK7JanSGbx9k7QClaPjarlhJSsh8oApCyQrs9GqfsyO3-GIDf_tJ79ckwrcA-K536Gvz8bxQhMXKuKYjDsgw==",
          "url": "http://localhost:8086",
          "organisation": "naiades",
          "bucket": "carouge",
          "startTime":"2021-07-07T00:00:00",
          "stopTime":"2021-07-13T00:00:00",
          "every":"1h",
          "fusion": Fusions[idx]
      }

      today = datetime.datetime.today()
      folder = 'features_data'

      config['stopTime'] =datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

      file_json = open(f'{folder}/features_carouge_' + str(idx + 1) + '.json', 'r')

      lines = file_json.readlines()
      last_line = lines[-1]
      print(last_line)
      print(json.loads(last_line))
      print(idx)
      print(json.loads(last_line)['timestamp'])
      tss = int(json.loads(last_line)['timestamp']/1000 + 60*60)

      config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:%M:%S")

      #print(config['startTime'])
      #print(config['stopTime'] )

      file_json = open(f'config_carouge_' + str(idx + 1) + '.json', 'w')
      file_json.write(json.dumps(config, indent=4, sort_keys=True) )
      file_json.close()

      sf2 = batchFusion(config)

      update_outputs = True
      try:
          fv, t = sf2.buildFeatureVectors()
      except:
          print('Feature vector generation failed')
          update_outputs = False

      if(update_outputs):

          file_json = open(f'{folder}/features_carouge_' + str(idx + 1) + '.json', 'a')
          for j in range(t.shape[0]):
              fv_line = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}
              if((all(isinstance(x, (float, int)) for x in fv[j])) and (not np.isnan(fv[j]).any())):
                  file_json.write((json.dumps(fv_line) + '\n' ))

        file_json.close()


        for j in range(t.shape[0]):
            output = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}
            output_topic = "features_carouge_flowerbed" + str(idx + 1)
            # Start Kafka producer
            if((all(isinstance(x, (float, int)) for x in fv[j])) and (not np.isnan(fv[j]).any())):
                future = producer.send(output_topic, output)

                try:
                    record_metadata = future.get(timeout=10)
                except Exception as e:
                    print('Producer error: ' + str(e))



schedule.every().hour.do(RunBatchFusionOnce)
RunBatchFusionOnce()

now = datetime.datetime.now()
current_time = now.strftime("%H:%M:%S")
#print("Current Time =", current_time)

while True:
    schedule.run_pending()
    time.sleep(1)