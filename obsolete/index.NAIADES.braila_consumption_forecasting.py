# OBSOLETE - THIS COULD NEVER BE WORKING!!!
# DESCRIPTION:
# Data fusion for Braila consumption use case.

# includes
import pandas as pd
import numpy as np
import json
import copy
import time
import datetime
import schedule
import logging

from kafka import KafkaProducer

# project-based includes
from src.fusion.stream_fusion import streamFusion, batchFusion

# logger initialization
LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", level=logging.INFO)

# import secrets
with open("secrets.json", "r") as jsonfile:
    secrets = json.load(jsonfile)
    print(secrets)

# connecting to Kafka; TODO - put this into a config file
producer = KafkaProducer(bootstrap_servers=secrets["bootstrap_servers"], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# generating fusions structure for all the devices
LOGGER.info("Starting building configurations for fusion.")

# -------------------------------------------------------------
# CONFIG PART
# -------------------------------------------------------------

# definition of devices
measurements_analog = [
    # 'flow211106H360',
    'flow211206H360',
    'flow211306H360',
    'flow318505H498'
]

# template for a particular device
template = {
    "aggregate": "mean",
    "measurement": "braila",
    "fields": ["totalizer2"],
    "tags": {None: None},
    "window": "1h",
    "when": "-0h"
}

fusion = [] # list of data fusions

# iterate through the devices
for m in measurements_analog:
    template['measurement'] = m

    # iterate from zero to 12h in the past
    for i in range(13):

        # add totalizer1
        temp = copy.deepcopy(template)
        temp['fields'] = ["totalizer1"]
        temp['when'] = f'-{i}h'
        fusion.append(temp)

        # add totalizer2
        temp = copy.deepcopy(template)
        temp['fields'] = ["totalizer2"]
        temp['when'] = f'-{i}h'
        fusion.append(temp)

# -------------------------------------------------------------
# Function definition part
# -------------------------------------------------------------

def RunBatchFusionOnce():
    """Create batch fusion for current nodes"""

    # config template for InfluxDB
    config = {
        "token": secrets["influx_token"],
        "url": "http://localhost:8086",
        "organisation": "naiades",
        "bucket": "braila",
        "startTime": secrets["start_time"],
        "stopTime": secrets["stop_time"],
        "every": "15m",
        "fusion": fusion
    }

    features_folder = 'features_data'

    config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:00:00")
    #config['startTime'] = datetime.datetime.utcfromtimestamp((today - datetime.datetime(1970, 1, 2 + start)).total_seconds()).strftime("%Y-%m-%dT%H:00:00")

    print(config['stopTime'] )

    file_json = open(f'{features_folder}/features_braila_consumption_forecasting.json', 'r')

    lines = file_json.readlines()
    last_line = lines[-1]
    tss = int(json.loads(last_line)['timestamp']/1000 + 60*60)

    #print(last_line)
    #print(tss)
    #print(datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00"))

    config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00")

    print(config['startTime'])

    file_json = open(f'braila_consumption_forecasting_config.json', 'w')
    file_json.write(json.dumps(config, indent=4, sort_keys=True) )
    file_json.close()

    #sf2 = batchFusion(config)
    sf2 = batchFusion(config)


    update_outputs = True
    try:
        fv, t = sf2.build_feature_vectors()
    except:
        print('Feature vector generation failed')
        update_outputs = False

    if(update_outputs):


      consumption_tosend = []
      for i in range(len(t)):
        #print(fv[i,:])
        if(True):
          sensor1 = fv[i, :26]
          sensor2 = fv[i, 26:2*26]
          sensor3 = fv[i, 2*26:3*26]
          sensor4 = fv[i, 3*26:4*26]

          vec = []
          for j in range(12):
              sum = 0

              if(np.isnan(np.float64(sensor1[2*j] - sensor1[2*j + 2]))):
                sum += 0
              else:
                sum+= sensor1[2*j] - sensor1[2*j + 2]
              if(np.isnan(np.float64(sensor1[2*j + 1] - sensor1[2*j + 3]))):
                sum -= 0
              else:
                sum -= sensor1[2*j + 1] - sensor1[2*j + 3]

              if(np.isnan(np.float64(sensor1[2*j] - sensor1[2*j + 2]))):
                sum += 0
              else:
                sum+= sensor2[2*j] - sensor2[2*j + 2]
              if(np.isnan(np.float64(sensor2[2*j + 1] - sensor2[2*j + 3]))):
                sum -= 0
              else:
                sum -= sensor2[2*j + 1] - sensor2[2*j + 3]

              if(np.isnan(np.float64(sensor3[2*j] - sensor3[2*j + 2]))):
                sum += 0
              else:
                sum+= sensor3[2*j] - sensor3[2*j + 2]
              if(np.isnan(np.float64(sensor3[2*j + 1] - sensor3[2*j + 3]))):
                sum -= 0
              else:
                sum -= sensor3[2*j + 1] - sensor3[2*j + 3]

              if(np.isnan(np.float64(sensor4[2*j] - sensor4[2*j + 2]))):
                sum += 0
              else:
                sum+= sensor4[2*j] - sensor4[2*j + 2]
              if(np.isnan(np.float64(sensor4[2*j + 1] - sensor4[2*j + 3]))):
                sum -= 0
              else:
                sum -= sensor4[2*j + 1] - sensor4[2*j + 3]

              vec.append(sum)
          print(vec)
          consumption_tosend.append(vec)

      print(t.shape)
      print(len(consumption_tosend))

      for j in range(t.shape[0]):
          if(not np.isnan(consumption_tosend[j]).any()):
            fv_line = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(consumption_tosend[j])}

            #data is uploaded at different times - this ensures that FV's won't be sent if data hasn't been uploaded for one or more of the sensors
            with open(f'{features_folder}/features_braila_consumption_forecasting.json', 'a') as file_json:
              file_json.write((json.dumps(fv_line) + '\n' ))


      file_json.close()

      for j in range(t.shape[0]):
          if(not np.isnan(consumption_tosend[j]).any()):
            output = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(consumption_tosend[j])}
            output_topic = "features_braila_consumption_forecasting"

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