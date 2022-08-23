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

# starting Kafka producer
producer = KafkaProducer(bootstrap_servers=secrets["bootstrap_servers"], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# CONFIG generations ---------------------------------------------
LOGGER.info("Generating Alicante consumption configurations (NO weather)")

# Alicante locations definition for config generation
locations = [
    'alipark',
    'autobuses',
    'benalua',
    'diputacion',
    'mercado',
    'montaneta',
    'rambla'
]

# set of fusions
fusions = {}

# iterating through all the Alicante locations
for m in locations:
    fusion = []
    # general template for a feature
    template = {
        "aggregate": "mean",
        "measurement": "alicante",
        "fields": ["value"],
        "tags": {None: None},
        "window": "30m",
        "when": "-0h"
    }

    # adding flow features (12-hour profile with 30min resolution)
    template['measurement'] = f'{m}_flow'
    for i in range(24):
        temp = copy.deepcopy(template)
        temp['fields'] = ["value"]
        # 12-hour profile of with 30min resolution
        temp['when'] = f'{(-23 + i)*30}m'
        fusion.append(temp)

    # adding fusion to the set of all fusions
    fusions[m] = copy.deepcopy(fusion)

# FUNCTION definition --------------------------------------------

def RunBatchFusionOnce():
    # iterate through all the locations
    for location in locations:
        config = {
            "token": secrets["influx_token"],
            "url": "http://localhost:8086",
            "organisation": "naiades",
            "bucket": "alicante",
            "startTime": "2022-08-15T00:00:00",
            "stopTime": "2022-08-17T00:00:00",
            "every": "30m",
            "fusion": fusions[location]
        }


        # folders for storing features and config data
        features_folder = 'features_data'
        config_folder = 'config_data'
        config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:00:00")

        # reading last generating feature for obtaining last successful timestamp
        file_json = open(f'{features_folder}/features_alicante_{location}_flow_forecasting.json', 'r')
        lines = file_json.readlines()
        last_line = lines[-1]
        # adding 30 minutes; why?
        tss = int(json.loads(last_line)['timestamp']/1000 + 30*60)
        config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00")

        # writing back the config file
        # TODO: possible bug - we should write this down only if output is successfull
        file_json = open(f'{config_folder}/alicante_{location}_flow_forecasting_config.json', 'w')
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
            tosend = []
            for i in range(len(t)):
                Flow = fv[i, :24]

                vec = Flow.copy()
                tosend.append(vec)


            for j in range(t.shape[0]):
                if(not pd.isna(tosend[j][-1])):
                    fv_line = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(tosend[j])}

                    with open(f'{features_folder}/features_alicante_{location}_flow_forecasting.json', 'a') as file_json:
                        file_json.write((json.dumps(fv_line) + '\n' ))

                    file_json.close()

                    output = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(tosend[j])}
                    output_topic = f'features_alicante_{location}_flow_forecasting'

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