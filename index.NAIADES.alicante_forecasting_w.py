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
LOGGER.info("Generating Alicante configurations")

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
        temp['when'] = f'-{(23-i)*30}m'
        fusion.append(temp)

    # fixing template for weather data (12-hour hourly profile)
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

    # adding fusion to the set of all fusions
    fusions[m] = copy.deepcopy(fusion)


# FUNCTION definition --------------------------------------------

def RunBatchFusionOnce():
    # iterate through all the locations
    for location in locations:
        # template for the batch fusion config
        config = {
            "token": secrets["influx_token"],
            "url": "http://localhost:8086",
            "organisation": "naiades",
            "bucket": "alicante",
            "startTime": "2022-08-15T00:00:00",
            "stopTime": "2022-08-16T00:00:00",
            "every": "1h",
            "fusion": fusions[location]
        }

        today = datetime.datetime.today()
        features_folder = 'features_data'
        config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:00:00")

        # reading last generating feature for obtaining last successful timestamp
        file_json = open(f'{features_folder}/features_alicante_{location}_forecasting_w.json', 'r')
        lines = file_json.readlines()
        last_line = lines[-1]
        tss = int(json.loads(last_line)['timestamp']/1000 + 30 * 60)

        config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00")
        file_json = open(f'alicante_{location}_forecasting_w_config.json', 'w')
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

# MAIN part of the program -------------------------------
# create hourly scheduler
schedule.every().hour.do(RunBatchFusionOnce)
RunBatchFusionOnce()
LOGGER.info('Component started successfully.')

# checking scheduler
while True:
    schedule.run_pending()
    time.sleep(1)