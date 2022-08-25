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

        # folders for storing features and config data
        features_folder = 'features_data'
        config_folder = 'config_data'

        config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:00:00")

        # reading last generated feature vector for obtaining last successful timestamp
        file_json = open(f'{features_folder}/features_alicante_{location}_forecasting_w.json', 'r')
        lines = file_json.readlines()
        last_line = lines[-1]
        # adding 30 minutes; why?
        tss = int(json.loads(last_line)['timestamp']/1000 + 30 * 60)
        config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00")

        # writing back the config file
        # TODO: possible bug - we should write this down only if output is successfull
        file_json = open(f'{config_folder}/alicante_{location}_forecasting_w_config.json', 'w')
        file_json.write(json.dumps(config, indent=4, sort_keys=True) )
        file_json.close()

        # initiate the batch fusion
        sf2 = batchFusion(config)

        # get outputs if possible
        update_outputs = True
        try:
            fv, t = sf2.buildFeatureVectors()
        except Exception as e:
            LOGGER.error('Feature vector generation failed %s', str(e))
            update_outputs = False

        # if feature vector was successfully generated, append the data into the file
        # and send it to Kafka
        if (update_outputs):

            consumption_tosend = []
            # transverse through all the feature vectors
            for i in range(len(t)):
                # extract flow profile
                flow = fv[i, :24]
                # doubling weather vector, duplicating every second value
                weather = fv[i, 24:]
                # make weather vector - double sized
                weather_ext = np.zeros(len(weather)*2)
                # fill in even places
                weather_ext[::2] = weather
                # fill in odd places
                weather_ext[1::2] = weather
                # build final vector
                vec = np.concatenate([flow, weather_ext])
                consumption_tosend.append(vec)
                last_values = vec[23::24]

                # TODO: take care about potential missing data imputation in the profiles
                # if feature vectors contain some NaNs then we do not send the feature vector
                if (not pd.isna(last_values).any()):
                    # generating timestamp and timestamp in readable form
                    ts = int(t[i].astype('uint64')/1000000)
                    ts_string = datetime.datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%dT%H:%M:%S")

                    # generate outputs
                    output = {"timestamp": ts, "ftr_vector": list(consumption_tosend[i])}
                    output_topic = f'features_alicante_{location}_forecasting_w'

                    # TODO: What is this?
                    #   data is uploaded at different times - this ensures that FV's won't
                    #   be sent if data hasn't been uploaded for one or more of the sensors

                    # append the data in the file
                    with open(f'{features_folder}/features_alicante_{location}_forecasting_w.json', 'a') as file_json:
                        file_json.write((json.dumps(output) + '\n' ))
                        file_json.close()

                    # send to Kafka and check success of the result
                    future = producer.send(output_topic, output)
                    try:
                        record_metadata = future.get(timeout = 10)
                        LOGGER.info("[%s] Feature vector sent to topic: %s", ts_string, output_topic)
                    except Exception as e:
                        LOGGER.exception('Producer error: ' + str(e))
                else:
                    LOGGER.info("[%s] Feature vector contains NaN or non-int/float: %s: %s", ts_string, output_topic, json.dumps(output))

# MAIN part of the program -------------------------------

# create hourly scheduler
schedule.every().hour.do(RunBatchFusionOnce)
RunBatchFusionOnce()
LOGGER.info('Component started successfully.')

# checking scheduler
while True:
    schedule.run_pending()
    time.sleep(1)