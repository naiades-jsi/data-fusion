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
        temp['when'] = f'-{(23-i)*30}m'
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
            "startTime": secrets["start_time"],
            "stopTime": secrets["stop_time"],
            "every": "30m",
            "fusion": fusions[location]
        }

        # folders for storing features and config data
        features_folder = 'features_data'
        config_folder = 'config_data'
        config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:00:00")

        # reading last generating feature for obtaining last successful timestamp
        try:
            file_json = open(f'{features_folder}/features_alicante_{location}_flow_forecasting.json', 'r')
            lines = file_json.readlines()
            last_line = lines[-1]
            # adding 30 minutes; why?
            tss = int(json.loads(last_line)['timestamp']/1000 + 30*60)
            startTime = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00")

            if startTime > config['startTime']:
                config['startTime'] = startTime
        except Exception as e:
            LOGGER.info("No old features file was found (%s), keeping config time (%s).",
                f'{features_folder}/features_alicante_{location}_flow_forecasting.json',
                secrets["start_time"])

        # writing back the config file
        # TODO: possible bug - we should write this down only if output is successfull
        file_json = open(f'{config_folder}/alicante_{location}_flow_forecasting_config.json', 'w')
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
            tosend = []

            # TODO: this should be obsolete - copies itself
            for i in range(len(t)):
                Flow = fv[i, :24]
                vec = Flow.copy()
                tosend.append(vec)

            # create vector to send
            for j in range(t.shape[0]):
                # generating timestamp and timestamp in readable form
                ts = int(t[j].astype('uint64')/1000000)
                ts_string = datetime.datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%dT%H:%M:%S")

                # generate outputs
                output = {"timestamp": ts, "ftr_vector": list(tosend[j])}
                output_topic = f'features_alicante_{location}_flow_forecasting'

                # only if first and last element are ok
                if (not pd.isna(tosend[j][-1])) and (not pd.isna(tosend[j][0])):
                    # write feature vector to file
                    with open(f'{features_folder}/features_alicante_{location}_flow_forecasting.json', 'a') as file_json:
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
                    # count nans
                    number_of_nans = pd.isna(tosend[j]).sum()
                    LOGGER.info("[%s] Feature vector contains NaN or non-int/float: %s: %d", ts_string, output_topic, number_of_nans)


# MAIN part of the program -------------------------------

# create hourly scheduler
schedule.every(1).hour.at(":10").do(RunBatchFusionOnce)
RunBatchFusionOnce()

LOGGER.info('Component started successfully.')

# checking scheduler
while True:
    schedule.run_pending()
    time.sleep(1)