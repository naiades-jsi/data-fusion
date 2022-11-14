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
from src.fusion.batch_fusion import BatchFusion

# logger initialization
LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", level=logging.INFO)

# import secrets
with open("secrets.json", "r") as jsonfile:
    secrets = json.load(jsonfile)

# starting Kafka producer
producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# CONFIG generations ---------------------------------------------
LOGGER.info("Generating Alicante salinity configurations")

# Alicante salinity sensors
measurements_conductivity = [
    'salinity_EA002_26_conductivity',
    'salinity_EA003_36_conductivity',
    'salinity_EA003_21_conductivity',
    'salinity_EA004_21_conductivity',
    'salinity_EA007_36_conductivity',
    'salinity_EA008_36_conductivity',
    'salinity_EA005_21_conductivity'
]

# set of fusions
fusions = {}

# feature template for InfluxDB
template = {
    "aggregate": "mean",
    "measurement": "alicante",
    "fields": ["value"],
    "tags": {None: None},
    "window": "10m",
    "when": "-0h"
}

# iterating through sensors
for m in measurements_conductivity:
    # exception for unstable sensor
    if (m == 'salinity_EA005_21_conductivity'):
        fusion = []
        template['measurement'] = m
        template["window"] = "30m"
        template["aggregate"] = "max"
        temp = copy.deepcopy(template)
        fusion.append(temp)
    else:
        fusion = []
        template['measurement'] = m
        template["window"] = "10m"
        template["aggregate"] = "mean"
        temp = copy.deepcopy(template)
        fusion.append(temp)

    # adding fusion to the set of all fusions
    fusions[m] = copy.deepcopy(fusion)


# FUNCTION definition --------------------------------------------

def RunBatchFusionOnce():
    # iterate through all the locations
    for location in measurements_conductivity:
        # template configuration for the batch fusion
        config = {
            "token": secrets["influx_token"],
            "url": "http://localhost:8086",
            "organisation": "naiades",
            "bucket": "alicante",
            "startTime": secrets["start_time"],
            "stopTime": secrets["stop_time"],
            "every": "10m",
            "fusion": fusions[location]
        }

        # folders for storing features and config data
        features_folder = 'features_data'
        config_folder = 'config_data'

        # reading generated feature vectors for obtaining last successful timestamp
        try:
            with open(f'{features_folder}/features_alicante_{location}_raw.json', 'r') as file_json:
                lines = file_json.readlines()
                last_line = lines[-1]
                if(location == 'salinity_EA005_21_conductivity'):
                    shift = 30 # in minutes
                else:
                    shift = 10
                tss = int(json.loads(last_line)['timestamp']/1000 + shift*60)
                config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:%M:00")
        except Exception as e:
            LOGGER.error("Exception: %s", str(e))
            LOGGER.info("Keeping original start timestamp: %s", config["startTime"])

        # setting up stop times
        config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:00")

        LOGGER.info("Setting start and stop times to: %s and %s", config['startTime'], config['stopTime'])

        # writing back the config file
        file_json = open(f'{config_folder}/alicante_{location}_raw_config.json', 'w')
        file_json.write(json.dumps(config, indent=4, sort_keys=True) )
        file_json.close()

        # initiate the batch fusion
        sf2 = BatchFusion(config)

        # get outputs if possible
        update_outputs = True
        try:
            fv, t = sf2.build_feature_vectors()
        except Exception as e:
            LOGGER.error('Feature vector generation failed: %s', str(e))
            update_outputs = False

        # if feature vector was successfully generated, append the data into the file
        # and send it to Kafka
        if (update_outputs):
            # write to features log file
            with open(f'{features_folder}/features_alicante_{location}_raw.json', 'a+') as file_json:
                # iterate through all the feature vectors
                for j in range(t.shape[0]):
                    # generating timestamp and timestamp in readable form
                    ts = int(t[j].astype('uint64')/1000000)
                    ts_string = datetime.datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%dT%H:%M:%S")

                    # generating ouput topic and feature vector
                    output_topic = f"features_alicante_{location}"
                    output = { "timestamp": ts, "ftr_vector": list(fv[j]) }

                    # are there NaNs in the feature vector?
                    if ((all(isinstance(x, (float, int)) for x in fv[j])) and (not np.isnan(fv[j]).any())):
                        # write to file
                        file_json.write((json.dumps(output) + '\n' ))

                        # send to Kafka and check the sucess of the producer
                        future = producer.send(output_topic, output)
                        try:
                            record_metadata = future.get(timeout = 10)
                            LOGGER.info("[%s] Feature vector sent to topic: %s", ts_string, output_topic)
                        except Exception as e:
                            print('Producer error: ' + str(e))
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