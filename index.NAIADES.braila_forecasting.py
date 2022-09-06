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

# connecting to Kafka; TODO - put this into a config file
producer = KafkaProducer(bootstrap_servers=secrets["bootstrap_servers"], value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# generating fusions structure for all the devices
LOGGER.info("Starting building configurations for fusion.")

# -------------------------------------------------------------
# CONFIG PART
# -------------------------------------------------------------

# definition of devices
measurements_analog = [
    # 'flow211106H360', # this sensor is not working
    'flow211206H360',
    'flow211306H360',
    'flow318505H498'
]

# template for an aggregate
template = {
    "aggregate": "mean",
    "measurement": "braila",
    "fields": ["flow_rate_value"],
    "tags": {None: None},
    "window": "20m",
    "when": "-0h"
}

# list of fusions
fusions = []
for m in measurements_analog:
    # a single fusino
    fusion = []
    # build a time series from 35 - 0 hours
    for i in range(36):
        template['measurement'] = m
        temp = copy.deepcopy(template)
        temp['when'] = f"-{(35-i)*20}m"
        fusion.append(temp)
    fusions.append(fusion)

# -------------------------------------------------------------
# Function definition part
# -------------------------------------------------------------

def RunBatchFusionOnce():
    """Create batch fusion for current nodes"""

    # iterate through all the devices
    for location in measurements_analog:
        # template config for a fusion
        config = {
            "token": secrets["influx_token"],
            "url": "http://localhost:8086",
            "organisation": "naiades",
            "bucket": "braila",
            "startTime": secrets["start_time"],
            "stopTime": secrets["stop_time"],
            "every": "20m",
            "fusion": fusions[location]
        }

        # folders for storing features and config data
        features_folder = 'features_data'
        config_folder = 'config_data'

        # set stop time according to latest timestamp
        config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:00:00")

        # read last successfull timestamp from features file
        try:
            file_json = open(f'{features_folder}/features_braila_{location}_forecasting.json', 'r')
            lines = file_json.readlines()
            last_line = lines[-1]
            tss = int(json.loads(last_line)['timestamp']/1000 + 30*60)
            startTime = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00")

            # only use time from features file if bigger than the one from secrets
            if startTime > config['startTime']:
                config['startTime'] = startTime
        except Exception as e:
            LOGGER.info("Exception while reading features file: %s", str(e))
            LOGGER.info("Using default timestamp: %s", config["startTime"])

        # save curent config
        file_json = open(f'{config_folder}/braila_{location}_forecasting_config.json', 'w+')
        file_json.write(json.dumps(config, indent=4, sort_keys=True) )
        file_json.close()

        # initiate the batch fusion
        sf2 = batchFusion(config)

        update_outputs = True
        try:
            fv, t = sf2.buildFeatureVectors()
        except Exception as e:
            LOGGER.error('Feature vector generation failed: %s', str(e))
            update_outputs = False

        # if feature vector was successfully generated, append the data into the file
        if (update_outputs):
            # iterate through vector of timestamps
            for j in range(t.shape[0]):

                # generating timestamp and timestamp in readable form
                ts = int(t[j].astype('uint64')/1000000)
                ts_string = datetime.datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%dT%H:%M:%S")

                output = {"timestamp": ts, "ftr_vector": list(fv[j])}
                output_topic = f'features_braila_{location}_forecasting'

                # only if one of the last five numbers in feature vectors is not NaN
                # TODO: why is this OK???
                if (not pd.isna(fv[j][-5:]).any()):
                    # save data to features file
                    with open(f'{features_folder}/features_braila_{location}_forecasting.json', 'a') as file_json:
                        file_json.write((json.dumps(output) + '\n' ))

                    # send data to kafka
                    future = producer.send(output_topic, output)

                    # check response from kafka
                    try:
                        record_metadata = future.get(timeout=10)
                    except Exception as e:
                        LOGGER.exception('Producer error: ' + str(e))
                else:
                    LOGGER.info("[%s] Feature vector contains NaN or non-int/float: %s: %s", ts_string, output_topic, json.dumps(output))

# -------------------------------------------------------------
# MAIN part of the fusion script
# -------------------------------------------------------------

# create scheduler
LOGGER.info("Scheduling starting")
schedule.every().hour.do(RunBatchFusionOnce)
RunBatchFusionOnce()

# checking scheduler (TODO: is this the correct way to do it)
while True:
    schedule.run_pending()
    time.sleep(1)