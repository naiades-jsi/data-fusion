# DESCRIPTION:
# Data fusion for Carouge use case; features based on SuisseMeteo data and not environmental
# station.

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
device_names = [
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
# transverse through devices
for idx in range(8):
    fusion = []

    # template for a particular device
    template = {
        "aggregate": "mean",
        "measurement": device_names[idx],
        "fields": ["value"],
        "tags": {None: None},
        "window": "1h",
        "when": "-0h"
    }

    # make copy of the template first (it's a valid)
    temp = copy.deepcopy(template)
    fusion.append(temp)

    # last value one day ago
    temp = copy.deepcopy(template)
    temp["when"] = "-1h"
    fusion.append(temp)

    # add current humidity
    temp = copy.deepcopy(template)
    temp["measurement"] = "weather_observed"
    temp["fields"] = ["humidity"]
    temp["window"] = "1d"
    temp["when"] = "-0h"
    fusion.append(temp)

    # add precipitation for last 3 days
    temp = copy.deepcopy(template)
    temp["measurement"] = "weather_observed"
    temp["fields"] = ["precipitation"]
    temp["window"] = "3d"
    temp["when"] = "-0h"
    fusion.append(temp)

    # add illuminance for last day
    temp = copy.deepcopy(template)
    temp["measurement"] = "weather_observed"
    temp["fields"] = ["illuminance"]
    temp["window"] = "1d"
    temp["when"] = "-0h"
    fusion.append(temp)

    # add current daily temperature average
    temp = copy.deepcopy(template)
    temp["measurement"] = "weather_observed"
    temp["fields"] = ["temperature"]
    temp["window"] = "1d"
    temp["when"] = "-0h"
    fusion.append(temp)

     # add temperature average for last day
    temp = copy.deepcopy(template)
    temp["measurement"] = "weather_observed"
    temp["fields"] = ["temperature"]
    temp["window"] = "1d"
    temp["when"] = "-1d"
    fusion.append(temp)

    # append the current feature vector config to the list of fusions
    Fusions.append(fusion)

# -------------------------------------------------------------
# Function definition part
# -------------------------------------------------------------

def RunBatchFusionOnce():
    """Create batch fusion for current nodes"""

    for idx in range(8):
        today = datetime.datetime.today()

        # config tempalte for InfluxDB
        # note to change the key, if needed
        config = {
            "token": secrets["influx_token"],
            "url": "http://localhost:8086",
            "organisation": "naiades",
            "bucket": "carouge",
            "startTime": "2021-06-01T00:00:00",
            "stopTime": "2022-06-30T00:00:00",
            "every": "1h",
            "fusion": Fusions[idx]
        }

        # folders for storing features and config data
        features_folder = 'features_data'
        config_folder = 'config_data'

        # updating stop time for batch fusion
        config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        # generating JSON filename
        try:
            file_json = open(f'{features_folder}/features_carouge_sm_' + str(idx + 1) + '.json', 'r')
            # reading features file
            lines = file_json.readlines()
            last_line = lines[-1]
            tss = int(json.loads(last_line)['timestamp']/1000 + 60*60)
            LOGGER.info("Obtaining last timestamp for features_carouge_%s: %d", str(idx + 1), tss)
            # setting start time
            config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:%M:%S")
        except Exception as e:
            LOGGER.info("Error reading features file - using default time (%s); probably does not exist: %s", config["startTime"], str(e))


        # dumping the actual config for this node into a file for further debugging
        file_json = open(f'{config_folder}/config_carouge_sm_' + str(idx + 1) + '.json', 'w+')
        file_json.write(json.dumps(config, indent=4, sort_keys=True) )
        file_json.close()

        # initiate the batch fusion
        sf2 = batchFusion(config)

        # get ouputs if possible
        update_outputs = True
        try:
            fv, t = sf2.buildFeatureVectors()
        except Exception as e:
            LOGGER.error('Feature vector generation failed: %s', str(e))
            update_outputs = False

        # if feature vector was successfully generated, append the data into the file
        if (update_outputs):
            file_json = open(f'{features_folder}/features_carouge_sm_' + str(idx + 1) + '.json', 'a+')
            # go through the vector of timestamps and save the data
            # into a file
            for j in range(t.shape[0]):
                # build the output JSON
                fv_line = { "timestamp": int(t[j].astype('uint64')/1000000), "ftr_vector": list(fv[j])}
                # only save feature vectors without NaNs and with ints and floats
                if ((all(isinstance(x, (float, int)) for x in fv[j])) and (not np.isnan(fv[j]).any())):
                    file_json.write((json.dumps(fv_line) + '\n' ))

            file_json.close()

            # go through the vector of timestamps and post the feature vectors in the
            # correct topics
            for j in range(t.shape[0]):
                # generating timestamp and timestamp in readable form
                ts = int(t[j].astype('uint64')/1000000)
                ts_string = datetime.datetime.utcfromtimestamp(ts / 1000).strftime("%Y-%m-%dT%H:%M:%S")

                output = { "timestamp": ts, "ftr_vector": list(fv[j])}
                output_topic = "features_carouge_flowerbed" + str(idx + 1)
                # send data to Kafka producer only if it does contain only floats and ints and no NaNs
                if((all(isinstance(x, (float, int)) for x in fv[j])) and (not np.isnan(fv[j]).any())):
                    #future = producer.send(output_topic, output)
                    try:
                        # record_metadata = future.get(timeout = 10)
                        LOGGER.info("[%s] Feature vector sent to topic: %s", ts_string, output_topic)
                    except Exception as e:
                        LOGGER.exception('Producer error: ' + str(e))
                else:
                    LOGGER.info("[%s] Feature vector contains NaN or non-int/float: %s: %s", ts_string, output_topic, json.dumps(output))

# -------------------------------------------------------------
# MAIN part of the fusion script
# -------------------------------------------------------------

# create scheduler
LOGGER.info("Starting scheduler")
schedule.every().hour.do(RunBatchFusionOnce)
RunBatchFusionOnce()

# checking scheduler (TODO: is this the correct way to do it)
# while True:
#    schedule.run_pending()
#    time.sleep(1)