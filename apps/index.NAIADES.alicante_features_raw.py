"""Module for data fusion for Alicante (NAIADES).

This module creates feature vectors from InfluxDB data for ... __TODO__.
"""

# imports
from src.fusion.stream_fusion import streamFusion, batchFusion

import pandas as pd
import numpy as np
import json
import copy
import time
import datetime
import schedule

from kafka import KafkaProducer

# constants -- to be moved to configuration structure (TODO)
config_folder = "config" # TODO: should this be renamed to "current_config" or similar?
features_folder = 'features_data'

# initializing kafka producer
producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# building fusion configs
measurements_conductivity = [
    'salinity_EA002_26_conductivity',
    'salinity_EA003_36_conductivity',
    'salinity_EA007_36_conductivity',
    'salinity_EA008_36_conductivity'
]

template = {
    "aggregate":"mean",
    "measurement":"alicante",
    "fields":["value"],
    "tags":{None: None},
    "window":"1m",
    "when":"-0h"
}

fusions = {} # TODO - should this be fusions = [], because it is an array, not a list?

for m in measurements_conductivity:
    fusion = []
    template['measurement'] = m
    temp = copy.deepcopy(template)
    fusion.append(temp)
    fusions[m] = copy.deepcopy(fusion)

def RunBatchFusionOnce():
    """Runs batch fusion for Alicante once.

    Docs needed ... __TODO___
    """

    # run data fusion for each of the locations
    for location in measurements_conductivity:
        today = datetime.datetime.today()

        # config template -- TODO: this should be moved to an outside static config structure (as JSON)
        config = {
            "token":"k_TK7JanSGbx9k7QClaPjarlhJSsh8oApCyQrs9GqfsyO3-GIDf_tJ79ckwrcA-K536Gvz8bxQhMXKuKYjDsgw==",
            "url": "http://localhost:8086",
            "organisation": "naiades",
            "bucket": "alicante",
            "startTime":"2021-07-07T00:00:00",
            "stopTime":"2021-07-13T00:00:00",
            "every":"1m",
            "fusion": fusions[location]
        }

        # feature vectors should be generated up to the last timestamp
        today = datetime.datetime.today()
        config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:00")

        # try determining the last time, a feature vector was generated for
        # and set it as a start time
        with open(f'{features_folder}/features_alicante_{location}_raw.json', 'w+') as file_json:
            try:
                lines = file_json.readlines()
                last_line = lines[-1]
                tss = int(json.loads(last_line)['timestamp']/1000 + 60*60)
            except:
                tss = 1649000000

        config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:%M:00")

        # write current config for data fusion into JSON file
        file_json = open(f'{config_folder}/alicante_salinity_{location}_raw_config.json', 'w')
        file_json.write(json.dumps(config, indent=4, sort_keys=True))
        file_json.close()

        # initialize batch fusion
        sf2 = batchFusion(config)

        # by default we expect that new data will be generated and that the
        # outputs (Kafka and files) should be updated
        update_outputs = True
        try:
            # building feature vectors
            fv, t = sf2.buildFeatureVectors()
        except:
            print('Feature vector generation failed.')
            # TODO: print error too
            update_outputs = False

        # writing to a file
        if (update_outputs):
            with open(f'{features_folder}/features_alicante_{location}_raw.json', 'a+') as file_json:
                # go through all generated feature vectors
                for j in range(t.shape[0]):
                    # generate correct format of a feature vector
                    fv_line = {
                        "timestamp": int(t[j].astype('uint64')/1000000),
                        "ftr_vector": list(fv[j])
                    }

                    # write a feature vector into the log file only if all the features are
                    # in numeric format
                    if ((all(isinstance(x, (float, int)) for x in fv[j])) and (not np.isnan(fv[j]).any())):
                        file_json.write((json.dumps(fv_line) + '\n'))

                file_json.close() # TODO: is this really needed, does not a file close by itself after "with" is finished

        # writing to kafka
        # TODO: all the structures and checks in this and previous block are the same
        #       so these blocks should be joined together
        for j in range(t.shape[0]):
            # create output structure for kafka
            output = {
                "timestamp":int(t[j].astype('uint64')/1000000),
                "ftr_vector":list(fv[j])
            }
            output_topic = f"features_alicante_{location}"

            # start kafka producer and send the data
            if((all(isinstance(x, (float, int)) for x in fv[j])) and (not np.isnan(fv[j]).any())):
                future = producer.send(output_topic, output)

            # check sending to kafka
            try:
                record_metadata = future.get(timeout=10)
            except Exception as e:
                print('Producer error: ' + str(e))

# schedule batch fusion once per day (TODO: this looks like once per hour)
schedule.every().hour.do(RunBatchFusionOnce)
print(schedule.get_jobs())
now = datetime.datetime.now()

current_time = now.strftime("%H:%M:%S")
print("Current Time = ", current_time)

# run batch once in the beginning and then schedule it accordingly
RunBatchFusionOnce()
while True:
    schedule.run_pending()
    time.sleep(1)