from src.fusion.stream_fusion import streamFusion, batchFusion

import pandas as pd
import numpy as np
import json
import copy
import time
import datetime
import schedule
import logging

from kafka import KafkaProducer

# logger initialization
LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", level=logging.INFO)

# import secrets
with open("secrets.json", "r") as jsonfile:
    secrets = json.load(jsonfile)
    print(secrets)

# starting Kafka producer
producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# definition of sensors; flow and pressure
measurements_analog = [
    #'flow211106H360',
    'flow211206H360',
    'flow211306H360',
    'flow318505H498'
]

measurements_presure = [
    'pressure5770',
    'pressure5771',
    'pressure5772',
    'pressure5773'
]

# template for flow
template = {
    "aggregate": "mean",
    "measurement": "braila",
    "fields": ["analog_input2"],
    "tags": {None: None},
    "window": "2h",
    "when": "-1h"
}

# list of data fusions
fusion = []

# iterate through flow sensors
for m in measurements_analog:
    template['measurement'] = m
    for i in range(1):
        temp = copy.deepcopy(template)
        # make an exception for new sensor
        if(m == 'flow318505H498'):
            temp['window'] = '4h'
        else:
            temp['window'] = '4h'
        temp['when'] = f'-{i}h'
        fusion.append(temp)

# update template for pressure
template['fields'] = ['value']
template['window'] = '4h'

# iterate through pressure sensors
for m in measurements_presure:
    template['measurement'] = m
    for i in range(1):
        temp = copy.deepcopy(template)
        temp['when'] = f'-{i}h'
        fusion.append(temp)

# batch fusion definition
def RunBatchFusionOnce():
    config = {
        "token": secrets["influx_token"],
        "url": "http://localhost:8086",
        "organisation": "naiades",
        "bucket": "braila",
        "startTime": secrets["start_time"],
        "stopTime": secrets["stop_time"],
        "every": "1h",
        "fusion": fusion
    }

    # folders for storing features and config data
    features_folder = 'features_data'
    config_folder = 'config_data'

    config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:00:00")

    with open(f'{features_folder}/leakage_pressure_updated.json', 'a+')as file_json:
        try:
            # updating start time from the last successfully obtained feature
            lines = file_json.readlines()
            last_line = lines[-1]
            tss = int(json.loads(last_line)['timestamp']/1000 + 60*60)
            startTime = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00")

            # use features start time only if it is later than in the config
            if startTime > config['startTime']:
                config['startTime'] = startTime
        except:
            LOGGER.info("Could not read the last time from features file")

    # saving config file
    file_json = open(f'{config_folder}/config_leakage_pressure_updated.json', 'w+')
    file_json.write(json.dumps(config, indent=4, sort_keys=True) )
    file_json.close()

    # initializing batch fusion
    sf2 = batchFusion(config)

    # main part
    update_outputs = True
    try:
        fv, t = sf2.build_feature_vectors()
    except:
        LOGGER.info('Feature vector generation failed')
        update_outputs = False

    if(update_outputs):

        # first sensor missing therefore zeros for now
        zero = np.zeros((fv.shape[0], 1))

        fv = np.concatenate((zero, fv), axis=1)

        # use formula to transform TODO add in system
        fv[:, 1:4] = (fv[:,1:4] - 0.6) * 4 * 10.197


        with open(f'{features_folder}/features_pressure_updated.json', 'a+') as file_json:
            for j in range(t.shape[0]):
                fv_line = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}

                #data is uploaded at different times - this ensures that FV's won't be sent if data hasn't been uploaded for one or more of the sensors
                last_values = []
                for n in range(7):
                    last_values.append(fv[j][n])
                last_values = np.array(last_values)

                if((all(isinstance(x, (float, int)) for x in last_values)) and (not np.isnan(last_values).any())):
                    file_json.write((json.dumps(fv_line) + '\n' ))
            file_json.close()


        for j in range(t.shape[0]):
            output = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}
            output_topic = "features_braila_leakage_detection_updated"

            last_values = []
            for n in range(7):
                last_values.append(fv[j][n])
            last_values = np.array(last_values)

            # sending to kafka if everything is allright
            if((all(isinstance(x, (float, int)) for x in last_values)) and (not np.isnan(last_values).any())):
                future = producer.send(output_topic, output)

                try:
                    record_metadata = future.get(timeout=10)
                    LOGGER.info("Feature vector successfully sent: %s", output_topic)
                except Exception as e:
                    LOGGER.error('Producer error: ' + str(e))
            else:
                LOGGER.error("Feature vector not successfully generated: %s", json.dumps(output))


# schedule batch once per day
schedule.every().day.at("09:00").do(RunBatchFusionOnce)
RunBatchFusionOnce()
LOGGER.info("Component started successfully")

while True:
    schedule.run_pending()
    time.sleep(1)