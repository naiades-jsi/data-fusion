# includes
import pandas as pd
import numpy as np
import json
import copy
import time
import datetime
import schedule

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
LOGGER.info("Generating Braila flow anomaly detecton configurations")

# Braila flow sensors
measurements_analog = [
    'flow211106H360',
    'flow211206H360',
    'flow211306H360',
    'flow318505H498'
]

# template for a single feature
template = {
    "aggregate": "mean",
    "measurement": "braila",
    "fields": ["flow_rate_value"],
    "tags": {None: None},
    "window": "5m",
    "when": "-0h"
}

# set of fusions
fusions = {}

# adding to fusions
for m in measurements_analog:
    fusion = []
    template['measurement'] = m
    temp = copy.deepcopy(template)
    temp['when'] = "-0h"
    fusion.append(temp)
    fusions[m] = copy.deepcopy(fusion)

# FUNCTION definition --------------------------------------------

def RunBatchFusionOnce():
    for location in measurements_analog:
        config = {
            "token": secrets["influx_token"],
            "url": "http://localhost:8086",
            "organisation": "naiades",
            "bucket": "braila",
            "startTime": secrets["start_time"],
            "stopTime": secrets["stop_time"],
            "every": "5m",
            "fusion": fusions[location]
        }

        # folders for storing features and config data
        features_folder = 'features_data'
        config_folder = 'config_data'
        config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:00:00")

        # reading last generating feature for obtaining last successful timestamp
        try:
            file_json = open(f'{features_folder}/features_braila_{location}_anomaly.json', 'r')
            lines = file_json.readlines()
            last_line = lines[-1]
            tss = int(json.loads(last_line)['timestamp']/1000 + 5*60)
            # only change start time if later than the one in the config
            startTime = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00")
            if startTime > config['startTime']:
                config['startTime'] = startTime
        except:
            LOGGER.info("No old features file was found (%s), keeping config time (%s).",
                f'{features_folder}/features_alicante_{location}_flow_forecasting.json',
                secrets["start_time"])

        # writing back the config file
        # TODO: possible bug - we should write this down only if output is successfull
        file_json = open(f'{config_folder}/braila_{location}_night_anomaly_config.json', 'w')
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

        if(update_outputs):

            for j in range(t.shape[0]):
                if(not np.isnan(fv[j]).any()):
                    fv_line = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}

                    #data is uploaded at different times - this ensures that FV's won't be sent if data hasn't been uploaded for one or more of the sensors
                    with open(f'{folder}/features_braila_{location}_anomaly.json', 'a') as file_json:
                        file_json.write((json.dumps(fv_line) + '\n' ))


            for j in range(t.shape[0]):
                if(not np.isnan(fv[j]).any()):
                    output = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}
                    output_topic = f'features_braila_{location}_anomaly'
                    future = producer.send(output_topic, output)

                    try:
                        record_metadata = future.get(timeout=10)
                    except Exception as e:
                        print('Producer error: ' + str(e))

#Do batch fusion once per hour
schedule.every().hour.do(RunBatchFusionOnce)


#print(schedule.get_jobs())
now = datetime.datetime.now()

current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)

RunBatchFusionOnce()
while True:

    schedule.run_pending()
    time.sleep(1)