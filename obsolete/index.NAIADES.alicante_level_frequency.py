# includes
import pandas as pd
import numpy as np
import json
import copy
import time
import datetime
import logging
import schedule
from scipy.fft import fft
from kafka import KafkaProducer

# project-based includes
from src.fusion.stream_fusion import batchFusion

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

# CONFIG generations ---------------------------------------------
LOGGER.info("Generating Alicante level-frequency configurations")

# Alicante level sensors
template = {
    "aggregate": "mean",
    "measurement": "salinity_EA001_36_level",
    "fields": ["value"],
    "tags": {None: None},
    "window": "2m",
    "when": "-0h"
}

# set of fusions
fusion = []

# creating a time series of last 200 minutes with 2 minute resolution
for i in range(100):
    temp = copy.deepcopy(template)
    temp['when'] = f'-{200 - 2*i}m'
    fusion.append(temp)

# FUNCTION definition --------------------------------------------
def RunBatchFusionOnce():
    config = {
        "token": secrets["influx_token"],
        "url": "http://localhost:8086",
        "organisation": "naiades",
        "bucket": "alicante",
        "startTime": secrets["start_time"],
        "stopTime": secrets["stop_time"],
        "every":"5m",
        "fusion": fusion
    }

    today = datetime.datetime.today()
    folder = 'features_data'

    config['stopTime'] = datetime.datetime.now().strftime("%Y-%m-%dT%H:00:00")

    with open(f'{folder}/features_alicante_level_frequency.json', 'r')as file_json:
        try:
            lines = file_json.readlines()
            last_line = lines[-1]
            print(last_line)
            frequency = json.loads(last_line)['ftr_vector'][0]
            tss = int(json.loads(last_line)['timestamp']/1000 + 2*60)
        except:
            pass

    config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00")
    print(config['startTime'])
    print(config['stopTime'])

    file_json = open(f'alicante_level_frequency.json', 'w')
    file_json.write(json.dumps(config, indent=4, sort_keys=True) )
    file_json.close()

    sf2 = batchFusion(config)


    update_outputs = True
    try:
        fv, t = sf2.build_feature_vectors()
        print(f'{len(fv)}/{len(t)} feature vectors generated', flush = True)
    except:
        print('Feature vector generation failed')
        update_outputs = False

    if(update_outputs):

        for j, vec in enumerate(fv):
            if(isinstance(frequency, (float, int))and (not np.isnan(frequency))):
                try:
                    arr = np.array(vec)
                    f = np.argmax(np.abs(fft(vec - 0.5*(np.max(vec) + np.min(vec)))[1:int(len(vec)/2)]+1))
                    frequency = 0.90*frequency + 0.1*f
                except:
                    frequency = None


                if((all(isinstance(x, (float, int)) for x in vec)) and (not np.isnan(vec).any())):
                    with open(f'{folder}/features_alicante_level_frequency.json', 'a+') as file_json:
                        fv_line = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list([frequency])}

                        if(isinstance(frequency, (float, int))and (not np.isnan(frequency))):
                          file_json.write((json.dumps(fv_line) + '\n' ))

                    output = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list([frequency])}
                    output_topic = "features_alicante_level_frequency"

                    future = producer.send(output_topic, output)

                    try:
                        record_metadata = future.get(timeout=10)
                    except Exception as e:
                        print('Producer error: ' + str(e))

# do batch fusion once per hour
schedule.every().hour.do(RunBatchFusionOnce)
RunBatchFusionOnce()

while True:
    schedule.run_pending()
    time.sleep(1)