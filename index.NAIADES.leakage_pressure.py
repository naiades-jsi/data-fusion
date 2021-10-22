from src.fusion.stream_fusion import streamFusion, bachFusion

import pandas as pd
import numpy as np
import json
import copy
import time
import datetime
import schedule

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))

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

#start = 14 #1
stop = 9   #0

template = {
            "aggregate":"mean",
            "measurement":"braila",
            "fields":["analog_input2"],
            "tags":{None: None},
            "window":"1h",
            "when":"-1h"
            }
fusion = []
for m in measurements_analog:
    template['measurement'] = m
    for i in range(24):
        temp = copy.deepcopy(template)
        temp['when'] = f'-{i}h'
        fusion.append(temp)

template['fields'] = ['value']
for m in measurements_presure:
    template['measurement'] = m

    for i in range(24):
        temp = copy.deepcopy(template)
        temp['when'] = f'-{i}h'
        fusion.append(temp)

#while (True):
wait = True
once = True

def RunBatchFusionOnce():
    today = datetime.datetime.today()

    #if wait:
    #    if today.hour < 20:
    #        future = datetime.datetime(today.year, today.month, today.day, 20, 0)
    #    else:
    #        future = datetime.datetime(today.year, today.month, today.day + 1, 20, 0)
    #
    #    print('Sleep:', (future - today).total_seconds())
    #    time.sleep((future - today).total_seconds())

    config = {
        "token":"k_TK7JanSGbx9k7QClaPjarlhJSsh8oApCyQrs9GqfsyO3-GIDf_tJ79ckwrcA-K536Gvz8bxQhMXKuKYjDsgw==",
        "url": "http://localhost:8086",
        "organisation": "naiades",
        "bucket": "braila",
        "startTime":"2021-07-07T00:00:00",
        "stopTime":"2021-07-13T00:00:00",
        "every":"1h",
        "fusion": fusion
    }

    #print(json.dumps(config, indent=4, sort_keys=True))

    today = datetime.datetime.today()
    folder = 'features_data'

    config['stopTime'] = datetime.datetime.utcfromtimestamp((today - datetime.datetime(1970, 1, 1 + stop)).total_seconds()).strftime("%Y-%m-%dT%H:00:00")
    #config['startTime'] = datetime.datetime.utcfromtimestamp((today - datetime.datetime(1970, 1, 2 + start)).total_seconds()).strftime("%Y-%m-%dT%H:00:00")
    
    print(config['stopTime'] )

    file_json = open(f'{folder}/features_pressure.json', 'r')

    lines = file_json.readlines()
    last_line = lines[-1]
    tss = int(json.loads(last_line)['timestamp']/1000 + 60*60)
    
    #print(last_line)
    #print(tss)
    #print(datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00"))

    config['startTime'] = datetime.datetime.utcfromtimestamp(tss).strftime("%Y-%m-%dT%H:00:00")

    print(config['startTime'])

    file_json = open(f'config.json', 'w')
    file_json.write(json.dumps(config, indent=4, sort_keys=True) )
    file_json.close()

    #sf2 = bachFusion(config)
    sf2 = bachFusion(config)


    fv, t = sf2.buildFeatureVectors()

    # firs sensor missing therefore zeros for now
    zero = np.zeros([fv.shape[0], 24])

    fv = np.concatenate((zero, fv), axis=1)

    

    # use formula to transform TODO add in sistem
    fv[:, 24: 24 * 4] = (fv[:,24: 24 * 4] - 0.6) * 4 * 10.197


    file_json = open(f'{folder}/features_pressure.json', 'a')
    for j in range(t.shape[0]):
        fv_line = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}
        file_json.write((json.dumps(fv_line) + '\n' ))

    file_json.close()


    for j in range(t.shape[0]):
        output = {"timestamp":int(t[j].astype('uint64')/1000000), "ftr_vector":list(fv[j])}
        output_topic = "features_braila_leakage_detection"
        # Start Kafka producer
        
        future = producer.send(output_topic, output)

        try:
            record_metadata = future.get(timeout=10)
        except Exception as e:
            print('Producer error: ' + str(e))

#Do batch fusion once per day
schedule.every().day.at("09:00").do(RunBatchFusionOnce)
print(schedule.get_jobs())
now = datetime.datetime.now()

current_time = now.strftime("%H:%M:%S")
print("Current Time =", current_time)
while True:
    
    schedule.run_pending()
    time.sleep(1)