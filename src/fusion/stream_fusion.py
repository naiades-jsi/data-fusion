from .agregates.agregate_query import AgregateQuery
from .agregates.agregate_py import AgregatePy

from .time_parser import TimeParser

import numpy as np
import pandas as pd
import logging

# logger initialization
LOGGER = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", level=logging.INFO)

from kafka import KafkaConsumer

class StreamFusion():
    def __init__(self, config):
        # TODO from config detemine nodes used for fussion
        self.config = config
        self.token = config["token"]
        self.url = config["url"]
        self.organisation = config["organisation"]
        self.bucket = config["bucket"]
        self.fusion = config["fusion"]

        self.agregate = AgregateQuery(self.token, self.url, self.organisation, self.bucket)

    def fuildFeatuureVectorKafka(self, topics, bootstrap_server):
        consumer = KafkaConsumer(bootstrap_servers=bootstrap_server)
        consumer.subscribe(topics)

        for msg in consumer:
            self.buildFeatureVector()

    def buildFeatureVector(self):
        feature_vector = []

        time_parser = TimeParser()

        for feature in self.fusion:
            aggregate = feature["aggregate"]
            measurement = feature["measurement"]
            fields = feature["fields"]
            tags = feature["tags"]
            window = feature["window"]
            when = feature["when"]
            try:
                self.bucket = feature['bucket']
            except:
                pass

            offset_time = time_parser.parseToInt(when)
            when = time_parser.parseToMS(offset_time)

            window_time = time_parser.parseToInt(window)
            window = time_parser.parseToMS(window_time)

            start_time = str(offset_time - window_time) + 'ms'
            print(start_time, when)

            try:
                what = feature['what']
            except:
                what = 'influx'
                pass

            if what == 'python':
                feat = AgregatePy(self.token, self.url, self.organisation, self.bucket).agregate_time(
                    agr=aggregate,
                    every=window,
                    window=window,
                    start_time=start_time,
                    stop_time=when,
                    shift= '-' + when,
                    offset=offset_time,
                    measurement=measurement,
                    fields=fields,
                    tags=tags
                )
            else:
                feat = self.agregate.agregate_time(
                    agr=aggregate,
                    every=window,
                    window=window,
                    start_time=start_time,
                    stop_time=when,
                    shift = '-' + when,
                    offset=offset_time,
                    measurement=measurement,
                    fields=fields,
                    tags=tags
                )

            for f in fields:
                try:
                    if what == 'python':
                        feature_vector.append( feat["_value"])
                    else:
                        feature_vector.append( feat["_value"][feat["_field"] == f].iloc[0] )
                except Exception as e:
                    LOGGER.error('Missing value')
                    feature_vector.append(None)

                # print(feat["_time"], '\n')

        return feature_vector, np.datetime64('now')

    def save(self):
        pass