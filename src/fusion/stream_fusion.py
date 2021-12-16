from .agregates.agregate_query import AgregateQuery
from .agregates.agregate_py import AgregatePy

from .time_parser import TimeParser

import numpy as np
import pandas as pd

from kafka import KafkaConsumer

class streamFusion():
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
                except:
                    print('missing value')
                    feature_vector.append(None)

                print(feat["_time"], '\n')

        return feature_vector, np.datetime64('now')

    def save(self):
        pass

class bachFusion():
    def __init__(self, config):
        # TODO from config detemine nodes used for fussion
        self.config = config
        self.token = config["token"]
        self.url = config["url"]
        self.organisation = config["organisation"]
        self.bucket = config["bucket"]
        self.fusion = config["fusion"]

        self.startTime = config["startTime"]
        self.stopTime = config["stopTime"]

        self.every = config["every"]
        
        self.agregate = AgregateQuery(self.token, self.url, self.organisation, self.bucket)

    def buildFeatureVectors(self):
        feature_vector = []

        time_parser = TimeParser()

        for feature in self.fusion:
            aggregate = feature["aggregate"]
            measurement = feature["measurement"]
            fields = feature["fields"]
            tags = feature["tags"]
            window = feature["window"]
            try:
                when = feature["when"]
            except:
                when = 0

            try:
                self.bucket = feature['bucket']
            except:
                pass

            stop_time = self.stopTime
            if isinstance(stop_time, str):
                if len(stop_time) > 18:
                    stop_time = - time_parser.diffFromNow(stop_time)

            start_time = self.startTime
            if isinstance(start_time, str):
                if len(start_time) > 18:
                    start_time = - time_parser.diffFromNow(start_time)

            every = self.every

            when_time = time_parser.parseToInt(when)
            when = time_parser.parseToMS(when)

            offset_time = time_parser.parseToInt(stop_time)
            stop_time = time_parser.parseToMS(offset_time + when_time)

            window_time = time_parser.parseToInt(window)
            window = time_parser.parseToMS(window_time)

            every_time = time_parser.parseToInt(every)
            every = time_parser.parseToMS(every_time)

            start_time = time_parser.parseToInt(start_time) + when_time

            start_time = str(int((start_time - offset_time )/ every_time) * every_time - window_time) + 'ms'

            #Error here - cannot query empty range (if start_time < stop_time ?)
            feat = self.agregate.agregate_time(
                agr=aggregate,
                every=every,
                window=window,
                start_time=start_time,
                stop_time=stop_time,
                shift = '-' + when,
                offset=offset_time,
                measurement=measurement,
                fields=fields,
                tags=tags
            )

            feat = feat.drop_duplicates(subset=['_stop'], keep='first')
            feat = feat.drop_duplicates(subset=['_start'], keep='last')
            
            if (not(feat.empty)):
              if (feat['_time'].iloc[-1] - feat['_time'].iloc[-2]) < pd.Timedelta(1, unit='s'):
                  feat.drop(feat.tail(1).index,inplace=True)
              #print(feat['_time'])
            for f in fields:
                try:
                  feature_vector.append( feat["_value"][feat["_field"] == f].values )
                  times = feat["_time"][feat["_field"] == f].values
                except:
                  feature_vector.append([np.nan])
                  print('missing value')
                  
                  

        #fix unexual row lengths
                  
        row_lengths = []

        for row in feature_vector:
            row_lengths.append(len(row))
            
        
        max_length = max(row_lengths)
        
        for i in range(len(feature_vector)):
            while len(feature_vector[i]) < max_length:
                feature_vector[i] = np.concatenate([feature_vector[i],[np.nan]])    

        feature_vector = np.array(feature_vector)

        feature_vector = np.array(np.transpose(feature_vector))
        

        return feature_vector, times

    def save(self):
        pass
