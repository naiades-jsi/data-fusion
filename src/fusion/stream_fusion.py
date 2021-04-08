from .agregates.agregate_query import AgregateQuery
from .agregates.agregate_py import AgregatePy

import numpy as np

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

    def buildFeatureVector(self):
        feature_vector = []

        for feature in self.fusion:
            aggregate = feature["aggregate"]
            measurement = feature["measurement"]
            fields = feature["fields"]
            tags = feature["tags"]
            window = feature["window"]
            when = feature["when"]

            offset_time = 0
            if isinstance(when, int):
                offset_time = int(when)
                when = str(when) + 'ms'
            elif (when[-2:-1] == 'ms'):
                offset_time = int(when[0:-1])
            elif (when[-1] == 's'):
                offset_time = int(when[0:-1]) * 1000
            elif (when[-1] == 'm'):
                offset_time = int(when[0:-1]) * 1000 * 60
            elif (when[-1] == 'h'):
                offset_time = int(when[0:-1]) * 1000 * 60 * 60
            elif (when[-1] == 'd'):
                offset_time = int(when[0:-1]) * 1000 * 60 * 60 * 24
            elif (when[-1] == 'w'):
                offset_time = int(when[0:-1]) * 1000 * 60 * 60 * 24 * 7

            window_time = 0
            if isinstance(window, int):
                window_time = int(window)
                window = str(window) + 'ms'
            elif (window[-2:-1] == 'ms'):
                window_time = int(window[0:-1])
            elif (window[-1] == 's'):
                window_time = int(window[0:-1]) * 1000
            elif (window[-1] == 'm'):
                window_time = int(window[0:-1]) * 1000 * 60
            elif (window[-1] == 'h'):
                window_time = int(window[0:-1]) * 1000 * 60 * 60
            elif (window[-1] == 'd'):
                window_time = int(window[0:-1]) * 1000 * 60 * 60 * 24
            elif (window[-1] == 'w'):
                window_time = int(window[0:-1]) * 1000 * 60 * 60 * 24 * 7

            start_time = str(offset_time - window_time) + 'ms'

            feat = self.agregate.agregate_time(
                agr=aggregate,
                every=window,
                window=window,
                start_time=start_time,
                stop_time=when,
                offset=offset_time,
                measurement=measurement,
                fields=fields,
                tags=tags
            )

            for f in fields:
                try:
                    feature_vector.append( feat["_value"][feat["_field"] == f].iloc[0] )
                except:
                    print('python aggregate')
                    feature_vector.append(feat)

        return feature_vector, feat["_time"].iloc[0]

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

        for feature in self.fusion:
            aggregate = feature["aggregate"]
            measurement = feature["measurement"]
            fields = feature["fields"]
            tags = feature["tags"]
            window = feature["window"]
            stop_time = self.stopTime
            every = self.every

            offset_time = 0
            if isinstance(stop_time, int):
                offset_time = int(stop_time)
                stop_time = str(stop_time) + 'ms'
            elif (stop_time[-2:-1] == 'ms'):
                offset_time = int(stop_time[0:-1])
            elif (stop_time[-1] == 's'):
                offset_time = int(stop_time[0:-1]) * 1000
            elif (stop_time[-1] == 'm'):
                offset_time = int(stop_time[0:-1]) * 1000 * 60
            elif (stop_time[-1] == 'h'):
                offset_time = int(stop_time[0:-1]) * 1000 * 60 * 60
            elif (stop_time[-1] == 'd'):
                offset_time = int(stop_time[0:-1]) * 1000 * 60 * 60 * 24
            elif (stop_time[-1] == 'w'):
                offset_time = int(stop_time[0:-1]) * 1000 * 60 * 60 * 24 * 7

            window_time = 0
            if isinstance(window, int):
                window_time = int(window)
                window = str(window) + 'ms'
            elif (window[-2:-1] == 'ms'):
                window_time = int(window[0:-1])
            elif (window[-1] == 's'):
                window_time = int(window[0:-1]) * 1000
            elif (window[-1] == 'm'):
                window_time = int(window[0:-1]) * 1000 * 60
            elif (window[-1] == 'h'):
                window_time = int(window[0:-1]) * 1000 * 60 * 60
            elif (window[-1] == 'd'):
                window_time = int(window[0:-1]) * 1000 * 60 * 60 * 24
            elif (window[-1] == 'w'):
                window_time = int(window[0:-1]) * 1000 * 60 * 60 * 24 * 7

            every_time = 0
            if isinstance(every, int):
                every_time = int(every)
                every = str(every) + 'ms'
            elif (every[-2:-1] == 'ms'):
                every_time = int(every[0:-1])
            elif (every[-1] == 's'):
                every_time = int(every[0:-1]) * 1000
            elif (every[-1] == 'm'):
                every_time = int(every[0:-1]) * 1000 * 60
            elif (every[-1] == 'h'):
                every_time = int(every[0:-1]) * 1000 * 60 * 60
            elif (every[-1] == 'd'):
                every_time = int(every[0:-1]) * 1000 * 60 * 60 * 24
            elif (every[-1] == 'w'):
                every_time = int(every[0:-1]) * 1000 * 60 * 60 * 24 * 7

            start_time = self.startTime
            if isinstance(start_time, int):
                start_time = int(start_time)
                start_time = str(start_time) + 'ms'
            elif (start_time[-2:-1] == 'ms'):
                start_time = int(start_time[0:-1])
            elif (start_time[-1] == 's'):
                start_time = int(start_time[0:-1]) * 1000
            elif (start_time[-1] == 'm'):
                start_time = int(start_time[0:-1]) * 1000 * 60
            elif (start_time[-1] == 'h'):
                start_time = int(start_time[0:-1]) * 1000 * 60 * 60
            elif (start_time[-1] == 'd'):
                start_time = int(start_time[0:-1]) * 1000 * 60 * 60 * 24
            elif (start_time[-1] == 'w'):
                start_time = int(start_time[0:-1]) * 1000 * 60 * 60 * 24 * 7

            start_time = str(int((start_time - offset_time )/ every_time) * every_time - window_time) + 'ms'

            
            feat = self.agregate.agregate_time(
                agr=aggregate,
                every=every,
                window=window,
                start_time=start_time,
                stop_time=stop_time,
                offset=offset_time,
                measurement=measurement,
                fields=fields,
                tags=tags
            )

            for f in fields:
                try:
                    feature_vector.append( feat["_value"][feat["_field"] == f].values )
                except:
                    print('python aggregate')
                    feature_vector.append(feat)

        feature_vector = np.transpose(np.array(feature_vector))
        times = feat["_time"][feat["_field"] == f].values

        return feature_vector, times

    def save(self):
        pass
