from src.data_base.query_data import QueryFromDB
from .agregate import Agregate

import pandas as pd

class AgregatePy(Agregate):
    # agregates calculated with python
    def __init__(self, token, url, organisation, bucket):
        super().__init__(token, url, organisation, bucket)
        self.queryDB = QueryFromDB(self.token, self.url, self.organisation, self.bucket)

    def customAggregate(self):
        result = None


        return result

    def agregate_rolling(self, agr='mean', every:str = '5m', window:str = '5min',  start_time = None, stop_time = '-0h', measurement = '', fields = [], tags = {None: None}):
        if start_time is None:
            start_time = '-' + window

        query_str = self._build_query(
            start_time = start_time,
            stop_time = stop_time, 
            measurement = measurement, 
            fields = fields, 
            tags = tags
            )
        
        df = self.queryDB.query_df(query_str)
        #return df
        #TODO different agregates
        if agr == 'mean':
            return df.rolling(window=window, on='_time').mean()
        elif agr == 'max':
            return df.rolling(window=window, on='_time').max()
        elif agr == 'min':
            return df.rolling(window=window, on='_time').min()
        elif agr == 'std':
            return df.rolling(window=window, on='_time').std()
        elif agr == 'median':
            return df.rolling(window=window, on='_time').median()
        elif agr == 'quantile50':
            return df.rolling(window=window, on='_time').quantile(q=0.5)
        elif agr == 'quantile90':
            return df.rolling(window=window, on='_time').quantile(q=0.9)
        elif agr == 'kurtosis':
            return df.rolling(window=window, on='_time').kurtosis()
        elif agr == 'skew':
            return df.rolling(window=window, on='_time').skew()


    def agregate_time(self, agr = 'mean', every:str = '5m', window:str = '5min',  start_time = None, stop_time = '-0h', shift= '0h', measurement = '', fields = [], tags = {None: None}, offset=None):
        if start_time is None:
            start_time = '-' + window

        query_str = self._build_query(
            start_time = start_time,
            stop_time = stop_time,
            shift = shift,
            measurement = measurement, 
            fields = fields, 
            tags = tags
            )

        df = self.queryDB.query_df(query_str)
        _time = df['_time'].iloc[-1] 

        if agr == 'mean':
            df = df.mean()
            df['_time'] = _time
            return df
        elif agr == 'max':
            df = df.max()
            df['_time'] = _time
            return df
        elif agr == 'min':
            df = df.min()
            df['_time'] = _time
            return df
        elif agr == 'std':
            df = df.std()
            df['_time'] = _time
            return df
        elif agr == 'median':
            df = df.median()
            df['_time'] = _time
            return df
        elif agr == 'quantile50':
            df = df.quantile(q=0.5)
            df['_time'] = _time
            return df
        elif agr == 'quantile90':
            df = df.quantile(q=0.9)
            df['_time'] = _time
            return df
        elif agr == 'kurtosis':
            df = df.kurtosis()
            df['_time'] = _time
            return df
        elif agr == 'skew':
            df = df.skew()
            df['_time'] = _time
            return df

        
    def saveToDB(self):
        pass
