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


    def agregate_time(self, agr = 'mean', every:str = '5m', window:str = '5min',  start_time = None, stop_time = '-0h', measurement = '', fields = [], tags = {None: None}, offset=None):
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
        
        if agr == 'mean':
            return df.mean()['_value']
        elif agr == 'max':
            return df.max()['_value']
        elif agr == 'min':
            return df.min()['_value']
        elif agr == 'std':
            return df.std()['_value']
        elif agr == 'median':
            return df.median()['_value']
        
    def saveToDB(self):
        pass
