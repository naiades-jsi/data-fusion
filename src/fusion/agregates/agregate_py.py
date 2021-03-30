from src.data_base.query_data import QueryFromDB
from .agregate import Agregate

import pandas as pd

class AgregatePy(Agregate):
    # agregates calculated with python
    def __init__(self):
        super().__init__()
        self.queryDB = QueryFromDB()

    def agregate_rolling(self, every:str = '5m', window:str = '5min',  start_time = None, stop_time = '-0h', measurement = '', fields = [], tags = {None: None}):
        # it is not the same as in influxDB some differences TODO check why
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
        return df.rolling(window=window, on='_time').min() # code in differend agregates TODO not just mean
        
    def agregate(self, every:str = '5m', window:str = '5min',  start_time = None, stop_time = '-0h', measurement = '', fields = [], tags = {None: None}):
        # it is not the same as in influxDB some differences TODO check why
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
        
        return df.min()['_value'] # code in differend agregates TODO not just mean

    def saveToDB(self):
        pass
