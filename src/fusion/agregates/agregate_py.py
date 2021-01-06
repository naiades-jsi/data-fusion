from src.data_base.query_data import QueryFromDB
from .agregate import Agregate

import pandas as pd

class AgregatePy(Agregate):
    # agregates calculated with python
    def __init__(self):
        super().__init__()
        self.queryDB = QueryFromDB()

    def agregate(self, window:str = '5min',  start_time = '-1h', stop_time = '-0h', measurement = '', field = '', tags = {'':''}):
        # it is not the same as in influxDB some differences TODO check why
        if start_time is None:
            start_time = '-' + window

        query_str = self._build_query(
            start_time = start_time,
            stop_time = stop_time, 
            measurement = measurement, 
            field = field, 
            tags = tags
            )
        
        df = self.queryDB.query_df(query_str)
        #return df
        return df.rolling(window=window, on='_time').mean()

    def saveToDB(self):
        pass
