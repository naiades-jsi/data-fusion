from src.data_base.query_data import QueryFromDB
from .agregate import Agregate

from datetime import datetime
import time 

class AgregateQuery(Agregate):
    # TODO test if works correctly

    def __init__(self):
        #super().__init__()
        self.queryDB = QueryFromDB()
        pass

    def agregate(self, agr:str = 'mean', window:str = '5m', start_time = None, stop_time = '-0m', measurement = '', field = '', tags = {'':''}):
        if start_time is None:
            start_time = '-' + window

        query_str = self._build_query(
            start_time = start_time,
            stop_time = stop_time, 
            measurement = measurement, 
            field = field, 
            tags = tags
            )
        query_str = self.queryDB.agregate(query_str, agr, window)

        print(query_str)
        return self.queryDB.query_df(query_str)

    def agregate_now(self, agr:str = 'mean', window:str = '5m', start_time = None, stop_time = '-0m', measurement = '', field = '', tags = {'':''}):
        # set offset so agregate is calculated from now 
        if start_time is None:
            start_time = '-' + window

        query_str = self._build_query(
            start_time = start_time,
            stop_time = stop_time, 
            measurement = measurement, 
            field = field, 
            tags = tags
            )
        
        query_str = self.queryDB.agregate(query_str, agr, window, offset=str(int(time.time() * 1000000))+'ms')

        print(query_str)
        return self.queryDB.query_df(query_str)
