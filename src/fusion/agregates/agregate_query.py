from src.data_base.query_data import QueryFromDB
from .agregate import Agregate

from datetime import datetime
import time

class AgregateQuery(Agregate):
    """
    Aggreagte the data from the database (InfluxDB).
    """
    def __init__(self, token, url, organisation, bucket):
        super().__init__(token, url, organisation, bucket)
        self.queryDB = QueryFromDB(self.token, self.url, self.organisation, self.bucket)
        pass

    """ def agregate(self, agr:str = 'mean', every:str = '5m', window:str = '5m', start_time = None, stop_time = '-0m', measurement = '', fields = [], tags = {None: None}):
        if start_time is None:
            start_time = '-' + window

        query_str = self._build_query(
            start_time = start_time,
            stop_time = stop_time,
            measurement = measurement,
            fields = fields,
            tags = tags
        )

        query_str = self.queryDB.agregate(query_str, agr, every, window)

        return self.queryDB.query_df(query_str)

    def agregate_now(self, agr:str = 'mean', every:str = '5m', window:str = '5m', start_time = None, stop_time = '-0m', measurement = '', fields = [], tags = {None: None}):
        # set offset so agregate is calculated from now
        if start_time is None:
            start_time = '-' + window

        query_str = self._build_query(
            start_time = start_time,
            stop_time = stop_time,
            measurement = measurement,
            fields = fields,
            tags = tags
        )

        query_str = self.queryDB.agregate(query_str, agr, every, window, offset=str(int(time.time() * 1000 ))+'ms')

        return self.queryDB.query_df(query_str) """

    def agregate_time(self, agr:str = 'mean', every:str = '5m', window:str = '5m', start_time = None, stop_time = '-0m', shift = '0m', offset:int=0, measurement = '', fields = [], tags = {None: None}):
        # set offset so agregate is calculated from now
        if isinstance(every, int):
            every = str(every) + 'ms'

        query_str = self._build_query(
            start_time = start_time,
            stop_time = stop_time,
            shift = shift,
            measurement = measurement,
            fields = fields,
            tags = tags
        )

        query_str = self.queryDB.agregate(query_str, agr, every, window, offset=str(int(time.time() * 1000 - offset))+'ms')

        return self.queryDB.query_df(query_str)
