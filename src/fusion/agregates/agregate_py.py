from agregate_query import AgregateQuery
from .agregate import Agregate

import pandas as pd

class AgregatePy(Agregate):
    # agregates calculated with python
    def __init__(self):
        #super().__init__()
        self.agregateQuery = AgregateQuery()

    def agregate(self, window:str = '5m',  start_time = '-1h', stop_time = '-0h', measurement = '', field = '', tags = {'':''}):
        if start_time is None:
            start_time = '-' + window

        query_str = self._build_query(
            start_time = start_time,
            stop_time = stop_time, 
            measurement = measurement, 
            field = field, 
            tags = tags
            )

    def saveToDB(self):
        pass
