from src.data_base.query_data import QueryFromDB

class Agregate():

    def __init__(self):
        pass

    def agregate(self, window:str = '5m',  start_time = '-1h', stop_time = '-0h', measurement = '', field = '', tags = {'':''}):
        pass

    def _build_query(self, start_time = '-1h', stop_time = '-0h', measurement: str = '', field:str = '', tags:dict = {'':''}):
        query_str = self.queryDB.bucket_query()
        query_str = self.queryDB.time_query(query_str, start_time, stop_time)
        query_str = self.queryDB.filter_query(query_str, measurement = measurement, field = field, tags = tags)
        
        return query_str

    def saveToDB(self):
        pass

