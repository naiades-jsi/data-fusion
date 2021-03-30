from src.data_base.query_data import QueryFromDB

class Agregate():

    def __init__(self, token:str='', url:str='', organisation:str='', bucket:str=''):
        self.token = token
        self.url = url
        self.organisation = organisation
        self.bucket = bucket

    def agregate(self, every:str = '5m', window:str = '5m',  start_time = '-1h', stop_time = '-0h', measurement = '', fields = [], tags = {'':''}):
        pass

    def _build_query(self, start_time = '-1h', stop_time = '-0h', measurement: str = '', fields:list = [], tags:dict = {'':''}):
        query_str = self.queryDB.bucket_query()
        query_str = self.queryDB.time_query(query_str, start_time, stop_time)
        query_str = self.queryDB.filter_query(query_str, measurement = measurement, fields = fields, tags = tags)
        
        query_str = self.queryDB.group(query_str)
        
        return query_str

    def saveToDB(self):
        pass

