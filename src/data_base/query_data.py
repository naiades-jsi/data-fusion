from influxdb_client import InfluxDBClient
import pandas as pd

class QueryFromDB():
    # defined functions to crated usefull querys
    # they do not have all options. More advanced options TODO in future.
    # or write query manualy
    # TODO test if all querys work 

    def __init__(self):
        # TODO create config
        
        self.token = "4eGf10hbhUsPENRw89Jjm_taql2teo-SEJlzL_ESp7Wpl_H3CR1A5sUzf7wGoz7mjM4SdBs3McPNXkrW7M0EzA=="
        self.url = "http://localhost:8086"
        self.org = "TestOrg"
        self.bucket = "TestBucket"
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        pass

    def query(self, query: str = ''):
        #query = f'from(bucket: \\"{self.bucket}\\") |> range(start: -1h)'
        tables = self.client.query_api().query(query, org=self.org)
        return tables

    def query_df(self, query:str):
        # DataFrames
        df = self.client.query_api().query_data_frame(query)
        if isinstance(df, pd.DataFrame):
            pass
        else:
            # if list of DataFrames concatenate
            df = pd.concat(df)    
        df = df.sort_values(by='_time').reset_index(drop=True)
        return df

    def bucket_query(self, query: str = ''):
        query = query + f'from(bucket:"{self.bucket}")'
        return query

    def time_query(self, query: str = '', start_time = '-1h', stop_time = '-0h'):
        query = query + f'|> range(start:{start_time}, stop:{stop_time})'
        return query

    def group(self, query: str = ''):
        # TODO
        query = query + '|> group()'
        return query
    
    def filter_query(self, query: str = '', measurement: str = '', field:str = '', tags:dict = {'':''}):
        
        # TODO may need some improvementst (check)

        filter_q = '|> filter(fn: (r) =>'
        if measurement != '':
            filter_q = filter_q + f'r._measurement == "{measurement}"'
        if field != '':
            if measurement != '':
                filter_q = filter_q + ' and '
            filter_q = filter_q + f'r._field == "{field}"'
        for key, value in tags.items():
            if key != '':
                if measurement != '' and field != '':
                    filter_q = filter_q + ' and '

                filter_q = filter_q + f'r.{key} == "{value}"'
        filter_q = filter_q + ')'

        query = query + filter_q
        return query

    def sort(self, query:str = ''):
        query = query + '|> sort(columns: ["_time"])'
        return query

    def yi(self, query: str = ''):
        query = query + '|> yield()'
        return query

    def window(self, query: str = '', every:str = '5m', offset = '0m'):
        query = query + f'|> window(every: {every}, offset: {offset})'
        return query

    def duplicate(self, query: str = '', column:str = "_stop", to:str = "_time"):
        query = query + f'|> duplicate(column: "{column}", as: "{to}")'
        return query

    def agregate(self, query: str = '', agr:str = 'mean', every:str = '5m', offset:str=None, timeSrc:str="_stop", timeDst:str="_time", createEmpty:str= 'true' ):
        if offset is None:
            query = query + f'|> aggregateWindow(every: {every}, fn: {agr}, timeSrc: "{timeSrc}", timeDst: "{timeDst}", createEmpty: {createEmpty} )'
        else:
            query = self.window(query, every, offset)
            query = query + f'|> {agr}()'
            query = self.duplicate(query, timeSrc, timeDst)

        return query
    