from src.data_base.query_data import QueryFromDB

class Aggregate():
    """
    Stream aggregate base class.
    """

    def __init__(self, token:str='', url:str='', organisation:str='', bucket:str=''):
        """
        Initialize the aggregate class.

        Parameters
        ----------
        token : str, optional
            Token for the InfluxDB. The default is ''.
        url : str, optional
            URL of the InfluxDB. The default is ''.
        organisation : str, optional
            Organisation of the InfluxDB. The default is ''.
        bucket : str, optional
            Bucket of the InfluxDB. The default is ''.

        Returns
        -------
        None.
        """

        self.token = token
        self.url = url
        self.organisation = organisation
        self.bucket = bucket

    def aggregate(self, every:str = '5m', window:str = '5m',  start_time = '-1h', stop_time = '-0h', shift = '0h', measurement = '', fields = [], tags = {'':''}):
        pass

    def _build_query(self, start_time = '-1h', stop_time = '-0h', shift='0h', measurement: str = '', fields:list = [], tags:dict = {'':''}):
        """
        Build the query string.

        Parameters
        ----------
        start_time : str, optional
            Start time of the query. The default is '-1h'.
        stop_time : str, optional
            Stop time of the query. The default is '-0h'.
        shift : str, optional
            Shift time of the query. The default is '0h'.
        measurement : str, optional
            Measurement of the query. The default is ''.
        fields : list, optional
            Fields of the query. The default is [].
        tags : dict, optional
            Tags of the query. The default is {'':''}.

        Returns
        -------
        query_str : str
            Query string.
        """
        query_str = self.queryDB.bucket_query()
        query_str = self.queryDB.time_query(query_str, start_time, stop_time)
        query_str = self.queryDB.filter_query(query_str, measurement = measurement, fields = fields, tags = tags)
        query_str = self.queryDB.shift_time(query_str, shift = shift)


        query_str = self.queryDB.group(query_str)

        return query_str

    def saveToDB(self):
        """
        Save the aggregate to the InfluxDB (stub).

        Returns
        -------
        None.
        """
        pass

