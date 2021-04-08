from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

class PushToDB():

    def __init__(self, config:str=''):
        # TODO create config
        self.token = "B1WdM9eu5E-GV5l3zA-eRvbboy_-2jzPyIZUW7x2rbhVLd2DRYRU-UgQrmdOf8bXTKAhMp8XVRhmSUCaeGpTZA=="
        self.url = "http://localhost:8086"
        self.org = "TestOrg"
        self.bucket = "TestBucket"
        self.client = InfluxDBClient(url=self.url, token=self.token, org=self.org)
        pass
        
    def push_data(self, point, bucket: str = 'TestBucket'):
        writer = self.client.write_api(write_options=SYNCHRONOUS)
        writer.write(bucket=bucket, record=point)
        pass

    def push_data_pd(self, df):
        #TODO
        pass

    def create_point(self, measurement: str, time, tags: dict, fields:dict):
        # TODO could add time zone
        point = Point(measurement)

        point.time(time)
        for key, value in tags.items():
            point = point.tag(key, value)
        for key, value in fields.items():
            point = point.field(key, value)
        
        return point
