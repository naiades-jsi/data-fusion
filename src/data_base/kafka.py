from .push_data import PushToDB

from kafka import KafkaConsumer

import time
import copy
import json

class Kafka ():
    def __init__(self, topics = [], bootstrap_servers="localhost:9092", token="", org="", url=""):
        self.topics = topics
        self.bootstrap_servers = bootstrap_servers
        self.token = token
        self.org = org
        self.url = url
        pass
        
        
    def pushFromKafka(self, bucket="", timestamp_def="timestamp", fields_def=None, tags_def=None, measurement='', topics = ['measurements_node_test'], bootstrap_server="localhost:9092"):

        url = self.url
        token = self.token
        org = self.org

        push = PushToDB(token=token, url=url, org=org)


        consumer = KafkaConsumer(bootstrap_servers=bootstrap_server)
        consumer.subscribe(topics)

        for msg in consumer:
            rec = eval(msg.value)
            timestamp = rec[timestamp]

            fields = {}
            for key in fields_def.keys():
                fields[key] = rec[fields_def[key]]
            if fields_def==None:
                fields = None

            tags = {}
            for key in tags_def.keys():
                fields[key] = rec[fields_def[key]]
            if tags_def==None:
                tags = None

            point = push.create_point(measurement, timestamp, tags, fields)
            push.push_data(point, bucket)
    
    def listenKafkaForFusion(self, function=None, t=5, timestamp_def="timestamp", file_name=""):
        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap_servers)
        consumer.subscribe(self.topics)
        topic_list = copy.deepcopy(self.topics)

        time_dic = {}
        for topic in topics:
            time_dic[topic] = 0

        file_kafka = open("../kafka/ " file_name + ".json", "w+")

        file_kafka.write(json.dumps(time_dic, indent=4, sort_keys=True))


        for msg in consumer:
            topic = msg.topic
            rec = eval(msg.value)
            timestamp = rec[timestamp]

            time_dic[topic] = timestamp
            file_kafka.write(json.dumps(time_dic, indent=4, sort_keys=True))

            time.sleep(t)
            function(kafka = True) # TODO function should read file and accept argument kafka and run if times in file acceptable

        file_kafka.colse()
