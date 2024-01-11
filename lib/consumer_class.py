from confluent_kafka import Consumer
from threading import Thread
import json

class KafkaConsumer:
    def __init__(self, conf:dict):
        self.conf = conf
        self.consumer_client = Consumer(conf)
        # print("Kafka consumer created successfully.") # test

class KafkaConsumerStream(KafkaConsumer):
    def __init__(self, consumer_client, topics:list):
        super().__init__(consumer_client)
        self.topics = topics
        self.consumer_client.subscribe(self.topics)

    def poll_messages(self, period:float=1.0):
        try:
            while True:
                # set poll
                msg = self.consumer_client.poll(period)
                if msg is None:
                    continue
                
                # run functions
                self.tasks(msg)

        except KeyboardInterrupt:
            pass
        
        finally:
            self.consumer_client.close()
        
    def tasks(self, msg):
        # read datas (topic, partition, key, message)
        topic = msg.topic()
        partition = msg.partition()
        key = msg.key().decode('utf-8') if msg.key() else None
        value = msg.value().decode('utf-8')
        # value_type = type(value)
        # print(f"topic: {topic}, partition: {partition}, key: {key}, value: {value}, value type: {value_type}")
        
        threads = []
        threads.append(Thread(target = self.insert_to_mysql, args = (key, value)))
        threads.append(Thread(target = self.define_grade, args = (topic, key, value)))
        for thread in threads:
            thread.start()
        
    def insert_to_mysql(self, key, value):
        sensor_id = key
        measurement = value
        print("test")
        
    def define_grade(self, topic, key, value):
        location_id = topic.split("_")[-1]
        sensor_id = key
        measurement = value
        print("test")
        
if __name__ == "__main__":
    conf = {'bootstrap.servers': 'localhost:9092',         
            'group.id': 'TEST',
            'auto.offset.reset': 'earliest' }
    topics = ["test"]
    consumer = KafkaConsumerStream(conf, topics)
    consumer.poll_messages()
    