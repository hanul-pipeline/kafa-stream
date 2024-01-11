from confluent_kafka import Producer
from time import time

class KafkaProducer:
    def __init__(self, conf:dict):
        self.conf = conf
        self.producer_client = Producer(conf)
        # print("Kafka producer created successfully.") # test
    
    def publish_message(self, topic:str, key:str, message, partition:int=None):
        timestamp = int(time())
        try:
            if partition != None:
                self.producer_client.produce(topic=topic, key=key, value=message, partition=partition, timestamp=timestamp)
            else:
                self.producer_client.produce(topic=topic, key=key, value=message, timestamp=timestamp)
            self.producer_client.flush()
            print(f'Message published successfully to topic {topic}.')
        except Exception as E:
            print(f'Error appeared while publising message to topic {topic}. - {E}')

if __name__ == "__main__":
    conf = {'bootstrap.servers': 'localhost:9092'}
    producer = KafkaProducer(conf)
    producer.publish_message(topic="test", key="demo", message="Hello, Kafka!")
    