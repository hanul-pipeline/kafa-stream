from confluent_kafka import Producer

class KafkaProducer:
    def __init__(self, conf:dict):
        self.conf = conf
        self.producer_client = Producer(conf)
        # print("Kafka producer created successfully.") # test
    
    def publish_message(self, topic:str, key:str, message, partition:int=None):
        try:
            if partition != None:
                self.producer_client.produce(topic=topic, key=key, value=message, partition=partition)
            else:
                self.producer_client.produce(topic=topic, key=key, value=message)
            self.producer_client.flush()
            print(f'Message published successfully to topic {topic}.')
        except Exception as E:
            print(f'Error appeared while publising message to topic {topic}. - {E}')

if __name__ == "__main__":
    conf = {'bootstrap.servers': 'localhost:9092'}
    producer = KafkaProducer(conf)
    producer.publish_message(topic="test", key="demo", message="Hello, Kafka!")
    