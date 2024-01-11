from confluent_kafka import Consumer
from datetime import datetime
from threading import Thread

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
        # read datas (topic, partition, key, message, nowdate)
        topic = msg.topic()
        partition = msg.partition()
        key = msg.key().decode('utf-8') if msg.key() else None
        value = msg.value().decode('utf-8')
        nowdate = datetime.fromtimestamp(msg.timestamp()[-1]).strftime('%Y-%m-%d %H:%M:%S')
        
        threads = []
        threads.append(Thread(target = self.insert_measurement_to_mysql, args = (key, value, nowdate)))
        threads.append(Thread(target = self.define_grade, args = (topic, key, value, nowdate)))
        for thread in threads:
            thread.start()
        
    def insert_measurement_to_mysql(self, key, value, nowdate):
        sensor_id = key
        measurement = value
        insert_date = nowdate
        print("test")
    
    def insert_alert_to_mysql(self, key, value, nowdate):
        sensor_id = key
        measurement = value
        insert_date = nowdate
        print("test")
        
    def define_grade(self, topic, key, value, nowdate):
        location_id = topic.split("_")[-1]
        sensor_id = key
        measurement = value
        insert_date = nowdate
        
        self.insert_alert_to_mysql(key, value, nowdate)
        

if __name__ == "__main__":
    conf = {'bootstrap.servers': 'localhost:9092',         
            'group.id': 'TEST',
            'auto.offset.reset': 'earliest' }
    topics = ["test"]
    consumer = KafkaConsumerStream(conf, topics)
    consumer.poll_messages()
    