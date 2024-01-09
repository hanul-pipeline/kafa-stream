
def publish_message(conf, topic: str, key: str, message, partition:int=None):
    from confluent_kafka import Producer
 
    try:
        # Create Producer Client
        producer = Producer(conf)
        if partition != None:
            # Publish Message With Partitions
            producer.produce(topic=topic, key=key, value=message, partition=partition)
        else:
            # Publish Message
            producer.produce(topic=topic, key=key, value=message)
        producer.flush()
    except Exception as E:
        print(E)

# Test
if __name__ == "__main__":
    
    # json.dumps(message).encode('utf-8')

    kafka_conf = {
        'bootstrap.servers': 'localhost:9092'
    }
    topic_name = 'my_topic1'
    key_value = 'example_key'
    message_value = 'Hello, Kafka!'
    partition_number = 2 

    publish_message(kafka_conf, topic_name, key_value, message_value, partition=partition_number)