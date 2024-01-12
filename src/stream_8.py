import os, sys

sys.path.append(f"{os.path.dirname(os.path.abspath(__file__))}/../lib")
from consumer_class import KafkaConsumerStream

# define main
def main():
    try:
        conf = {
            'bootstrap.servers': 'localhost:9092',         
            'group.id': 'consumer_location_8',
            'auto.offset.reset': 'earliest' 
        }
        topics = ["location_8"]
        
        # run consumer
        consumer = KafkaConsumerStream(conf, topics)
        consumer.poll_messages()
    
    # report exception & restart script
    except Exception as E:
        print(E) # <--- fix
        print("RESTARTING SCRIPT.") # <--- fix
        main()

if __name__ == "__main__":
    main()
    