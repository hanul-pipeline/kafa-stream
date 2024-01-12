import traceback, requests, os, sys

sys.path.append(f"{os.path.dirname(os.path.abspath(__file__))}/../lib")
from consumer_class import KafkaConsumerStream
from notification_lib import send_line_noti_thread

# define main
def main():
    try:
        conf = {
            'bootstrap.servers': 'localhost:9092',         
            'group.id': 'consumer_location_4',
            'auto.offset.reset': 'earliest' 
        }
        topics = ["location_4"]
        
        # run consumer
        consumer = KafkaConsumerStream(conf, topics)
        consumer.poll_messages()
    
    # report exception & restart script
    except Exception as E:
        print(E) # <--- fix
        send_line_noti_thread(E) # <--- fix
        print("RESTARTING SCRIPT.") # <--- fix
        main()

if __name__ == "__main__":
    main()
    