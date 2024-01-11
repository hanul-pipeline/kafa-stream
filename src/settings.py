import os, sys

sys.path.append(f"{os.path.dirname(os.path.abspath(__file__))}/../lib")

def create_topics():
    from admin_class import KafkaAdmin

    # create admin client
    conf = {'bootstrap.servers': 'localhost:9092'}
    admin = KafkaAdmin(conf = conf)

    # topic = location_id
    # partition = 
    # key = sensor_id
    # message = measurement

    # create topics
    name_list = ["location_4", "location_7", "location_8"]
    num_partitions = [1, 4, 3]
    admin.create_topics(name_list = name_list, num_partitions = num_partitions)

if __name__ == "__main__":
    create_topics()