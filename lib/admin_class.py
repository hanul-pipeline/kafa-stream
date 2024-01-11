from confluent_kafka.admin import AdminClient, NewTopic

class KafkaAdmin:
    def __init__(self, conf:dict):
        self.conf = conf
        self.admin_client = AdminClient(self.conf)
        # print("Kafka admin created successfully.") # test
    
    def create_topics(self, name_list:list, num_partitions:list=None, replication_factor:list=None):
        from time import sleep
        # check topic existance
        topics_metadata = self.admin_client.list_topics().topics
        
        for i in range(len(name_list)):
            name = name_list[i]
            if name in topics_metadata:
                print(f"Topic '{name}' Already Exists.")
            else:
                # create topic
                new_topic = NewTopic(name, 
                                     num_partitions[i] if num_partitions != None else 1, 
                                     replication_factor[i] if replication_factor != None else 1)
                self.admin_client.create_topics([new_topic])
                print(f"Topic '{name}' created successfully.")
            sleep(1)
    
    def delete_topics(self, name_list:list):
        from time import sleep
        # check topic existance
        topics_metadata = self.admin_client.list_topics().topics
        for name in name_list:
            if name not in topics_metadata:
                print(f"Topic '{name}' Does Not Exists.")
            else:
                # delete topic
                self.admin_client.delete_topics([name])
                print(f"Topic '{name}' deleted successfully.")
            sleep(1)

if __name__ == "__main__":
    conf = {'bootstrap.servers': 'localhost:9092'}
    admin = KafkaAdmin(conf)
    admin.delete_topics(name_list=["test"])
    