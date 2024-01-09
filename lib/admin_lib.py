
def create_topics(conf, name_list: list, num_partitions: int = 1, replication_factor: int = 1):
    from confluent_kafka.admin import AdminClient, NewTopic
    
    # Create Admin Client
    admin_client = AdminClient(conf)
    
    # Return Topic Lists
    topics_metadata = admin_client.list_topics().topics
    for name in name_list:
        if name in topics_metadata:
            print(f"Topic '{name}' Already Exists.")
        else:
            # Define & Create New Topic
            new_topic = NewTopic(name, num_partitions, replication_factor)
            admin_client.create_topics([new_topic])
            print(f"Topic '{name}' created successfully with {num_partitions} partitions.")
            
def delete_topics(conf, name_list:list):
    from confluent_kafka.admin import AdminClient
    
    # Create Admin
    admin_client = AdminClient(conf)
    
    # Return Topic Lists
    topics_metadata = admin_client.list_topics().topics
    for name in name_list:
        if name not in topics_metadata:
            print(f"Topic '{name}' Does Not Exists.")
        else:
            # Delete Topic
            admin_client.delete_topics([name])
            print(f"Topic '{name}' deleted successfully.")

# Test
if __name__ == "__main__":
    kafka_conf = {
        'bootstrap.servers': 'localhost:9092'
    }
    topic_names = ['my_topic1']
    create_topics(kafka_conf, topic_names, num_partitions=3, replication_factor=1)
