import os

from log_processor_kafka.generator.ConnexionLogGenerator import ConnexionLogGenerator

ipv4_country_db_file_path = os.path.abspath('./ressources/geolite2-country-ipv4.csv')
connexion_log_gen = ConnexionLogGenerator(ipv4_country_db_file_path)
log = connexion_log_gen.generate_log()
print(log)

# from kafka.admin import KafkaAdminClient, NewTopic
# from kafka import KafkaProducer
#
# # Set your Kafka broker address
# bootstrap_servers = 'localhost:9092'
#
# # Create an admin client
# admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
#
# # Define the topic name and partition count
# topic_name = 'test_topic'
# partitions = 1
#
# topic_metadata = admin_client.list_topics()
# print("Existing topics:")
# print(topic_metadata)
#
# # Create a new topic
# new_topic = NewTopic(name=topic_name, num_partitions=partitions, replication_factor=1)
#
# # Check if the topic already exists
# if topic_name in topic_metadata:
#     print(f"Topic '{topic_name}' already exists.")
# else:
#     # Create the topic
#     admin_client.create_topics(new_topics=[new_topic], validate_only=False)
#     print(f"Topic '{topic_name}' created successfully.")
#
# # Close the admin client
# admin_client.close()
