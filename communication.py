from confluent_kafka import SerializingProducer, KafkaException
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.admin import AdminClient, NewTopic

import json
import logging


SECURITY_TOPIC = "security"


class SMMetricsReporter:
    def __init__(self, **kwargs):
        self.kafka_broker_url = kwargs.get('kafka_broker_url', 'kafka:9092')
        conf_prod_stat={
        'bootstrap.servers': self.kafka_broker_url,  # Kafka broker URL
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': lambda v, ctx: json.dumps(v)
         }
        
        self.producer = SerializingProducer(conf_prod_stat)    
        self.logger = logging.getLogger("sec_mgmt_metrics_reporter")
        self.logger.setLevel(str(kwargs.get('logging_level', 'INFO')).upper())
        self.check_and_create_security_topic()


    def report(self, metrics):

        try:
            self.producer.produce(topic=SECURITY_TOPIC, value=metrics)
            self.producer.flush()
            self.logger.debug(f"Published to topic: {SECURITY_TOPIC}")
        except Exception as e:
            self.logger.error(f"Failed to produce statistics: {e}")

    def check_and_create_security_topic(self):
        admin_client = AdminClient({'bootstrap.servers': self.kafka_broker_url})
        existing_topics = admin_client.list_topics(timeout=10).topics.keys()

        if SECURITY_TOPIC not in existing_topics:
            topic_to_create = NewTopic(SECURITY_TOPIC, num_partitions=1, replication_factor=1)
        
            self.logger.debug(f"Creating missing security topic: {SECURITY_TOPIC}")
            result = admin_client.create_topics([topic_to_create])

            for topic, future in result.items():
                try:
                    future.result()
                    self.logger.info(f"Topic '{topic}' created successfully.")
                except KafkaException as e:
                    self.logger.error(f"Failed to create topic '{topic}': {e}")