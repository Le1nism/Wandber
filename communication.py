from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
import json
import logging


class SMMetricsReporter:
    def __init__(self, **kwargs):
        kafka_broker_url = kwargs.get('kafka_broker')
        conf_prod_stat={
        'bootstrap.servers': kafka_broker_url,  # Kafka broker URL
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': lambda v, ctx: json.dumps(v)
         }
        
        self.producer = SerializingProducer(conf_prod_stat)    
        self.logger = logging.getLogger("sec_mgmt_metrics_reporter")
        self.logger.setLevel(kwargs.get('logging_level', str(kwargs.get('logging_level', 'INFO')).upper()))

    def report(self, metrics):

        topic_statistics=f"nid_statistics"
        try:
            self.producer.produce(topic=topic_statistics, value=metrics)
            self.producer.flush()
            self.logger.debug(f"Published to topic: {topic_statistics}")
        except Exception as e:
            self.logger.error(f"Failed to produce statistics: {e}")