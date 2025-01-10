import argparse
import logging
import threading
import json
import time
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient
import pickle

from preprocessing import Buffer


FEDERATED_LEARNING = "FEDERATED_LEARNING"


def create_consumer(**kwargs):
    # Kafka consumer configuration
    conf_cons = {
        'bootstrap.servers': kwargs.get('kafka_broker_url'),  # Kafka broker URL
        'group.id': kwargs.get('kafka_consumer_group_id'),  # Consumer group ID
        'auto.offset.reset': kwargs.get('kafka_auto_offset_reset')
    }
    return Consumer(conf_cons)



def check_vehicle_topics():

    admin_client = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    existing_topics = admin_client.list_topics(timeout=10).topics.keys()
    # get all topics ending with "_weights"
    vehicle_topics = [topic for topic in existing_topics if topic.endswith("_weights")]
    return vehicle_topics


def deserialize_message(msg):

    try:
        # Decode the message and deserialize it into a Python dictionary
        message_value = pickle.loads(msg.value())
        logging.info(f"received message from topic [{msg.topic()}]")
        return message_value
    except json.JSONDecodeError as e:
        logging.error(f"Error deserializing message: {e}")
        return None


def process_message(topic, msg):
    """
        Process the deserialized message based on its topic.
    """
    pass


def consume_weights_data(vehicle_weights_topics, **kwargs):

    consumer = create_consumer(**kwargs)

    consumer.subscribe(vehicle_weights_topics)
    logger.info(f"will start consuming {vehicle_weights_topics}")

    try:
        while True:
            msg = consumer.poll(5.0)  
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"End of partition reached for {msg.topic()}")
                else:
                    logger.error(f"consumer error: {msg.error()}")
                continue

            deserialized_data = deserialize_message(msg)
            if deserialized_data:
                process_message(msg.topic(), deserialized_data)

    except KeyboardInterrupt:
        logger.info(f" Consumer interrupted by user.")
    except Exception as e:
        logger.error(f" Error in consumer: {e}")
    finally:
        consumer.close()
        logger.info(f" Consumer closed.")


def push_weights(**kwargs):
    while True:
        time.sleep(kwargs.get('weights_push_freq_seconds', 300))
        weights_reporter.push_weights(brain.model.state_dict())





def main():
    """
        Start the consumer for the specific vehicle.
    """
    global KAFKA_BROKER
    global anomalies_buffer, diagnostics_buffer, brain, metrics_reporter, logger, weights_reporter

    parser = argparse.ArgumentParser(description='Federated Learning script.')
    parser.add_argument('--logging_level', default='INFO' ,type=str, help='Logging level')
    parser.add_argument('--project_name', type=str, default="OPEN_FAIR", help='Wandb Project name')
    parser.add_argument('--run_name', type=str, default="Some run", help='Wandb run name')
    parser.add_argument('--online', action='store_true', help='Send wand metrics to the public wandb cloud')
    parser.add_argument('--kafka_broker_url', type=str, default='kafka:9092', help='Kafka broker URL')
    parser.add_argument('--kafka_consumer_group_id', type=str, default=FEDERATED_LEARNING, help='Kafka consumer group ID')
    parser.add_argument('--kafka_auto_offset_reset', type=str, default='earliest', help='Start reading messages from the beginning if no offset is present')
    parser.add_argument('--kafka_topic_update_interval_secs', type=int, default=30, help='Topic update interval for the kafka reader')

    args = parser.parse_args()

    logger = logging.getLogger(FEDERATED_LEARNING)
    logger.setLevel(args.logging_level)

    KAFKA_BROKER = args.kafka_broker_url

    logging.debug(f"Starting Federated Learning :)")

    # how many vehicles we have out there?
    vehicle_weights_topics = check_vehicle_topics()

    consuming_thread=threading.Thread(target=consume_weights_data, args=(vehicle_weights_topics,), kwargs=vars(args))
    consuming_thread.daemon=True
    consuming_thread.start()
    """
    

    training_thread=threading.Thread(target=train_model, kwargs=vars(args))
    training_thread.daemon=True
    
    
    training_thread.start()

    
    training_thread.join()
    """
    consuming_thread.join()


if __name__=="__main__":
    main()