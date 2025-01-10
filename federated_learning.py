import argparse
import logging
import threading
import json
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient
from aggregation import federated_averaging, fed_yogi
import pickle
from modules import MLP
from preprocessing import Buffer
import time

FEDERATED_LEARNING = "FEDERATED_LEARNING"

aggregation_functions = {
    "fedavg": federated_averaging,
    "fedyogi": fed_yogi,
    "fedprox": None,
    "fedsgd": None
    }


def create_consumer(**kwargs):
    # Kafka consumer configuration
    conf_cons = {
        'bootstrap.servers': kwargs.get('kafka_broker_url'),  # Kafka broker URL
        'group.id': kwargs.get('kafka_consumer_group_id'),  # Consumer group ID
        'auto.offset.reset': kwargs.get('kafka_auto_offset_reset')
    }
    return Consumer(conf_cons)



def check_vehicle_topics(**kwargs):

    admin_client = AdminClient({'bootstrap.servers': kwargs.get('kafka_broker_url')})
    existing_topics = admin_client.list_topics(timeout=10).topics.keys()
    # get all topics ending with "_weights"
    vehicle_topics = [topic for topic in existing_topics if topic.endswith("_weights")]
    logger.debug("Found the following vehicle topics: %s", vehicle_topics)
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


def init_global_model(**kwargs):
    initialization_strategy = kwargs.get('initialization_strategy')
    global_model.initialize_weights(initialization_strategy)
    logger.info(f"Global model initialized using {initialization_strategy} initialization.")
    

def process_message(topic, msg, **kwargs):
    """
        Process the deserialized message based on its topic.
    """
    global weights_buffer

    weights_buffer[topic].add(msg)

    if kwargs.get('aggregation_interval_secs') == 0:
        aggregate_weights(**kwargs)
        

def aggregate_weights_periodically(**kwargs):
    while True:
        time.sleep(kwargs.get('aggregation_interval_secs'))
        aggregate_weights(**kwargs)


def aggregate_weights(**kwargs):
    global global_model, weights_buffer

    # check if we have at least one element in each buffer:
    if all([len(buffer) > 0 for buffer in weights_buffer.values()]):
        logger.debug(f"Aggregating the weights from {len(weights_buffer)} vehicles.")
        aggregation_function = aggregation_functions[kwargs.get('aggregation_strategy')]
        if aggregation_function is federated_averaging:
            aggregated_state_dict = aggregation_function(global_model.state_dict(), [buffer.get() for buffer in weights_buffer.values()])
        else:
            aggregated_state_dict = aggregation_function(global_model.state_dict(), [buffer.get() for buffer in weights_buffer.values()], **kwargs)
        
        # pop the oldest element from each one of the buffers
        for buffer in weights_buffer.values():
            buffer.pop()
        global_model.load_state_dict(aggregated_state_dict)
    else:
        logger.debug(f"Waiting for more data to aggregate the weights.")


def create_weights_buffer(vehicle_weights_topics, **kwargs):
    """
        Create a buffer for each vehicle to store the weights.
    """
    weights_buffer = {}
    for topic in vehicle_weights_topics:
        weights_buffer[topic] = Buffer(size=kwargs.get('weights_buffer_size', 3), label=topic)
    return weights_buffer


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
                process_message(msg.topic(), deserialized_data, **kwargs)

    except KeyboardInterrupt:
        logger.info(f" Consumer interrupted by user.")
    except Exception as e:
        logger.error(f" Error in consumer: {e}")
    finally:
        consumer.close()
        logger.info(f" Consumer closed.")


def create_global_model_placeholder(**kwargs):
    return MLP(kwargs.get('input_dim', 59), kwargs.get('output_dim', 1), **kwargs)


def main():
    global logger, weights_buffer, global_model

    parser = argparse.ArgumentParser(description='Federated Learning script.')
    parser.add_argument('--logging_level', default='INFO' ,type=str, help='Logging level')
    parser.add_argument('--project_name', type=str, default="OPEN_FAIR", help='Wandb Project name')
    parser.add_argument('--run_name', type=str, default="Some run", help='Wandb run name')
    parser.add_argument('--online', action='store_true', help='Send wand metrics to the public wandb cloud')
    parser.add_argument('--kafka_broker_url', type=str, default='kafka:9092', help='Kafka broker URL')
    parser.add_argument('--kafka_consumer_group_id', type=str, default=FEDERATED_LEARNING, help='Kafka consumer group ID')
    parser.add_argument('--kafka_auto_offset_reset', type=str, default='earliest', help='Start reading messages from the beginning if no offset is present')
    parser.add_argument('--kafka_topic_update_interval_secs', type=int, default=30, help='Topic update interval for the kafka reader')
    parser.add_argument('--initialization_strategy', type=str, default="xavier", help='Initialization strategy for global model')
    parser.add_argument('--aggregation_strategy', type=str, default="fedavg", help='Aggregation strategy for FL')
    parser.add_argument('--weights_buffer_size', type=int, default=3, help='Size of the buffer for weights')
    parser.add_argument('--aggregation_interval_secs', type=int, default=59, help='Aggregation interval in seconds')
    args = parser.parse_args()

    logger = logging.getLogger(FEDERATED_LEARNING)
    logger.setLevel(args.logging_level)

    # create a global model placeholder
    global_model = create_global_model_placeholder(**vars(args))
    init_global_model(**vars(args))

    # how many vehicles we have out there?
    vehicle_weights_topics = check_vehicle_topics(**vars(args))

    # create buffers for the local weights of each vehicle
    weights_buffer = create_weights_buffer(vehicle_weights_topics, **vars(args))

    consuming_thread=threading.Thread(target=consume_weights_data, args=(vehicle_weights_topics,), kwargs=vars(args))
    consuming_thread.daemon=True
    consuming_thread.start()

    if args.aggregation_interval_secs > 0:
        # create a thread to aggregate the weights each aggregation_interval_secs:
        aggregation_thread = threading.Thread(target=aggregate_weights_periodically, kwargs=vars(args))
        aggregation_thread.start()
        

    consuming_thread.join()
    if args.aggregation_interval_secs > 0:
        aggregation_thread.join()


if __name__=="__main__":
    main()