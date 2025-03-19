import argparse
import logging
import threading
import json
import time
from confluent_kafka import Consumer, KafkaError

from preprocessing import HealthProbesBuffer
from brain import Brain
from communication import SMMetricsReporter
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import torch
import signal
import string
import random

SECURITY_MANAGER = "SECURITY_MANAGER"
HEALTHY = "HEALTHY"
INFECTED = "INFECTED"

batch_counter = 0
epoch_counter = 0

epoch_loss = 0
epoch_accuracy = 0
epoch_precision = 0
epoch_recall = 0
epoch_f1 = 0

health_records_received = 0
victim_records_received = 0
normal_records_received = 0

def create_consumer(**kwargs):
    def generate_random_string(length=10):
        letters = string.ascii_letters + string.digits
        return ''.join(random.choice(letters) for i in range(length))
    # Kafka consumer configuration
    conf_cons = {
        'bootstrap.servers': kwargs.get('kafka_broker_url'),  # Kafka broker URL
        'group.id': kwargs.get('kafka_consumer_group_id')+generate_random_string(7),  # Consumer group ID for message offset tracking
        'auto.offset.reset': kwargs.get('kafka_auto_offset_reset')  # Start reading from the earliest message if no offset is present
    }
    return Consumer(conf_cons)


def deserialize_message(msg):
    """
    Deserialize the JSON-serialized data received from the Kafka Consumer.

    Args:
        msg (Message): The Kafka message object.

    Returns:
        dict or None: The deserialized Python dictionary if successful, otherwise None.
    """
    try:
        # Decode the message and deserialize it into a Python dictionary
        message_value = json.loads(msg.value().decode('utf-8'))
        logger.debug(f"received message from topic [{msg.topic()}]")
        return message_value
    except json.JSONDecodeError as e:
        logger.error(f"Error deserializing message: {e}")
        return None


def process_message(topic, msg):
    global health_records_received, victim_records_received, normal_records_received

    logger.debug(f"Processing message from topic [{topic}]")

    assert topic.endswith("_HEALTH"), f"Unexpected topic {topic}"
    health_records_received += 1
    
    vehicle_name = topic.split('_')[0]
    
    if vehicle_state_dict[vehicle_name] == INFECTED:
        victim_buffer.add(msg)
        victim_records_received += 1
    else:
        normal_buffer.add(msg)
        normal_records_received += 1
        
    if health_records_received % 500 == 0:
        logger.info(f"Received {health_records_received} health records: {victim_records_received} victims, {normal_records_received} normal.")


def subscribe_to_topics(topic_regex):
    global consumer

    consumer.subscribe([topic_regex])
    logger.debug(f"(re)subscribed to health topics.")


def consume_health_data(**kwargs):
    global consumer

    consumer = create_consumer(**kwargs)

    subscribe_to_topics('^.*_HEALTH$')

    try:
        while not stop_threads:
            msg = consumer.poll(5.0)  # Poll per 1 secondo
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
        logger.info(f"consumer interrupted by user.")
    except Exception as e:
        logger.error(f" error in consumer {e}")
    finally:
        consumer.close()
        logger.info(f"consumer closed.")


def train_model(**kwargs):
    global brain, batch_counter, epoch_counter
    global epoch_loss, epoch_accuracy, epoch_precision, epoch_recall, epoch_f1

    batch_size = kwargs.get('batch_size', 32)
    epoch_size = kwargs.get('epoch_batches', 50)
    save_model_freq_epochs = kwargs.get('save_model_freq_epochs', 10)

    while not stop_threads:
        batch_feats = None
        batch_labels = None
        batch_preds = None
        do_train_step = False
        batch_loss = 0

        normal_feats, normal_labels = normal_buffer.sample(batch_size)
        victim_feats, victim_labels = victim_buffer.sample(batch_size)

        if len(normal_feats) > 0:
            batch_feats = normal_feats
            do_train_step = True
            batch_labels = normal_labels

        if len(victim_feats) > 0:
            do_train_step = True
            batch_feats = (victim_feats if batch_feats is None else torch.vstack((batch_feats, victim_feats)))
            batch_labels = (victim_labels if batch_labels is None else torch.vstack((batch_labels, victim_labels)))

        if do_train_step:
            batch_counter += 1
            batch_preds, loss = brain.train_step(batch_feats, batch_labels)


            # convert bath_preds to binary using pytorch:
            batch_preds = (batch_preds > 0.5).float()
            batch_loss += loss
            batch_accuracy = accuracy_score(batch_labels, batch_preds)
            batch_precision = precision_score(batch_labels, batch_preds, zero_division=0)
            batch_recall = recall_score(batch_labels, batch_preds, zero_division=0)
            batch_f1 = f1_score(batch_labels, batch_preds, zero_division=0)


            epoch_loss += batch_loss
            epoch_accuracy += batch_accuracy
            epoch_precision += batch_precision
            epoch_recall += batch_recall
            epoch_f1 += batch_f1

            if batch_counter % epoch_size == 0:
                epoch_counter += 1

                epoch_loss /= epoch_size
                epoch_accuracy /= epoch_size
                epoch_precision /= epoch_size
                epoch_recall /= epoch_size
                epoch_f1 /= epoch_size

                metrics_reporter.report({
                    'total_loss': epoch_loss,
                    'accuracy': epoch_accuracy,
                    'precision': epoch_precision,
                    'recall': epoch_recall,
                    'f1': epoch_f1,
                    'diagnostics_processed': normal_records_received,
                    'anomalies_processed': victim_records_received})
                
                epoch_loss = epoch_accuracy = epoch_precision = epoch_recall = epoch_f1 = 0

                if epoch_counter % save_model_freq_epochs == 0:
                    model_path = kwargs.get('model_saving_path', 'default_sm_model.pth')
                    logger.info(f"Saving model after {epoch_counter} epochs as {model_path}.")
                    brain.save_model()

        time.sleep(kwargs.get('training_freq_seconds', 1))


def signal_handler(sig, frame):
    global stop_threads, stats_consuming_thread, training_thread, pushing_weights_thread, pulling_weights_thread
    logger.debug(f"Received signal {sig}. Gracefully stopping security manager.")
    stop_threads = True


def resubscribe():
    while  not stop_threads:
        try:
            # Wait for a certain interval before resubscribing
            time.sleep(resubscribe_interval_seconds)
            subscribe_to_topics('^.*_HEALTH$')
        except Exception as e:
            logger.error(f"Error in periodic resubscription: {e}")


def main():

    global batch_size, stop_threads, stats_consuming_thread, training_thread
    global victim_buffer, normal_buffer, brain, metrics_reporter, logger
    global resubscribe_interval_seconds, epoch_batches, vehicle_state_dict

    parser = argparse.ArgumentParser(description='Start the intrusion detection process.')
    parser.add_argument('--kafka_broker_url', type=str, default='kafka:9092', help='Kafka broker URL')
    parser.add_argument('--kafka_consumer_group_id', type=str, default=SECURITY_MANAGER, help='Kafka consumer group ID')
    parser.add_argument('--kafka_auto_offset_reset', type=str, default='earliest', help='Start reading messages from the beginning if no offset is present')
    parser.add_argument('--buffer_size', type=int, default=10000, help='Size of the message buffer')
    parser.add_argument('--batch_size', type=int, default=32, help='Size of the batch')
    parser.add_argument('--logging_level', type=str, default='INFO', help='Logging level')
    parser.add_argument('--kafka_topic_update_interval_secs', type=int, default=15, help='Seconds interval between Kafka topic update')
    parser.add_argument('--learning_rate', type=float, default=0.001, help='Learning rate for the optimizer')
    parser.add_argument('--epoch_size', type=int, default=50, help='Number of batches per epoch (for reporting purposes)')
    parser.add_argument('--training_freq_seconds', type=float, default=1, help='Seconds interval between training steps')
    parser.add_argument('--save_model_freq_epochs', type=int, default=10, help='Number of epochs between model saving')
    parser.add_argument('--model_saving_path', type=str, default='default_sm_model.pth', help='Path to save the model')
    parser.add_argument('--initialization_strategy', type=str, default="xavier", help='Initialization strategy for global model')
    parser.add_argument('--input_dim', type=int, default=5, help='Input dimension of the model')
    parser.add_argument('--output_dim', type=int, default=1, help='Output dimension of the model')
    parser.add_argument('--h_dim', type=int, default=20, help='Hidden dimension of the model')
    parser.add_argument('--num_layers', type=int, default=3, help='Number of layers in the model')
    parser.add_argument('--dropout', type=float, default=0.1, help='Dropout rate')
    parser.add_argument('--optimizer', type=str, default='Adam', help='Optimizer for the model')
    parser.add_argument('--vehicle_names', type=str, default='', help='Space-separated array of vehicle names')
    parser.add_argument('--preconf_attacking_vehicles', type=str, default='', help='Space-separated array of preconfigured infected vehicles')
    args = parser.parse_args()

    assert len(args.vehicle_names) > 0
    vehicle_names = args.vehicle_names.split()
    preconf_attacking_vehicles = args.preconf_attacking_vehicles.split() if args.preconf_attacking_vehicles else []
    vehicle_state_dict = {vehicle_name: HEALTHY for vehicle_name in vehicle_names}
    for vehicle_name in preconf_attacking_vehicles:
        assert vehicle_name in vehicle_state_dict
        vehicle_state_dict[vehicle_name] = INFECTED
    

    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=str(args.logging_level).upper())
    logger = logging.getLogger('security_manager')

    logger.info(f"Starting security manager...")    

    brain = Brain(**vars(args))
    
    metrics_reporter = SMMetricsReporter(**vars(args))

    victim_buffer = HealthProbesBuffer(args.buffer_size, label=1)
    normal_buffer = HealthProbesBuffer(args.buffer_size, label=0)

    resubscribe_interval_seconds = args.kafka_topic_update_interval_secs
    resubscription_thread = threading.Thread(target=resubscribe)
    resubscription_thread.daemon = True
    
    stats_consuming_thread=threading.Thread(target=consume_health_data, kwargs=vars(args))
    stats_consuming_thread.daemon=True

    training_thread=threading.Thread(target=train_model, kwargs=vars(args))
    training_thread.daemon=True
    
    signal.signal(signal.SIGINT, lambda sig, frame: signal_handler(sig, frame))
    stop_threads = False

    stats_consuming_thread.start()
    training_thread.start()
    resubscription_thread.start()
    
    while not stop_threads:
        time.sleep(1)
    
    resubscription_thread.join(1)
    stats_consuming_thread.join(1)
    training_thread.join(1)
    consumer.close()
    logger.info("Exiting main thread.")
    


if __name__=="__main__":
    main()