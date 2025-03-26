import wandb
import argparse
import logging
from kafka_consumer import KafkaConsumer
import signal 
import time

WANDBER = "WANDBER"


class Wandber:


    def __init__(self, args):
        logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=str(args.logging_level).upper())
        self.logger = logging.getLogger(WANDBER)
        self.logger.setLevel(args.logging_level.upper())
        self.logger.debug("Initializing wandb")
        self.wandb_mode = ("online" if args.online else "disabled")
        self.alive = True
        wandb.init(
            project=args.project_name,
            mode=self.wandb_mode,
            name=args.run_name,
            config=dict(vars(args))
        )
        self.logger.debug(f"Wandb initialized in {self.wandb_mode} mode")
        self.step = 0
        self.kafka_consumer = KafkaConsumer(
            parent=self,
            kwargs=vars(args)
        )
        signal.signal(signal.SIGINT, lambda sig, frame: self.signal_handler(sig, frame))
        self.kafka_consumer.start()
        while self.alive:
            time.sleep(1)
        self.kafka_consumer.stop()
        self.close_wandb()


    def signal_handler(self, sig, frame):
        self.logger.debug(f"Received signal {sig}. Gracefully stopping wandb and its consumer threads.")
        self.alive = False


    def push_to_wandb(self, key, value, step=None, commit=True):
        # self.logger.debug(f"Pushing {key} to wandb")
        wandb.log(
            {key: value}, 
            step=(step if step is not None else self.step), 
            commit=commit)
        if step is None:
            self.step += 1


    def close_wandb(self):
        wandb.finish()
        self.logger.debug("Wandb closed correctly.")


def main():

    parser = argparse.ArgumentParser(description='Wandb reporter process for Open FAIR.')
    parser.add_argument('--logging_level', default='INFO' ,type=str, help='Logging level')
    parser.add_argument('--project_name', type=str, default="MY_OF", help='Wandb Project name')
    parser.add_argument('--run_name', type=str, default="Some run", help='Wandb run name')
    parser.add_argument('--online', action='store_true', help='Send wand metrics to the public wandb cloud')
    parser.add_argument('--kafka_broker_url', type=str, default='kafka:9092', help='Kafka broker URL')
    parser.add_argument('--kafka_consumer_group_id', type=str, default=WANDBER, help='Kafka consumer group ID')
    parser.add_argument('--kafka_auto_offset_reset', type=str, default='earliest', help='Start reading messages from the beginning if no offset is present')
    parser.add_argument('--kafka_topic_update_interval_secs', type=int, default=30, help='Topic update interval for the kafka reader')
    args = parser.parse_args()
    wandber = Wandber(args)


if __name__ == "__main__":
    main()
    
