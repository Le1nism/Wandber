import wandb
import argparse
import logging
from kafka_consumer import KafkaConsumer

WANDBER = "WANDBER"


class Wandber:
    def __init__(self, args):
        
        self.logger = logging.getLogger(WANDBER)
        self.logger.setLevel(args.logging_level.upper())
        self.logger.debug("Initializing wandb")
        self.wandb_mode = ("online" if args.wandb else "disabled")

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
        self.kafka_consumer.readining_thread.start()
        self.kafka_consumer.readining_thread.join()


    def push_to_wandb(self, key, value, step=None, commit=True):
        self.logger.debug(f"Pushing {key} to wandb")
        wandb.log(
            {key: value}, 
            step=(step if step is not None else self.step), 
            commit=commit)
        if step is None:
            self.step += 1


    def close_wandb(self):
        logger.debug("Closing wandb")
        wandb.finish()


def main():
    global logger, kafka_consumer

    parser = argparse.ArgumentParser(description='Wandb reporter process for Open FAIR.')
    parser.add_argument('--wandb', type=bool, default=False, help='True If wandb should be used in online mode')
    parser.add_argument('--logging_level', default='INFO' ,type=str, help='Logging level')
    parser.add_argument('--project_name', type=str, default="OPEN_FAIR", help='Wandb Project name')
    parser.add_argument('--run_name', type=str, default="Some run", help='Wandb run name')
    
    parser.add_argument('--kafka_broker_url', type=str, default='kafka:9092', help='Kafka broker URL')
    parser.add_argument('--kafka_consumer_group_id', type=str, default=WANDBER, help='Kafka consumer group ID')
    parser.add_argument('--kafka_auto_offset_reset', type=str, default='earliest', help='Start reading messages from the beginning if no offset is present')
    
    args = parser.parse_args()
    
    wandber = Wandber(args)




if __name__ == "__main__":
    main()