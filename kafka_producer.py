import logging
import os
import time
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

PRODUCER_THROUGHPUT_PER_SECOND = int(os.environ.get("PRODUCER_THROUGHPUT_PER_SECOND", 1000))
BOOTSTRAP_SERVERS = [os.environ.get("KAFKA_BOOTSTRAP_SERVER", "localhost:52000")]
TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "demo_topic_1")

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


def _print_throughput(i, tstart, DESIRED_THROUGHPUT_PER_SECOND):
    """Prints the throughput of events pushed to kafka."""
    if i % DESIRED_THROUGHPUT_PER_SECOND == 0:
        logging.info(
            f"Posted {DESIRED_THROUGHPUT_PER_SECOND} messages in {(datetime.now() - tstart).seconds} seconds"
        )
        tstart = datetime.now()
    return tstart


producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

tstart = datetime.now()
for i in range(0, 100000):
    producer.send(TOPIC_NAME, bytes(f"Message {i}", "utf-8"))
    time.sleep(1 / PRODUCER_THROUGHPUT_PER_SECOND)
    tstart = _print_throughput(i, tstart, PRODUCER_THROUGHPUT_PER_SECOND)
