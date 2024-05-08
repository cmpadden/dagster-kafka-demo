import logging
import os
import time
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

DESIRED_THROUGHPUT_PER_SECOND = 1000
bootstrap_servers = [os.environ.get("KAFKA_BOOTSTRAP_SERVER", "localhost:52000")]
topicName = "First_Topic"

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


producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

tstart = datetime.now()
for i in range(0, 100000):
    producer.send(topicName, bytes(f"Message {i}", "utf-8"))
    time.sleep(1 / DESIRED_THROUGHPUT_PER_SECOND)
    tstart = _print_throughput(i, tstart, DESIRED_THROUGHPUT_PER_SECOND)
