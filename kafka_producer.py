import json
import logging
import os
import random
import time
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

PRODUCER_THROUGHPUT_PER_SECOND = int(os.environ.get("PRODUCER_THROUGHPUT_PER_SECOND", 1000))
BOOTSTRAP_SERVERS = [os.environ.get("KAFKA_BOOTSTRAP_SERVER", "localhost:52000")]
TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "demo_topic_1")
CATEGORIES = ["option1", "option2", "option3"]

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)


def _print_throughput(i, tstart, DESIRED_THROUGHPUT_PER_SECOND):
    """Prints the throughput of events pushed to kafka."""
    if i % DESIRED_THROUGHPUT_PER_SECOND == 0:
        logging.info(
            f"Posted {DESIRED_THROUGHPUT_PER_SECOND} messages in {datetime.now() - tstart}"
        )
        tstart = datetime.now()
    return tstart


tstart = datetime.now()
for i in range(0, 100000):
    payload = {"category": random.choice(CATEGORIES), "value": i}
    producer.send(TOPIC_NAME, json.dumps(payload).encode("utf-8"))
    time.sleep(1 / PRODUCER_THROUGHPUT_PER_SECOND)
    tstart = _print_throughput(i, tstart, PRODUCER_THROUGHPUT_PER_SECOND)
