import json
from confluent_kafka import Producer
import time


def read_config():
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def produce(topic, config):
  producer = Producer(config)

  with open("airbnb_reviews.json", "r", encoding='utf-8') as file:
    for line in file:
      line = json.loads(line)
      value = json.dumps(line)
      key = line.get("_id", "default_key")
      producer.produce(topic, key=key, value=value)
      time.sleep(5)
  producer.flush()

def main():
  config = read_config()
  topic = "kafka-ingestion"

  produce(topic, config)

main()