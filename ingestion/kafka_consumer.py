# (2.3) Task 2 - simple kafka consumer
# prints events in console
import json
from kafka import KafkaConsumer
from variables import KAFKA_BROKER, KAFKA_TOPIC


def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",  # start from beginning if no offset
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )


def run():
    consumer = create_consumer()
    print("Kafka consumer started. Listening for events...\n")

    try:
        for message in consumer:
            event = message.value

            print("----- EVENT RECEIVED -----")
            print(f"Partition: {message.partition}")
            print(f"Offset: {message.offset}")
            print(json.dumps(event, indent=2))
            print("--------------------------\n")

    except KeyboardInterrupt:
        print("\nStopping consumer...")

    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    run()
