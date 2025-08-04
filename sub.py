from kafka import KafkaConsumer
import json

class Subscriber:
    def __init__(self, cluster_id, brokers='localhost:9092'):
        self.subscriber_topic = f'subscriber_cluster_{cluster_id}_topic'
        self.consumer = KafkaConsumer(
            self.subscriber_topic,
            bootstrap_servers=brokers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=f'subscriber_cluster_{cluster_id}_group',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )

    def start(self):
        print(f"Subscriber for {self.subscriber_topic} is listening...")
        try:
            for message in self.consumer:
                data = message.value
                print(f"Received by subscriber: {data}")
        except Exception as e:
            print(f"Error in Subscriber: {e}")
        finally:
            self.close()

    def close(self):
        self.consumer.close()
        print(f"Subscriber for {self.subscriber_topic} closed.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python subscriber.py <cluster_id>")
        sys.exit(1)
    cluster_id = sys.argv[1]
    subscriber = Subscriber(cluster_id)
    try:
        subscriber.start()
    except KeyboardInterrupt:
        print(f"Shutting down Subscriber for cluster {cluster_id}...")
        subscriber.close()
