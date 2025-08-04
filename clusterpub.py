from kafka import KafkaConsumer, KafkaProducer
import json

class ClusterPublisher:
    def __init__(self, cluster_id, brokers='localhost:9092'):
        self.cluster_topic = f'cluster_{cluster_id}_topic'
        self.subscriber_topic = f'subscriber_cluster_{cluster_id}_topic'
        self.consumer = KafkaConsumer(
            self.cluster_topic,
            bootstrap_servers=brokers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id=f'cluster_{cluster_id}_group',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def start(self):
        print(f"Cluster Publisher {self.cluster_topic} is running...")
        try:
            for message in self.consumer:
                data = message.value
                self.producer.send(self.subscriber_topic, data)
                print(f"Forwarded to subscribers: {data}")
        except Exception as e:
            print(f"Error in Cluster Publisher {self.cluster_topic}: {e}")
        finally:
            self.close()

    def close(self):
        self.consumer.close()
        self.producer.close()
        print(f"Cluster Publisher {self.cluster_topic} closed.")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python cluster_publisher.py <cluster_id>")
        sys.exit(1)
    cluster_id = sys.argv[1]
    publisher = ClusterPublisher(cluster_id)
    try:
        publisher.start()
    except KeyboardInterrupt:
        print(f"Shutting down Cluster Publisher {cluster_id}...")
        publisher.close()
