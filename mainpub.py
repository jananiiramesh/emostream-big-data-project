from kafka import KafkaConsumer, KafkaProducer
import json

class MainPublisher:
    def __init__(self, brokers='localhost:9092'):
        self.processed_topic = 'processed_emoji_topic'
        self.cluster_topics = [f'cluster_{i}_topic' for i in range(1, 4)]
        self.consumer = KafkaConsumer(
            self.processed_topic,
            bootstrap_servers=brokers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='main_publisher_group',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        self.producer = KafkaProducer(
            bootstrap_servers=brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def start(self):
        print("Main Publisher is consuming and forwarding messages...")
        try:
            for message in self.consumer:
                data = message.value
                for topic in self.cluster_topics:
                    self.producer.send(topic, data)
                print(f"Forwarded to clusters: {data}")
        except Exception as e:
            print(f"Error in Main Publisher: {e}")
        finally:
            self.close()

    def close(self):
        self.consumer.close()
        self.producer.close()
        print("Main Publisher closed.")

if __name__ == "__main__":
    publisher = MainPublisher()
    try:
        publisher.start()
    except KeyboardInterrupt:
        print("Shutting down Main Publisher...")
        publisher.close()
