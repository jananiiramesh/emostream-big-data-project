from flask import Flask, request, jsonify
from kafka import KafkaProducer
import json
from threading import Thread
import time

app = Flask(__name__)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

FLUSH_INTERVAL = 0.5
emoji_queue = []

def flush_to_kafka():
    while True:
        time.sleep(FLUSH_INTERVAL)
        if emoji_queue:
            data_batch = emoji_queue[:]
            emoji_queue.clear()
            for data in data_batch:
                producer.send('emoji_topic', value=data)
            producer.flush()

flush_thread = Thread(target=flush_to_kafka, daemon=True)
flush_thread.start()

@app.route('/send_emoji', methods=['POST'])
def send_emoji():
    try:
        data = request.json
        if not data or 'user_id' not in data or 'emoji_type' not in data or 'timestamp' not in data:
            return jsonify({'error': 'Invalid data format'}), 400
        
        emoji_queue.append(data)
        return jsonify({'message': 'Emoji data received'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000, threaded=True)
