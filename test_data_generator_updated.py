import requests
import json
import time
import random
import threading
from datetime import datetime
import socketio
from queue import Queue
from concurrent.futures import ThreadPoolExecutor

class EmojiClient:
    def __init__(self, user_id, cluster_id=1):
        self.user_id = user_id
        self.cluster_id = cluster_id
        self.sio = socketio.Client()
        self.received_updates = Queue()
        self.setup_socket_handlers()
        self.session = requests.Session()

    def setup_socket_handlers(self):
        @self.sio.on('connect')
        def on_connect():
            print(f"Client {self.user_id} connected to WebSocket in Cluster {self.cluster_id}")
            self.sio.emit('register', {'user_id': self.user_id, 'cluster_id': self.cluster_id})

        @self.sio.on('emoji_update')
        def on_emoji_update(data):
            self.received_updates.put(data)
            print(f"Received Update: {data}")

    def connect(self):
        try:
            self.sio.connect('http://localhost:5001')
        except Exception as e:
            print(f"Connection error for user {self.user_id}: {e}")

    def disconnect(self):
        self.sio.disconnect()
        self.session.close()

    def send_emoji_batch(self, batch_size=50):
        """Send multiple emojis in rapid succession"""
        emojis = ["😀", "😂", "😍", "🎉", "👍", "❤️"]
        
        for _ in range(batch_size):
            data = {
                "user_id": self.user_id,
                "emoji_type": random.choice(emojis),
                "timestamp": datetime.now().isoformat()
            }
            try:
                self.session.post(
                    'http://localhost:5000/send_emoji',
                    json=data,
                    headers={'Content-Type': 'application/json'}
                )
            except Exception as e:
                print(f"Error sending emoji for user {self.user_id}: {str(e)}")

def run_client(user_id, cluster_id=1, target_eps=200, duration=60):
    """
    Run a client targeting specific emojis per second
    target_eps: target emojis per second for this client
    """
    client = EmojiClient(f"user_{user_id}", cluster_id)
    client.connect()
    
    start_time = time.time()
    batch_size = 50  
    target_batches_per_sec = target_eps / batch_size
    sleep_time = 1.0 / target_batches_per_sec
    
    sent_count = 0
    
    while time.time() - start_time < duration:
        batch_start = time.time()
        client.send_emoji_batch(batch_size)
        sent_count += batch_size
        
        elapsed = time.time() - batch_start
        if elapsed < sleep_time:
            time.sleep(sleep_time - elapsed)
    
    actual_duration = time.time() - start_time
    eps = sent_count / actual_duration
    print(f"Client {user_id} (Cluster {cluster_id}) sent {sent_count} emojis in {actual_duration:.2f} seconds ({eps:.2f} emojis/sec)")
    client.disconnect()

def monitor_throughput(start_time, total_clients, target_total_eps):
    """Monitor and report overall system throughput"""
    while True:
        time.sleep(5) 
        elapsed = time.time() - start_time
        print(f"\nSystem running for {elapsed:.1f} seconds")
        print(f"Target throughput: {target_total_eps} emojis/second with {total_clients} clients")

if __name__ == "__main__":
    cluster_config = [
        {"num_clients": 15, "target_eps_per_client": 200, "cluster_id": 1},
        {"num_clients": 15, "target_eps_per_client": 180, "cluster_id": 2},
        {"num_clients": 15, "target_eps_per_client": 160, "cluster_id": 3}
    ]
    
    duration = 90  #90 = 90 seconds :)
    total_target_eps = sum(config['num_clients'] * config['target_eps_per_client'] for config in cluster_config)
    total_clients = sum(config['num_clients'] for config in cluster_config)
    
    print(f"Starting {total_clients} clients targeting {total_target_eps} total emojis per second across {len(cluster_config)} clusters")
    
    start_time = time.time()
    
    monitor_thread = threading.Thread(
        target=monitor_throughput,
        args=(start_time, total_clients, total_target_eps),
        daemon=True
    )
    monitor_thread.start()
    
    #managing client threads
    with ThreadPoolExecutor(max_workers=total_clients) as executor:
        futures = []
        for cluster_conf in cluster_config:
            for i in range(cluster_conf['num_clients']):
                futures.append(
                    executor.submit(
                        run_client,
                        i,
                        cluster_conf['cluster_id'],
                        cluster_conf['target_eps_per_client'],
                        duration
                    )
                )
                time.sleep(0.1)  
        
        #wait for all clients to complete!
        for future in futures:
            future.result()

    end_time = time.time()
    print(f"\nTest completed in {end_time - start_time:.2f} seconds")
