from flask import Flask, request, jsonify
from sub_updated import Subscriber

app = Flask(__name__)
subscribers = {}  # Example: {"1": Subscriber(cluster_id="1")}

@app.route('/register', methods=['POST'])
def register_client():
    data = request.json
    cluster_id = data.get("cluster_id")
    client_id = data.get("client_id")
    client_topic = f'client_{client_id}_topic'
    
    if cluster_id not in subscribers:
        subscribers[cluster_id] = Subscriber(cluster_id)
        
    subscriber = subscribers[cluster_id]
    subscriber.register_client(client_id, client_topic)
    return jsonify({"status": "registered", "client_topic": client_topic})

@app.route('/deregister', methods=['POST'])
def deregister_client():
    data = request.json
    cluster_id = data.get("cluster_id")
    client_id = data.get("client_id")
    
    if cluster_id in subscribers:
        subscriber = subscribers[cluster_id]
        subscriber.deregister_client(client_id)
        return jsonify({"status": "deregistered"})
    return jsonify({"error": "Subscriber not found"}), 404

if __name__ == '__main__':
    app.run(port=5050)
