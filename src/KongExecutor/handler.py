from threading import Thread
from flask import Flask, request, jsonify
import cloudpickle

import src.dag as dag

app = Flask(__name__)

# To be replaced by Lambda Handler
@app.route('/', methods=['POST'])
def execute():
    try:
        d: dag.DAG = cloudpickle.loads(request.data)
        # Don't wait for final result, just wait for the Executor to finish
        thread = Thread(target=d.start_local_execution)
        thread.start()

        return jsonify({"status": "Accepted"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)