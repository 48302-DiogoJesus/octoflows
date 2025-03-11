from flask import Flask, request, jsonify
import cloudpickle

from ..dag import DAG

app = Flask(__name__)

@app.route('/', methods=['POST'])
def execute():
    try:
        dag: DAG = cloudpickle.loads(request.data)
        dag.start_local_execution()

        return jsonify({"status": "Accepted"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)