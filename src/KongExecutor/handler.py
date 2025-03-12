import asyncio
import threading
import atexit
from flask import Flask, request, jsonify
import cloudpickle

import src.executor as executor
import src.dag as dag

app = Flask(__name__)

# Global event loop reference
loop = None
loop_thread = None

def _async_event_loop():
    """Runs the asyncio event loop in a background thread."""
    global loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_forever()

# Start the event loop thread
loop_thread = threading.Thread(target=_async_event_loop, daemon=True)
loop_thread.start()

@app.route('/', methods=['POST'])
def execute():
    try:
        subdag: dag.DAG = cloudpickle.loads(request.data)
        ex = executor.FlaskProcessExecutor(subdag, 'http://localhost:5000/')

        if loop and not loop.is_closed():
            loop.call_soon_threadsafe(loop.create_task, ex.start_executing())

        return jsonify({"status": "Accepted"}), 202
    except Exception as e:
        return str(e), 500

# Graceful shutdown handler
def shutdown():
    global loop
    if loop and not loop.is_closed():
        loop.call_soon_threadsafe(loop.stop)
        if loop_thread is not None:
            loop_thread.join()

# Register shutdown handler
atexit.register(shutdown)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
