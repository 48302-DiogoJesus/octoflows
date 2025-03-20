import os
import sys
import time
# import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.storage.in_memory_storage import InMemoryStorage
from src.storage.redis_storage import RedisStorage
from src.worker import DockerWorker, LocalWorker
from src.dag_task_node import DAGTask

@DAGTask
def calculate_discount(original_price: float, discount_rate: float) -> float:
    # time.sleep(2)
    """Calculate the discounted price."""
    return original_price * (1 - discount_rate)

@DAGTask
def calculate_total_revenue(prices: list[float]) -> float:
    # time.sleep(1)
    """Calculate the total revenue from a list of prices."""
    return sum(prices)

redis_intermediate_storage_config = RedisStorage.Config(host="localhost", port=6379, password="redisdevpwd123")
inmemory_intermediate_storage_config = InMemoryStorage.Config()

localWorkerConfig = LocalWorker.Config(
    intermediate_storage_config=inmemory_intermediate_storage_config
)

dockerWorkerConfig = DockerWorker.Config(
    docker_gateway_address="http://localhost:5000",
    intermediate_storage_config=redis_intermediate_storage_config
)

# Define the workflow
products = [
    {"name": "Laptop", "original_price": 1000.0},
    {"name": "Phone", "original_price": 800.0},
    {"name": "Tablet", "original_price": 600.0},
] * 10

discount_rate = 0.1  # 10% discount

# Fan-out: Calculate discounts for all products
discounted_prices = [calculate_discount(product["original_price"], discount_rate) for product in products]

# Fan-in: Aggregate results to calculate total revenue and average price
total_revenue = calculate_total_revenue(discounted_prices)

# total_revenue.visualize_dag()
start_time = time.time()
result = total_revenue.compute(config=localWorkerConfig)
print(f"Total Revenue: ${result} | Makespan: {time.time() - start_time}s")
result2 = total_revenue.compute(config=localWorkerConfig)
print(f"Total Revenue: ${result2} | Makespan: {time.time() - start_time}s")

# result2 = total_revenue.compute(local=True)
# print(f"Total Revenue: ${result2}")