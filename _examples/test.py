import os
import sys
import time
import numpy as np
import json

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.dag_task_node import DAGTask, ExecutorType

@DAGTask
def calculate_discount(original_price: float, discount_rate: float) -> float:
    time.sleep(3)
    """Calculate the discounted price."""
    return original_price * (1 - discount_rate)

@DAGTask
def calculate_total_revenue(prices: list[float]) -> float:
    """Calculate the total revenue from a list of prices."""
    return sum(prices)

# Define the workflow
products = [
    {"name": "Laptop", "original_price": 1000.0},
    {"name": "Phone", "original_price": 800.0},
    {"name": "Tablet", "original_price": 600.0},
] * 50

discount_rate = 0.1  # 10% discount
tax_rate = 0.07  # 7% tax

# Fan-out: Calculate discounts for all products
discounted_prices = [calculate_discount(product["original_price"], discount_rate) for product in products]

# Fan-out: Apply taxes to all discounted prices
# final_prices = [apply_tax(price, tax_rate) for price in discounted_prices]

# Fan-out: Generate invoices for all products
# invoices = [generate_invoice(product["name"], final_price) for product, final_price in zip(products, final_prices)]

# Fan-in: Aggregate results to calculate total revenue and average price
total_revenue = calculate_total_revenue(discounted_prices)

# total_revenue.visualize_dag()
start_time = time.time()
result = total_revenue.compute(executorType=ExecutorType.REMOTE_DOCKER)
print(f"Total Revenue: ${result} | Makespan: {time.time() - start_time}s")
# result2 = total_revenue.compute(executorType=ExecutorType.REMOTE_DOCKER)
# print(f"Total Revenue: ${result2} | Makespan: {time.time() - start_time}s")

# result2 = total_revenue.compute(local=True)
# print(f"Total Revenue: ${result2}")