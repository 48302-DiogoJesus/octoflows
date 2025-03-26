import dask 
from dask.delayed import delayed
import operator
import time

L = range(512)
while len(L) > 1:
  L = list(map(delayed(operator.add), L[0::2], L[1::2]))

start_time = time.time()
print(f"DAG Nodes: {len(L[0].dask)}")
result = L[0].compute()
print(f"{time.time() - start_time}s Result: {type(result)} {result}") 
