
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Union, Optional, AsyncContextManager
from contextlib import asynccontextmanager
from enum import Enum

class BatchOperation:
    """Represents a single operation to be executed in a batch"""
    
    class Type(Enum):
        GET = "get"
        MGET = "mget"
        EXISTS = "exists"
        SET = "set"
        ATOMIC_INCREMENT_AND_GET = "atomic_increment_and_get"
        KEYS = "keys"
        PUBLISH = "publish"
        DELETE = "delete"
    
    def __init__(self, op_type: Type, args: tuple, kwargs, result_key: Optional[str] = None):
        self.op_type = op_type
        self.args = args
        self.kwargs = kwargs
        self.result_key = result_key  # Optional key to store result for later reference

class BatchContext:
    """Context for batching operations"""
    
    def __init__(self, storage: "Storage"):
        self._storage = storage
        self._operations: list[BatchOperation] = []
        self._results: dict[str, Any] = {}
    
    async def get(self, key: str, result_key: Optional[str] = None) -> "BatchContext":
        """Queue a get operation"""
        op = BatchOperation(BatchOperation.Type.GET, (key,), {}, result_key)
        self._operations.append(op)
        return self
    
    async def mget(self, keys: list[str], result_key: Optional[str] = None) -> "BatchContext":
        """Queue a mget operation"""
        op = BatchOperation(BatchOperation.Type.MGET, (keys,), {}, result_key)
        self._operations.append(op)
        return self
    
    async def exists(self, *keys: str, result_key: Optional[str] = None) -> "BatchContext":
        """Queue an exists operation"""
        op = BatchOperation(BatchOperation.Type.EXISTS, keys, {}, result_key)
        self._operations.append(op)
        return self
    
    async def set(self, key: str, value: Any, result_key: Optional[str] = None) -> "BatchContext":
        """Queue a set operation"""
        op = BatchOperation(BatchOperation.Type.SET, (key, value), {}, result_key)
        self._operations.append(op)
        return self
    
    async def atomic_increment_and_get(self, key: str, result_key: Optional[str] = None) -> "BatchContext":
        """Queue an atomic increment and get operation"""
        op = BatchOperation(BatchOperation.Type.ATOMIC_INCREMENT_AND_GET, (key,), {}, result_key)
        self._operations.append(op)
        return self
    
    async def keys(self, pattern: str, result_key: Optional[str] = None) -> "BatchContext":
        """Queue a keys operation"""
        op = BatchOperation(BatchOperation.Type.KEYS, (pattern,), {}, result_key)
        self._operations.append(op)
        return self
    
    async def publish(self, channel: str, message: Union[str, bytes], result_key: Optional[str] = None) -> "BatchContext":
        """Queue a publish operation"""
        op = BatchOperation(BatchOperation.Type.PUBLISH, (channel, message), {}, result_key)
        self._operations.append(op)
        return self
    
    async def delete(
        self, 
        key: str, 
        *, 
        pattern: bool = False, 
        prefix: bool = False, 
        suffix: bool = False, 
        result_key: Optional[str] = None
    ) -> "BatchContext":
        """
        Queue a delete operation.
        
        Args:
            key: The key pattern/prefix/suffix to match for deletion
            pattern: If True, treat key as a pattern to match (e.g., 'user:*')
            prefix: If True, delete all keys that start with the given key
            suffix: If True, delete all keys that end with the given key
            result_key: Optional key to store the result under
            
        Returns:
            Self for method chaining
            
        Note:
            Only one of pattern, prefix, or suffix should be True at a time.
            If none are True, performs an exact key match delete.
        """
        op = BatchOperation(
            BatchOperation.Type.DELETE, 
            (key,), 
            {"pattern": pattern, "prefix": prefix, "suffix": suffix}, 
            result_key
        )
        self._operations.append(op)
        return self
    
    def get_result(self, result_key: str) -> Any:
        """Get a result by its key after execution"""
        return self._results.get(result_key)
    
    def get_all_results(self) -> list[Any]:
        """Get all results in order of operations"""
        return [self._results.get(f"_op_{i}", None) for i in range(len(self._operations))]
    
    async def execute(self) -> list[Any]:
        """Execute all batched operations and return results"""
        results = await self._storage._execute_batch(self._operations)
        
        # Store results with their keys
        for i, (op, result) in enumerate(zip(self._operations, results)):
            # Store with custom key if provided
            if op.result_key:
                self._results[op.result_key] = result
            # Always store with index-based key for ordered access
            self._results[f"_op_{i}"] = result
        
        return results

class Storage(ABC):
    @dataclass
    class Config(ABC):
        @abstractmethod
        def create_instance(self) -> "Storage": pass

    @abstractmethod
    async def get(self, key: str) -> Any: pass
    
    @abstractmethod
    async def mget(self, keys: list[str]) -> list[Any]: pass
    
    @abstractmethod
    async def exists(self, *keys: str) -> Any: pass # returns number of keys that exist
    
    @abstractmethod
    async def set(self, key: str, value) -> Any: pass
    
    @abstractmethod
    async def atomic_increment_and_get(self, key: str) -> Any: pass
    
    @abstractmethod
    async def keys(self, pattern: str) -> list: pass

    # PUB/SUB
    @abstractmethod
    async def publish(self, channel: str, message: Union[str, bytes]) -> int: pass
    
    @abstractmethod
    async def subscribe(self, channel: str, callback: Callable[[dict, str], Any], decode_responses: bool = False, coroutine_tag: str = "", worker_id: str | None = None) -> str: pass
    
    @abstractmethod
    async def unsubscribe(self, channel: str, subscription_id: Optional[str]): pass

    @abstractmethod
    async def delete(self, key: str, *, pattern: bool = False, prefix: bool = False, suffix: bool = False) -> int:
        """
        Delete keys from the storage.
        
        Args:
            key: The key pattern/prefix/suffix to match for deletion
            pattern: If True, treat key as a pattern to match (e.g., 'user:*')
            prefix: If True, delete all keys that start with the given key
            suffix: If True, delete all keys that end with the given key
            
        Returns:
            int: Number of keys that were deleted
            
        Note:
            Only one of pattern, prefix, or suffix should be True at a time.
            If none are True, performs an exact key match delete.
        """
        pass
        
    @abstractmethod
    async def _execute_batch(self, operations: list[BatchOperation]) -> list[Any]:
        """Execute a batch of operations. Implementations should optimize this for their backend."""
        pass
    
    @asynccontextmanager
    async def batch(self) -> AsyncIterator[BatchContext]:
        """Create a batch context for queuing operations"""
        context = BatchContext(self)
        yield context