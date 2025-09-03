import asyncio

class AtomicFlag:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._flag_set = False
    
    async def set_if_not_set(self):
        async with self._lock:
            if not self._flag_set:
                self._flag_set = True
                return True
            return False
    
    async def clear(self):
        async with self._lock:
            self._flag_set = False
    
    async def is_set(self):
        async with self._lock:
            return self._flag_set