import time


class Timer:
    is_stopped = False

    def __init__(self):
        self.start_time = time.perf_counter()

    def stop(self):
        """ returns in milliseconds """
        if self.is_stopped: raise Exception("StopWatch is already stopped")
        total_time = (time.perf_counter() - self.start_time) * 1000
        self.is_stopped = True
        return total_time
