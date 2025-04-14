import time


class Timer:
    def __init__(self):
        self.start_time = time.perf_counter()

    def stop(self):
        """ returns in milliseconds """
        total_time = (time.perf_counter() - self.start_time) * 1000
        return total_time
