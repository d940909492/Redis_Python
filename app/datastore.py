import threading
import time

class RedisDataStore:
    def __init__(self):
        self.data = {}
        self.blocking_conditions = {}
        self.lock = threading.RLock()

    def get_item(self, key):
        item = self.data.get(key)
        if not item:
            return None
        if item[0] == 'string':
            _value, expiry_ms = item[1]
            if expiry_ms is not None and int(time.time() * 1000) > expiry_ms:
                if key in self.data: del self.data[key]
                return None
        return item

    def set_item(self, key, value):
        self.data[key] = value

    def get_condition_for_key(self, key):
        with self.lock:
            if key not in self.blocking_conditions:
                self.blocking_conditions[key] = threading.Condition(self.lock)
            return self.blocking_conditions[key]

    def notify_waiters(self, key):
        with self.lock:
            if key in self.blocking_conditions:
                condition = self.blocking_conditions[key]
                condition.notify()
                if not getattr(condition, '_waiters', True):
                    del self.blocking_conditions[key]
                    