import threading
import time

class RedisDataStore:
    def __init__(self):
        self.data = {}
        self.blocking_conditions = {}
        self.lock = threading.Lock()

    def get_item(self, key):
        with self.lock:
            item = self.data.get(key)
            if not item:
                return None
            if item[0] == 'string':
                _value, expiry_ms = item[1]
                if expiry_ms is not None and int(time.time() * 1000) > expiry_ms:
                    del self.data[key]
                    return None
            return item

    def set_item(self, key, value):
        with self.lock:
            self.data[key] = value
    
    def get_condition_for_key(self, key):
        with self.lock:
            if key not in self.blocking_conditions:
                self.blocking_conditions[key] = threading.Condition(self.lock)
            return self.blocking_conditions[key]

    def notify_waiters(self, key, notify_all=False):
        with self.lock:
            if key in self.blocking_conditions:
                condition = self.blocking_conditions[key]
                if notify_all:
                    condition.notify_all()
                else:
                    condition.notify()
                if not getattr(condition, '_waiters', True):
                    del self.blocking_conditions[key]