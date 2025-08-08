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
                value, expiry = item[1]
                if expiry is not None and time.time() > expiry:
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
                
                if not condition._waiters:
                    del self.blocking_conditions[key]