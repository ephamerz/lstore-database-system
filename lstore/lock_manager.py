import threading

class LockManager:

    def __init__(self):
        self.locks_dict = {}
        self.dict_lock = threading.Lock() # the lock for the dictionary. name is a little confusing
        # suggest alternative if you have

    """
    
    """
    def acquire(self):
        pass

    """
    
    """
    def release_all(self):
        pass