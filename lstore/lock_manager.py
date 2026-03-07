import threading
from lstore.config import SHARED, EXCLUSIVE, RECORD, INDEX

"""
Lock manager to manage locks for RECORDS in the database.
N/A to Bufferpool, Index, Page Directory
"""
class LockManager:

    def __init__(self):
        self.record_lock_dict = {} # locks for records
        self.index_lock_dict = {} # locks for indexes
        self.record_lock = threading.Lock() # lock for using the record dictionary
        self.index_lock = threading.Lock() # lock for using the index dictionary

        # we should always obtain locks from order of first to last (record then index)
        # we should always release locks from order of last to first (index then record)
        self.dict_list = [RECORD, INDEX] # the types of dictionaries we have. never modified, only used for easy iteration


    """
    Call from Query. Acquire lock for the given transaction on the given table and RID or index.

    :param transaction_id: int
    :param table_name: string
    :param id: int that lets us identify the index or RID we want to lock. it will either be the RID, or the index's column.
    :param lock_type: int
    :param object_to_lock: string that tells us if we're locking an index or a record
    """
    def acquire(self, transaction_id, table_name, id, lock_type, object_to_lock):
        key = (table_name, id) # RIDs/index cols are only unique within a table
        # set the lock dictionary for the appropriate object to lock
        lock_dict = self.record_lock_dict
        lock = self.record_lock
        if object_to_lock == INDEX:
            lock_dict = self.index_lock_dict
            lock = self.index_lock
        with lock:
            
            entry = lock_dict.get(key)

            # if no lock exists, give them a lock
            if not entry: 
                lock_dict[key] = {
                    "lock_type": lock_type,         # either shared, or exclusive
                    "holders": {transaction_id}     # set of transaction IDs
                }
                return True
            
            current_type = entry["lock_type"]
            holders = entry["holders"]

            # if the transaction requesting the lock already has a lock on the record
            if transaction_id in holders:
                if current_type == EXCLUSIVE:                           # already good since everything exclusive
                    return True
                
                if current_type == SHARED and lock_type == SHARED:      # currently shared and resquesting shared
                    return True
                
                if current_type == SHARED and lock_type == EXCLUSIVE:   # S -> X only if it is the holder
                    if holders == {transaction_id}:
                        entry["lock_type"] = EXCLUSIVE
                        return True
                    else:
                        # simply cant upgrade since others hold S
                        return False
            
            # transaction is not in holders
            if lock_type == SHARED:
                if current_type == SHARED:
                    holders.add(transaction_id)
                    return True
                return False        # if the current is exclusive elsewhere
            return False


    """
    Call from Transaction. Release all locks for the given transaction.

    release all the locks for all types of objects (indexes, records, etc.) for a transaction, using the release_all_helper
    :param transaction_id: int
    """
    def release_all(self, transaction_id):
        # LOCKS ARE ACQUIRED IN ORDER OF THE LIST, SO THEY MUST BE RELEASED IN REVERSE ORDER
        for object in reversed(self.dict_list):
            self.release_all_helper(transaction_id, object)

    """
    helper for releasing locks for specific objects, called internally

    :param transaction_id: int
    :param object_to_release: string telling us whether it was an index or record that we're releasing locks for.
    """
    def release_all_helper(self, transaction_id, object_to_release):
        lock_dict = self.record_lock_dict
        lock = self.record_lock
        if object_to_release == INDEX:
            lock_dict = self.index_lock_dict
            lock = self.index_lock
        with lock:
            # find every record/index this transaction holds a lock on
            keys_to_clean = [
                key for key, entry in lock_dict.items()
                if transaction_id in entry['holders']
            ]
            for key in keys_to_clean:
                entry = lock_dict.get(key)
                #if entry is None: # case shouldn't be possible as LockManger is locked in use
                #    continue

                entry["holders"].discard(transaction_id)

                if not entry["holders"]:
                    del lock_dict[key]