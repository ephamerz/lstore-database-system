import threading
from lstore.config import SHARED, EXCLUSIVE

"""
Lock manager to manage locks for RECORDS in the database.
N/A to Bufferpool, Index, Page Directory
"""
class LockManager:

    def __init__(self):
        self.lock_dict = {}
        self.lock = threading.Lock()

    """
    Call from Query. Acquire lock for the given transaction on the given table and RID.

    :param transaction_id: int
    :param table_name: string
    :param rid: int
    :param lock_type: int
    """
    def acquire(self, transaction_id, table_name, rid, lock_type):
        key = (table_name, rid) # RIDs are only unique within a table

        with self.lock:
            entry = self.lock_dict.get(key)

            # if no lock exists, give them a lock
            if not entry: 
                self.lock_dict[key] = {
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

    :param transaction_id: int
    """
    def release_all(self, transaction_id):
        with self.lock:
            # find every record this transaction holds a lock on
            keys_to_clean = [
                key for key, entry in self.lock_dict.items()
                if transaction_id in entry['holders']
            ]
            for key in keys_to_clean:
                entry = self.lock_dict.get(key)
                #if entry is None: # case shouldn't be possible as LockManger is locked in use
                #    continue

                entry["holders"].discard(transaction_id)

                if not entry["holders"]:
                    del self.lock_dict[key]