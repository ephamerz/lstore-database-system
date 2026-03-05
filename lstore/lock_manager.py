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
        with self.lock:
            key = (table_name, rid) # RIDs are only unique within a table
            if key not in self.lock_dict: # no lock held
                self.lock_dict[key] = {
                    'lock_type': lock_type,
                    'holders': {transaction_id}
                }
            else: # lock currently held
                # incomplete: compatibility check

                # cases to consider:
                # any type, held by same transaction : grant, return True (no self blocking)
                # S requested, S held by others : add to holders, return True
                # S requested, X held : return False
                # X requested, S held : return False
                # X requested, X held : return False
                pass

            return True

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
                self.lock_dict[key]['holders'].discard(transaction_id)
                # clean up empty entries
                if not self.lock_dict[key]['holders']:
                    del self.lock_dict[key]