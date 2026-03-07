from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import DELETE, INSERT, SELECT, SELECT_VERSION, UPDATE, SUM, SUM_VERSION, INCREMENT, EXCLUSIVE, SHARED, INDEX, RECORD
import threading
import time, random

_transaction_counter = 0
_counter_lock = threading.Lock()

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        #this list will contain all the changes/writes that occured(no select or sum) so abort can retrieve original record
        self.changes = []
        self.lock_manager = None
        # lock_manager needs a unique transaction ID to track locks per transaction
        global _transaction_counter
        with _counter_lock:
            self.transaction_id = _transaction_counter
            _transaction_counter += 1
        pass


    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, table, *args):
        #add table as well so it can be used in abort
        self.queries.append((query, table, args))
        # use grades_table for aborting
        
        #store lock manager for commit/abort from table
        if self.lock_manager == None:
            self.lock_manager = table.lock_manager            

        
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        #need to reset it after each transaction run
        self.changes = []
        
        for query, table, args in self.queries:
            #want to have the latest record before updating/deleting so store here so it gets caught
            original_record = None

            #need the original record and use the helper in 
            if query.__name__ in {UPDATE, DELETE, INCREMENT}:
                primary = args[0]
                original_record = self._abort(table, primary, self.transaction_id)
                while original_record == False:
                    time.sleep(0.001 + random.random() * 0.005)  # tiny random delay
                    original_record = self._abort(table, primary, self.transaction_id)

            kwargs = {"transaction_id": self.transaction_id}
            result = query(*args, **kwargs)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            
            #update the changes list to contain the previous record in case of abort ONLY for write changes so None = insert query
            if query.__name__ in {UPDATE, DELETE, INCREMENT, INSERT}:
                self.changes.append((query, table, args, original_record))

        return self.commit()


    def abort(self):
        #do roll-back and any other necessary operations

        #rollback the changes that happened//stack (LIFO)
        while self.changes:
            #poping will give the popped item and remove from changes
            query, table, args, original_record = self.changes.pop()
            lock_manager = self.lock_manager
            indexed_cols = table.index.indexed_columns
            # upgrade the index and record locks to exclusive
            for col in indexed_cols:
                acquired = lock_manager.acquire(self.transaction_id, table.name, col, EXCLUSIVE, INDEX)
                while not acquired:
                    time.sleep(0.001 + random.random() * 0.005)  # tiny random delay
                    acquired = lock_manager.acquire(self.transaction_id, table.name, col, EXCLUSIVE, INDEX)

            RIDs = table.index.locate(table.key, primary)
            rid = RIDs[0]

            acquired = lock_manager.acquire(self.transaction_id, table.name, rid, EXCLUSIVE, RECORD)
            while not acquired:
                time.sleep(0.001 + random.random() * 0.005)  # tiny random delay
                acquired = lock_manager.acquire(self.transaction_id, table.name, rid, EXCLUSIVE, RECORD)

            #if insert (param columns)
            if query.__name__ == INSERT:
                primary = args[table.key]
                #delete the inserted record by deleting primary key
                table.delete_record(primary)
                continue

            #if delete params(primary key)
            if query.__name__ == DELETE:              
                #make sure theres no error or failed to delete so no duplicates
                if original_record != None:
                    #rid = table.getNewRID() #is it okay to get a new rid or does it have to be the old one?
                    table.insert_new_record(original_record, rid)
                    continue
           
            #if update params (primary key and columns)
            #convert orignal_record to update format by getting the parameters
            if query.__name__ == UPDATE:
                #safety check to make sure no errors that crash the code
                if original_record == None:
                    continue
                #the 0 in (ex: t.add_query(q.update, grades_table, 0, *[None, 1, None, 2, None])) is the primary index
                primary = args[0]  
                #now convert orignal records to columns
                columns = [None] * table.num_columns
                for i in range(table.num_columns):   
                    #dont overwrite the primary key
                    if i != table.key:
                        columns[i] = original_record[i]
                #then reupdate it to correct one//similar to how the query(*args) was but update format more explicit
                query(primary, *columns)
                continue

            #if increment param (primary key and column)
            if query.__name__ == INCREMENT:
                if original_record == None:
                    continue
                #param primary key and column
                primary = args[0] 
                column = args[1]  
                columns = [None] * table.num_columns
                #only changes one col so dont need for loop and i goes to column
                if column != table.key:
                    columns[column] = original_record[column]
                    #update will undo the added column and restore the old values that were there
                    query.__self__.update(primary, *columns) 
                continue

        #undo lock since this finishes transaction
        if self.lock_manager != None:
            self.lock_manager.release_all(self.transaction_id)

        return False
    
    # moved it from table to here because i felt like it made more sense (left it commented in table just in case)
    #helper func for transaction to get original records for update and delete by storing it with primary key:
    def _abort(self, table, primary, transaction_id):
        #aborted = False # need to try to abort until it works since records we want to reset can be

        #get the rid for the latest record
        lock_manager = self.lock_manager
        acquired = lock_manager.acquire(transaction_id, table.name, table.key, SHARED, INDEX)
        if not acquired:
            return False
        RIDs = table.index.locate(table.key, primary)

        #safety check // record does not exist
        if len(RIDs) == 0:
            return None 
        
        baseRID = RIDs[0]

        #get latest column so i can use get_values_by_rid to get the latest record
        latest_column = list(range(table.num_columns))
        latest_record = table.get_values_by_rid(baseRID, latest_column, 0, transaction_id=transaction_id)

        #return what is stored inside the record
        return latest_record


    def commit(self):
        #commit to database

        #clear changes since abort didnt happen
        self.changes = []

        #undo lock since this finishes transaction
        if self.lock_manager != None:
            self.lock_manager.release_all(self.transaction_id)

        return True

