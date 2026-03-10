from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import METADATA_COLUMNS, BASE_RID_COLUMN, INDEX, RECORD, SHARED, EXCLUSIVE, DELETE, INSERT, SELECT, SELECT_VERSION, UPDATE, SUM, SUM_VERSION, INCREMENT, LOGICAL_ERROR


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        self.lock_manager = table.lock_manager
        pass

    def lock_indexes(self, transaction_id, lock_type):
        lock_manager = self.lock_manager
        table = self.table
        indexed_cols = table.index.indexed_columns
        for col in indexed_cols:
            acquired = lock_manager.acquire(transaction_id, table.name, col, lock_type, INDEX)
            if not acquired:
                return False
        return True

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key, transaction_id=None):
        lock_manager = self.lock_manager
        table = self.table

        # lock indexes and check it first too
        if not self.lock_indexes(transaction_id, EXCLUSIVE):
            return False

        RIDs = self.table.index.locate(self.table.key, primary_key) 
        if len(RIDs) == 0:
            if transaction_id is not None:
                return LOGICAL_ERROR
            return False
        
        # lock record
        acquired = lock_manager.acquire(transaction_id, table.name, RIDs[0], EXCLUSIVE, RECORD)
        if not acquired:
            return False

        return table.delete_record(primary_key)
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns, transaction_id=None):
        # #variables
        table = self.table
        index = table.index
        key_column = table.key
        lock_manager = self.lock_manager
        rid = table.getNewRID()

        # lock indexes and check it first too
        if not self.lock_indexes(transaction_id, EXCLUSIVE):
            return False
        RIDs = index.locate(key_column, columns[key_column]) 
        if len(RIDs) >= 1:
            # DINGDINGDING
            # if we find a record with the same primary key, we fail the insert and fail the transaction entirely
            if transaction_id is not None:
                # we need to be able to distinguish logic errors (trying to update primary key)
                # DINGDINGDING
                return LOGICAL_ERROR
            return False
        # lock the record
        acquired = lock_manager.acquire(transaction_id, table.name, rid, EXCLUSIVE, RECORD)
        if not acquired:
            return False

        return table.insert_new_record(columns, rid)     
        
        

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index, transaction_id=None):
        return self.select_version(search_key, search_key_index, projected_columns_index, 0, transaction_id)

    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version, transaction_id=None):
        #introduce some sort of rab bit hunting through the tail records, 
            # as well as checking what values we have gathered already
        #rid key map
        # get one RID
        # get RID of base record, then access indirection and get tail record, 
            # get specified column data we want
        lock_manager = self.lock_manager
        table = self.table
        records = []
        cols = [i for i, v in enumerate(projected_columns_index) if v == 1]

    
        # lock only search key index
        acquired = lock_manager.acquire(transaction_id, table.name, search_key_index, SHARED, INDEX)
        if not acquired:
            return False

        # check if the column is indexed. if not, we have to scan the table manually instead
        if self.table.index.indices[search_key_index] == None:
            # copy matching RIDs while holding the directory lock briefly, then release it
            # before record-level reads/locks to avoid lock leaks and lock-order deadlocks
            with table.page_directory_lock:
                candidate_rids = [
                    rid for (column, rid) in self.table.page_directory.keys()
                    if column == search_key_index
                ]

            for rid in candidate_rids:
                # get a shared lock on the rid
                acquired = lock_manager.acquire(transaction_id, table.name, rid, SHARED, RECORD)
                if not acquired:
                    return False

                baseRID = self.table.read(BASE_RID_COLUMN, rid)
                vals = self.table.get_values_by_rid(baseRID, cols, relative_version, transaction_id=transaction_id)
                if vals == False: # got blocked by a lock
                    return False
                value = vals[search_key_index]
                # if after manually scanning and reading it's actually the correct value we're searching by, then add it to the records list
                if value == search_key:
                    key_val = self.table.read(self.table.key, rid)
                    if vals == []:
                        continue
                    # if the column is 0 in projected columns, we put None
                    if len(vals) < len(projected_columns_index):
                        full_vals = [None] * self.table.num_columns
                        for i, col in enumerate(cols):
                            full_vals[col] = vals[i]
                        vals = full_vals
                    records.append(Record(rid, key_val, vals))
            return records

        # --- if we get here then the column is indexed ---
        rids = self.table.index.locate(search_key_index, search_key)
        if len(rids) == 0:
            return []

        for rid in rids:
            # get a shared lock on the rid
            acquired = lock_manager.acquire(transaction_id, table.name, rid, SHARED, RECORD)
            if not acquired:
                return False

            # get record(s) for the rid, projected cols, and version 0 
            vals = self.table.get_values_by_rid(rid, cols, relative_version, transaction_id=transaction_id)
            if vals == False:
                return False # got blocked by a lock
            if vals == []:
                continue
            # if the column is 0 in projected columns, we put None
            if len(vals) < len(projected_columns_index):
                full_vals = [None] * self.table.num_columns
                for i, col in enumerate(cols):
                    full_vals[col] = vals[i]
                vals = full_vals
            records.append(Record(rid, vals[0], vals))

        return records



#############



    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns, transaction_id=None):
        if columns[self.table.key] is not None:
            # illegal operation: primary key updates are not allowed.
            if transaction_id is not None:
                return LOGICAL_ERROR
            return False
        
        lock_manager = self.lock_manager
        table = self.table
        index = table.index

        # lock indexes and check it first too
        if not self.lock_indexes(transaction_id, EXCLUSIVE):
            return False
        RIDs = index.locate(table.key, primary_key) 
        if len(RIDs) == 0:
            return False
        rid = RIDs[0]
        # lock the record
        acquired = lock_manager.acquire(transaction_id, table.name, rid, EXCLUSIVE, RECORD)
        if not acquired:
            return False

        return self.table.update_record(primary_key, columns)

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, aggregate_column_index, transaction_id=None):
        return self.sum_version(start_range, end_range, aggregate_column_index, 0, transaction_id=transaction_id)
        # lock_manager = self.lock_manager
        # table = self.table
        # # lock primary key index
        # lock_manager.acquire(transaction_id, table.name, table.key, SHARED, INDEX)

        # try:
        #     # use the table's index to find all base RIDs with primary key in the range
        #     rids = self.table.index.locate_range(start_range, end_range, self.table.key)
        #     # if no records, fails
        #     if len(rids) == 0:
        #         return False
                
        #     # initializing running total count for aggregation
        #     total = 0

        #     #this will get the newest value in the col for every record and add it to the total
        #     for i in rids:
                
        #         #get the latest value for rid
        #         # no need to lock any records, all the shared locks will be acquired in get_values_by_rid
        #         values = self.table.get_values_by_rid(i, [aggregate_column_index], 0)

        #         #checks here to catch any errors
        #         if not isinstance(values, list) or len(values) == 0:
        #             continue

        #         #just want the aggregate_col value
        #         target_value = values[0]

        #         # only add the value to the total if value is actually obtained//prevent a type error if there is a None value
        #         if target_value != None:
        #             total += target_value
            
        #     return total
        # except:
        #     return False
       

    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version, transaction_id=None):
        lock_manager = self.lock_manager
        table = self.table
        # lock primary key index
        acquired = lock_manager.acquire(transaction_id, table.name, table.key, SHARED, INDEX)
        if not acquired:
            return False
        try:
            # use the table's index to find all base RIDs with primary key in the range
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            # if no records fall in range
            if len(rids) == 0:
                return False
            
            total = 0   # initializing running total count for aggregation result

            # for each base RID returned in the range
            for rid in rids:
                # lock rid
                acquired = lock_manager.acquire(transaction_id, table.name, rid, SHARED, RECORD)
                if not acquired:
                    return False

                # read primary key value directly from base
                primary_key = self.table.read(self.table.key + METADATA_COLUMNS, rid)  # +4 for metadata
                # retrieve the value of aggregate column at requested relative version through rabbit hunt
                # no need to lock any records for this part, all the shared locks will be acquired in rabbit_hunt
                value = self.table.rabbit_hunt(aggregate_column_index, primary_key, relative_version, base_rid = rid, transaction_id=transaction_id)
                if value == False:
                    return False # got blocked by a lock
                # add to total if valid value was returned
                if value is not None:
                    total += value

            return total
        except:
            return False


    
    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column, transaction_id=None):
        result = self.select(key, self.table.key, [1] * self.table.num_columns, transaction_id=transaction_id)
        if result is False or len(result) == 0:
            return False
        r = result[0]
        if r is not False:
            # creating an update list
            updated_columns = [None] * self.table.num_columns

            # incrementing the specific column
            updated_columns[column] = r[column] + 1

            # returning and applying the updated columns
            return self.update(key, *updated_columns, transaction_id=transaction_id)
        return False
    


