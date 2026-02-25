from lstore.table import Table, Record
from lstore.index import Index
from lstore.config import METADATA_COLUMNS


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """
    def __init__(self, table):
        self.table = table
        pass

    
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, primary_key):
        
        return self.table.delete_record(primary_key)
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # #variables
        table = self.table
        index = table.index
        key_column = table.key
        RIDs = index.locate(key_column, columns[key_column]) 
        if len(RIDs) >= 1:
             return False

        return table.insert_new_record(columns)     
        
        

    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, search_key, search_key_index, projected_columns_index):
        #introduce some sort of rab bit hunting through the tail records, 
            # as well as checking what values we have gathered already
        #rid key map
        # get one RID
        # get RID of base record, then access indirection and get tail record, 
        #get specified column data we want

        rids = self.table.index.locate(search_key_index, search_key)
        if len(rids) == 0:
            return []
        
        
        cols = [i for i, v in enumerate(projected_columns_index) if v == 1]

        #edit to just call get_values_by_rid cause fixing this in multiple places is a pain
        records = []
        for rid in rids:
            # get record(s) for the rid, projected cols, and version 0 
            vals = self.table.get_values_by_rid(rid, cols, 0)
            if vals == []:
                continue
            
            if search_key_index in cols:
                key_val = vals[cols.index(search_key_index)]
            else:
                key_val = self.table.get_values_by_rid(rid, [search_key_index], 0)[0]

            if key_val != search_key:
                continue
            
            
            # if the column is 0 in projected columns, we put None
            if len(vals) < len(projected_columns_index):
                full_vals = [None] * self.table.num_columns
                for i, col in enumerate(cols):
                    full_vals[col] = vals[i]
                vals = full_vals

            records.append(Record(rid, key_val, vals))
        return records


    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        #introduce some sort of rab bit hunting through the tail records, 
            # as well as checking what values we have gathered already
        #rid key map
        # get one RID
        # get RID of base record, then access indirection and get tail record, 
            # get specified column data we want
        rids = self.table.index.locate(search_key_index, search_key)
        if len(rids) == 0:
            return []
        
        cols = [i for i, v in enumerate(projected_columns_index) if v == 1]

        records = []
        for rid in rids:
            # get record(s) for the rid, projected cols, and version 0 
            vals = self.table.get_values_by_rid(rid, cols, relative_version)
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
    def update(self, primary_key, *columns):
        if columns[self.table.key] is not None:
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
    def sum(self, start_range, end_range, aggregate_column_index):
        try:
            # use the table's index to find all base RIDs with primary key in the range
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            # if no records, fails
            if len(rids) == 0:
                return False
                
            # initializing running total count for aggregation
            total = 0

            #edit to call get_values_by_rid rather than processing all here to not fix the tail issues multiple places
            #this will get the newest value in the col for every record and add it to the total
            for i in rids:
                #get the latest value for rid
                values = self.table.get_values_by_rid(i, [aggregate_column_index], 0)
                #if theres nothing there break out since its useless
                '''
                if len(values) == 0:
                    continue
                '''
                #edited here
                if not isinstance(values, list) or len(values) == 0:
                    continue
                #just want the aggregate_col value
                target_value = values[0]

                # only add the value to the total if value is actually obtained//prevent a type error if there is a None value
                if target_value != None:
                    total += target_value
            
            return total
        except:
            return False
       


    
    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    :param relative_version: the relative version of the record you need to retreive.
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
            # use the table's index to find all base RIDs with primary key in the range
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            # if no records fall in range
            if len(rids) == 0:
                return False
            
            total = 0   # initializing running total count for aggregation result

            # for each base RID returned in the range
            for rid in rids:
                # read primary key value directly from base
                primary_key = self.table.read(self.table.key + METADATA_COLUMNS, rid)  # +4 for metadata
                # retrieve the value of aggregate column at requested relative version through rabbit hunt
                value = self.table.rabbit_hunt(aggregate_column_index, primary_key, relative_version, base_rid = rid)
                # add to total if valud value was returned
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
    def increment(self, key, column):
        result = self.select(key, self.table.key, [1] * self.table.num_columns)
        if result is False or len(result) == 0:
            return False
        r = result[0]
        if r is not False:
            # creating an update list
            updated_columns = [None] * self.table.num_columns

            # incrementing the specific column
            updated_columns[column] = r[column] + 1

            # returning and applying the updated columns
            return self.update(key, *updated_columns)
        return False