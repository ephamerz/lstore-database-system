from lstore.table import Table, Record
from lstore.index import Index

LATEST_VERSION = 0

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
        # #read a record
        # #use index to locate rid
        # rid = self.table.index.locate( self.table.key, primary_key)
        
        # #if NA skip rest of the steps
        # if rid == None:
        #     return False

        # try:
        #     #address = self.table.page_directory[rid]
        #     #delete address
        #     del self.table.page_directory[rid]
        #     #delete primary key
        #     del self.table.index[primary_key]
        #     return True
        
        # #if locked
        # except: 
        #     return False
        return self.table.delete_record(primary_key)
    
    
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # #variables
        # schema_encoding = '0' * self.table.num_columns
        # primary_key = columns[self.table.key]
        # rid = len(self.table.page_directory)
        RIDs = self.table.index.locate(self.table.key, columns[self.table.key]) 
        if len(RIDs) >= 1:
             return False

        
        # try: 
        #     #insert address to directory to get to the columns
        #     self.table.page_directory[rid] = columns # tuple of column and RID
        #     #insert for rid key mapping
        #     self.table.index[primary_key] = rid
        #     return True
        
        # except:
        #     return False  
        return self.table.insert_new_record(columns)     
        
        

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
            #  get specified column data we want
        rids = self.table.index.locate(search_key_index, search_key)
        
        #no needed since select wont call key that DNE
        #if len(rids) == 0:
        #    return False
        
        rid = rids[0]

        cols = [i for i, v in enumerate(projected_columns_index) if v == 1]
        vals = self.table.get_values_by_rid(rid, cols, 0)   # 0 = latest

        return [Record(rid, search_key, vals)]


    """
    # Read matching record with specified search key
    # :param search_key: the value you want to search based on
    # :param search_key_index: the column index you want to search based on
    # :param projected_columns_index: what columns to return. array of 1 or 0 values.
    # :param relative_version: the relative version of the record you need to retreive.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        #introduce some sort of rab bit hunting through the tail records, 
            # as well as checking what values we have gathered already
        #rid key map
        # get one RID
        # get RID of base record, then access indirection and get tail record, 
            # get specified column data we want
        rids = self.table.index.locate(search_key_index, search_key)
        
        #not needed since select will never call key that DNE
        #if len(rids) == 0:
        #    return False
        
        rid = rids[0]

        cols = [i for i, v in enumerate(projected_columns_index) if v == 1]
        vals = self.table.get_values_by_rid(rid, cols, relative_version)  

        return [Record(rid, search_key, vals)]

#############



    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, primary_key, *columns):
        # # locating our record using the primary key, index to locate faster
        # rid = self.table.index.locate(self.table.key, primary_key)

        # # if the record doesn't exist, the update doesn't go through
        # if rid is None:
        #     return False
        
        # try:
        #     # reading the current record directly
        #     record = self.table.page_directory[rid]

        #     # starting the new version as a copy of the most recent
        #     new_record = list(record)

        #     # updating column by column
        #     for i in range(len(columns)):
        #         if columns[i] is not None:
        #             # to not change the primary key
        #             if i == self.table.key and columns[i] != primary_key:
        #                 return False
        #             new_record[i] = columns[i]

        #     # appending the newest version, but not overwriting the old ones
        #     self.table.page_directory[rid] = tuple(new_record)

        #     return True
        # except:
        #     return False
        # pass

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
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            if len(rids) == 0:
                return False
            
            total = 0
            physical_col_idx = aggregate_column_index + 4 # for metadata columns

            # processing each RID directly
            for rid in rids:
                # reading base schema to check if the column was ever updated
                base_schema = self.table.read(3, rid) # for SCHEMA_ENCODING_COLUMN
                # if column never updated, read from base directly
                if base_schema[aggregate_column_index] == '0':
                    value = self.table.read(physical_col_idx, rid)
                    if value is not None:
                        total += value
                else:
                    # column has updates, check tail
                    indirection_rid = self.table.read(0, rid)   # for INDIRECTION_COLUMN
                    if indirection_rid is None or indirection_rid == 0:
                        value = self.table.read(physical_col_idx, rid)
                    else:
                        # check if the latest tail has the column
                        tail_schema = self.table.read(3, indirection_rid)
                        if tail_schema[aggregate_column_index] == '1':
                            value = self.table.read(physical_col_idx, indirection_rid)
                        else:
                            value = self.table.read(physical_col_idx, rid)

                if value is not None:
                    total += value

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
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)

            if len(rids) == 0:
                return False
            
            total = 0
            for rid in rids:
                value = self.table.rabbit_hunt(aggregate_column_index, None, relative_version, base_rid = rid)
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
