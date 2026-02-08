from lstore.index import Index
import time
import struct
from lstore.page import Page, PageRange

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

MAX_BASE_PAGES = 16
METADATA_COLUMNS = 4
ENTRY_SIZE = 8 # 8 bytes

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key # the value of the primary key of the record
        self.columns = columns # columns: a list of the values corresponding to each column
    
    def __getitem__(self, index):
        value = self.columns[index]
        
        #this lets the query not need a filler 0 in the list when there is None
        #prevents crash when value is None
        if value == None:
            return 0
        else: 
            return value

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns 
        self.total_columns = num_columns + METADATA_COLUMNS # + 4 for rid, indirection, schema, timestamping. these 4 columns are internal to table
        self.page_directory = {} # key: column, RID --> value: page_range, page, page_offset
        self.page_ranges = []
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge

        self.RID_counter = 0 # counter for assigning RIDs
        self.page_ranges.append(PageRange(self.total_columns)) # create initial page range


        
    """
    # insert an entirely new record. this goes into a base page
    # columns: an array of the columns with values we want to insert. does not include the 4 metadata columns so we need to calculate those ourselves
    """
    def insert_new_record(self, columns):
        
        page_range = self.page_ranges[len(self.page_ranges)-1] # the page range we want to write to is the last one in the array
        # initialize an array with the complete list of data values to insert (metadata values + the record's values)
        values = [0] * METADATA_COLUMNS
        values[INDIRECTION_COLUMN] = 0 # not needed but included for clarity
        values[RID_COLUMN] = self.getNewRID()
        values[TIMESTAMP_COLUMN] = time.time()
        values[SCHEMA_ENCODING_COLUMN] = '0' * self.num_columns
        values += columns

        for i in range(self.total_columns):
            # check if the base page fully has room for each column. if any of them don't, we need to move on to the next base page
            if not page_range.base_pages[page_range.basePageToWrite][i].has_capacity(8): # len(values[i]) is future proofing lol -DH
                page_range.basePageToWrite += 1 
                break
        # if the page range is full, then allocate a new page range
        if page_range.basePageToWrite >= MAX_BASE_PAGES:
            self.page_ranges.append(PageRange(self.total_columns))
            page_range = self.page_ranges[len(self.page_ranges)-1]

        # ---- THIS NEEDS TO BE REVISITED as all milestones have all columns as 64 bit integers, no strings or anything
        # can safely write the entire base record into the base page of the selected page range
        page_offsets = [None] * self.total_columns # save the page offsets for each column for later
        for i in range(self.total_columns):
            page_offsets[i] = page_range.base_pages[page_range.basePageToWrite][i].page_size
            page_range.base_pages[page_range.basePageToWrite][i].write(values[i])
        # ----------------------------------------------------------------------


        # add the values to the index. for now just index the primary key
        for i in range(self.num_columns):
            self.index.insert_record(values[RID_COLUMN], values[i + METADATA_COLUMNS], i)

        # add the mapping to the page directory
        page_range_index = len(self.page_ranges)-1
        for i in range(self.total_columns):
            # the page range index is always the same.
            # the page is always the same since a record is aligned across the base page thus requiring them all to be written on the same page index in their columns
            # use the page offsets that were saved earlier
            self.page_directory[(i, values[RID_COLUMN])] = (page_range_index, page_range.basePageToWrite, page_offsets[i])
        return True
    
    """
    # update a record. append a tail page, update indirection columns as needed
    # columns: an array of the columns with values we want the record to be updated to. does not include the 4 metadata columns so we need to calculate those ourselves
    """
    def update_record(self, primary_key, columns):
        RIDs = self.index.locate(self.key, primary_key) 
        if len(RIDs) == 0: # record does not exist
            return False 
        # if the record exists there should only be one item in the list because primary keys are unique
        baseRID = RIDs[0]

        # read the indirection value's location in the base pages
        # we do this because we need to access the old version of the record and also link up the new tail page with the old tail page pointer
        # indirection_pointer = self.read(INDIRECTION_COLUMN, baseRID) # latest tail record

        # initialize an array with the complete list of data values to insert (metadata values + the record's values)
        values = [None] * METADATA_COLUMNS
        values[INDIRECTION_COLUMN] = None # not needed but included for clarity
        values[RID_COLUMN] = self.getNewRID()
        values[TIMESTAMP_COLUMN] = time.time()

        # set the schema encoding bits
        # ensure schema_encoding has length equal to the table's number of columns
        schema_encoding = ""
        for i in range(self.num_columns):
            # if the caller provided a value for this column and it's not None, mark as updated
            if i < len(columns) and (columns[i] is not None):
                schema_encoding += '1'
            # column not being updated
            else:
                schema_encoding += '0'
        values[SCHEMA_ENCODING_COLUMN] = schema_encoding

        base_schema = self.read(SCHEMA_ENCODING_COLUMN, baseRID)
        #base_schema = base_schema.decode()

        # --------- check if first time update, if so, insert that tail record -----------
        
        for i in range(len(columns)):
            if base_schema[i] == '0' and columns[i] != None: # check if this column has ever been updated before, and that we are trying to update it
                first_update = [None] * (METADATA_COLUMNS + len(columns))
                first_update[RID_COLUMN] = self.getNewRID()

                new_indirection = self.read(INDIRECTION_COLUMN, baseRID)
                if new_indirection == None or new_indirection == 0: # base has no updates yet
                    first_update[INDIRECTION_COLUMN] = baseRID # (1) base rid becomes tail's indirection value
                else: # base already had an update
                    first_update[INDIRECTION_COLUMN] = new_indirection  # (1) whatever was in the indirection column of base goes into first_updates indirection
                self.replace(baseRID, INDIRECTION_COLUMN, first_update[RID_COLUMN]) # (2) update base to newest tail
                
                # first_update_schema must reflect all columns (length = num_columns)
                first_update_schema = ""
                for j in range(self.num_columns):
                    if j == i:
                        first_update_schema += "1"
                    else:
                        first_update_schema += "0"
                first_update[SCHEMA_ENCODING_COLUMN] = first_update_schema
                first_update[TIMESTAMP_COLUMN] = self.read(TIMESTAMP_COLUMN, baseRID)
                first_update[i + 4] = self.read(i + 4, baseRID)

                # insert this first update into the tail page
                
                page_range_index = self.page_directory.get((RID_COLUMN, baseRID))[0] # uses base RID, RID_COLUMN as a random column to access the page range index of the base record
                page_range = self.page_ranges[page_range_index]
                last_tail_page = len(page_range.tail_pages) - 1

                # check for space, might be redundant checking every column
                for j in range(self.total_columns):
                    # check if the last tail page fully has room for each column. if any of them don't, we need to allocate a new tail page
                    if not page_range.tail_pages[last_tail_page][j].has_capacity(8): # len(values[i]) is future proofing  -DH
                        page_range.allocate_new_tail_page() 
                        # FIXED: tail_page should be tail_pages
                        last_tail_page = len(page_range.tail_pages) - 1 # using new tail page
                        break
                
                # can safely write the entire tail record into the tail page of the selected page range
                page_offsets = [None] * self.total_columns # save the page offsets for each column for later
                for j in range(self.total_columns):
                    page_offsets[j] = page_range.tail_pages[last_tail_page][j].page_size # done so since page sizes might differ due to None values
                    page_range.tail_pages[last_tail_page][j].write(first_update[j]) #write record to the last tail page

                # add the mapping to the page directory
                page_range_index = self.page_ranges.index(page_range) # find the index of the page range we've been looking at
                for j in range(self.total_columns):
                    # the page range index is the one our base record being updated is in
                    # the page is the first available tail page, so the last one 
                    # use the page offsets that were saved earlier 
                    # add MAX_BASE_PAGES offset to distinguish tail pages from base pages
                    self.page_directory[(j, first_update[RID_COLUMN])] = (page_range_index, last_tail_page + MAX_BASE_PAGES, page_offsets[j])

        # change base record's schema encoding value
        base_schema_encoding = ""
        for i in range(len(columns)):
            if schema_encoding[i] == '1' or base_schema[i] == '1': # checks if new update updates x column or previous updates have previously done so 
                base_schema_encoding += '1'
            else:
                base_schema_encoding += '0'
                
        self.replace(baseRID, SCHEMA_ENCODING_COLUMN, base_schema_encoding)

        # ready the tail record with new values 
        values += columns

        # this is redundant, keeping here for reference

        # # this would be the first update to the record, so we need to 
        # # (1) change the base's indirection pointer from None to our RID 
        # # (2) set the new tail page's indirection to the base page's RID
        # if indirection_pointer == None:
        #     self.replace(baseRID, RID_COLUMN, values[RID_COLUMN])
        #     values[INDIRECTION_COLUMN] = baseRID # (2)
        # # if this isn't the first update, then
        # # (1) set the new tail page's indirection to what was previously contained in the base page
        # # (2) update the base page's indirection to point to the new tail page's RID
        # else:
        #     values[INDIRECTION_COLUMN] = indirection_pointer # (1)
        #     self.replace(baseRID, RID_COLUMN, values[RID_COLUMN]) # (2)

        # # inserting the actual newest tail record 
        # # check each column. if it is the first update of that column, we need to insert a tail record that copies the original data
        # # no matter what, we always have to insert the actual new tail record
        


        #---- adding actual update ----------------------------

        current_base_indirection = self.read(INDIRECTION_COLUMN, baseRID)
        if current_base_indirection == None or current_base_indirection == 0:
            values[INDIRECTION_COLUMN] = baseRID  # Point to base if no previous updates
        else:
            values[INDIRECTION_COLUMN] = current_base_indirection  # Point to previous tail
        
        # update base to point to this new tail record
        self.replace(baseRID, INDIRECTION_COLUMN, values[RID_COLUMN])
        
        # get the page range from the base page
        page_range_index = self.page_directory.get((RID_COLUMN, baseRID))[0] # uses base RID, RID_COLUMN as a random column to access the page range index of the base record
        page_range = self.page_ranges[page_range_index]
        last_tail_page = len(page_range.tail_pages) - 1
        for i in range(self.total_columns):
            # check if the last tail page fully has room for each column. if any of them don't, we need to allocate a new tail page
            if not page_range.tail_pages[last_tail_page][i].has_capacity(8): # len(values[i]) is future proofing  -DH
                page_range.allocate_new_tail_page() 
                last_tail_page = len(page_range.tail_pages) - 1 # using new tail page
                break
        
        # can safely write the entire tail record into the tail page of the selected page range
        page_offsets = [None] * self.total_columns # save the page offsets for each column for later
        for i in range(self.total_columns):
            page_offsets[i] = page_range.tail_pages[last_tail_page][i].page_size # done so since page sizes might differ due to None values
            page_range.tail_pages[last_tail_page][i].write(values[i]) #write record to the last tail page       

        # add the values to the index. for now just index the primary key, no secondary keys right now
        # self.index.insert_record(values[RID_COLUMN], values[self.key], self.key)

        # add the mapping to the page directory
        page_range_index = self.page_ranges.index(page_range) # find the index of the page range we've been looking at
        for i in range(self.total_columns):
            # the page range index is the one our base record being updated is in
            # the page is the first available tail page, so the last one 
            # use the page offsets that were saved earlier 
            # add MAX_BASE_PAGES offset to distinguish tail pages from base pages
            self.page_directory[(i, values[RID_COLUMN])] = (page_range_index, last_tail_page + MAX_BASE_PAGES, page_offsets[i])
        #------------------------------------

        return True

    def delete_record(self, primary_key):
        RIDs = self.index.locate(self.key, primary_key) 
        if len(RIDs) == 0: # record does not exist
            return False 
        baseRID = RIDs[0]
        record_to_delete = self.read(INDIRECTION_COLUMN, baseRID) # get the newest tail
        
        while 1:
            # check for both 0 and None since we write 0 for "no indirection"
            if record_to_delete == None or record_to_delete == 0: # if no tail records
                break
            next_record = self.read(INDIRECTION_COLUMN, record_to_delete) # save the next record down the pointer stream
            self.replace(record_to_delete, RID_COLUMN, self.read(RID_COLUMN, record_to_delete) * -1) # multiply by -1 to mark for deletion
            record_to_delete = next_record # move onto the next record
            if next_record == baseRID: # if we reach base record
                break
        self.replace(baseRID, RID_COLUMN, baseRID * -1) # mark base record for death.

        return True



    # replaces value in specified column and RID
    def replace(self, RID, column_for_replace, value): 
        location = self.page_directory.get((column_for_replace, RID))
        page_range_index = location[0]
        page_index = location[1]
        page_offset = location[2]
        
        
        page_range = self.page_ranges[page_range_index]
        # Check if this is a base page (page_index < MAX_BASE_PAGES) or tail page
        if page_index < MAX_BASE_PAGES:
            page_range.base_pages[page_index][column_for_replace].replace(value, page_offset)
        else:
            # For tail pages, need to adjust the index
            tail_page_index = page_index - MAX_BASE_PAGES if page_index >= MAX_BASE_PAGES else page_index
            page_range.tail_pages[tail_page_index][column_for_replace].replace(value, page_offset)

    # passes in column and RID desired, gets address of page range, base page, page offset, returns value 
    def read(self, column_to_read, RID):
        location = self.page_directory.get((column_to_read, RID))
        if location is None:
            return None
        page_range_index = location[0]
        page_index = location[1]
        page_offset = location[2]
        
        page_range = self.page_ranges[page_range_index]
        if page_index < MAX_BASE_PAGES:
            read_value = page_range.base_pages[page_index][column_to_read].read(page_offset)
        else:
            tail_page_index = page_index - MAX_BASE_PAGES if page_index >= MAX_BASE_PAGES else page_index
            read_value = page_range.tail_pages[tail_page_index][column_to_read].read(page_offset)
        
        # convert bytes to int for all columns except SCHEMA_ENCODING_COLUMN
        # if read_value is not None and column_to_read != SCHEMA_ENCODING_COLUMN:
        #     read_value = int.from_bytes(read_value, byteorder='little')

        if read_value is not None and column_to_read == SCHEMA_ENCODING_COLUMN:
            read_value = read_value.decode()
        elif read_value is not None and column_to_read == TIMESTAMP_COLUMN:
            read_value = struct.unpack('<d', read_value)[0]
        elif read_value is not None:
            read_value = int.from_bytes(read_value, byteorder='little')

        
        return read_value

    def getNewRID(self):
        RID = self.RID_counter
        self.RID_counter += 1
        return RID


    def __merge(self):
        print("merge is happening")
        pass

    """
    Searches through tail records and returns the contents of a given column that match
    the given primary key in the given version.
    """
    # @param int col_idx: index of column 
    # @param int primary_key: value of primary key to match
    # @param int version_num: version number to match, where 0 is latest

    # @return col_contents: int value at column that matches primary key
    def rabbit_hunt(self, col_idx, primary_key, version_num, base_rid = None):
        version_num *= 1
        physical_col_idx = col_idx + METADATA_COLUMNS

        # if not given base RID, find base record using primary_key index
        if base_rid is None:
            RIDs = self.index.locate(self.key, primary_key) # locate all RIDs that match primary key in index
            if len(RIDs) == 0:  # if index returns nothing, record doesn't exist
                return None
            baseRID = RIDs[0]   # so then first RID treated as base RID for this record's version
        else:   # if know base RID, skip index lookup
            baseRID = base_rid

        # check base record's schema encoding to see if columns were ever updated
        base_schema = self.read(SCHEMA_ENCODING_COLUMN, baseRID)
        if base_schema[col_idx] == '0':
            # column never updated, so return base value b/c we know it is correct
            return self.read(physical_col_idx, baseRID)
        
        indirection_RID = self.read(INDIRECTION_COLUMN, baseRID)
        # check for both 0 and None since we write 0 for "no indirection"
        if indirection_RID == None or indirection_RID == 0: # base rid becomes tail's indirection value if None or 0
            indirection_RID = baseRID

        count = 0
        while baseRID != indirection_RID:

            schema = self.read(SCHEMA_ENCODING_COLUMN, indirection_RID)
            # schema may be stored as fixed-size bytes, decode and ensure it's at least num_columns long
            #schema = schema.decode()
            if len(schema) < self.num_columns:
                schema = schema + ('0' * (self.num_columns - len(schema)))
            if schema[col_idx] == '1': # if value @col_idx is present
                if count == version_num: # at version number, return
                    col_contents = self.read(physical_col_idx, indirection_RID)
                    return col_contents
                count += 1
                
            # iterate
            indirection_RID = self.read(INDIRECTION_COLUMN, indirection_RID)

        # catch all, return base record
        col_contents = self.read(physical_col_idx, baseRID)
        return col_contents
    
    def get_values_by_rid(self, rid, col_indices, relative_version):
        """
        Efficiently retrieves multiple column values for any RID.

        :param rid: the base RID of the record
        :param col_indices: list of user column indicies
        :param relative_version: 0 for latest, (-) for previous versions
        """
        result =[]  # list of values in same order as col_indices

        if relative_version == 0:
            # latest version, read base schema
            base_schema = self.read(SCHEMA_ENCODING_COLUMN, rid)
            indirection_rid = self.read(INDIRECTION_COLUMN, rid)    # read base indirection once to get latest values from newest tail record

            # for each requested column, decide whether we should read from base, or from latest tail record
            for col_idx in col_indices:
                physical_col_idx = col_idx + METADATA_COLUMNS
                # check if the column was ever updated
                if base_schema[col_idx] == '0':
                    # never updated, so read from the base
                    result.append(self.read(physical_col_idx, rid))
                # if the schema says it was updated, but no tail chain, continue to read from base
                elif indirection_rid is None or indirection_rid == 0:
                    # schema is updated but no tail exists, so read from base
                    result.append(self.read(physical_col_idx, rid))
                else:
                    # check if the latest tail has the column
                    tail_schema = self.read(SCHEMA_ENCODING_COLUMN, indirection_rid)
                    # if newest tail has the column, it becomes the latest value
                    if tail_schema[col_idx] == '1':
                        result.append(self.read(physical_col_idx, indirection_rid))
                    else:
                        # case where newest tail doesn't have the column, latest value stays from base record
                        result.append(self.read(physical_col_idx, rid))
        else:
            # using rabbit_hunt with the base_rid
            for col_idx in col_indices:
                result.append(self.rabbit_hunt(col_idx, 0, relative_version, base_rid = rid))   # primary_key is unused when base_rid is given, 0 is placeholder
        return result