from lstore.index import Index
import time
import struct
import threading
from queue import Queue 
from lstore.page import Page, PageRange

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN = 4

MAX_BASE_PAGES = 16
METADATA_COLUMNS = 5 # indirection, rid, time, schema, baserid
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
        self.empty_schema = '0' * self.num_columns #replaces repeated '0' * self.num_cols during inserts
        self.page_directory = {} # key: column, RID --> value: page_range, page, page_offset
        self.page_ranges = []
        self.index = Index(self)
        self.merge_threshold_pages = 50  # The threshold to trigger a merge
        self.merge_set = [] # holds tail records until 50 records, then add queue
        self.merge_queue = Queue() # used by merge thread
        self.deallocation_queue = Queue() # deallocate everything here

        self.RID_counter = 0 # counter for assigning RIDs
        self.page_ranges.append(PageRange(self.total_columns)) # create initial page range

        self.page_directory_lock = threading.Lock()
        self.merge_set_lock = threading.Lock()

        # Create background thread for merge
        thread = threading.Thread(target=self.merge)
        thread.daemon = True # want to wait for finish 
        thread.start()


        
    """
    # insert an entirely new record. this goes into a base page
    # columns: an array of the columns with values we want to insert. does not include the 4 metadata columns so we need to calculate those ourselves
    """
    def insert_new_record(self, columns):
        page_ranges = self.page_ranges
        total_columns = self.total_columns
        empty_schema = self.empty_schema #call to reduce repititions
        page_directory = self.page_directory
        page_range = page_ranges[-1] # the page range we want to write to is the last one in the array
        
        # initialize an array with the complete list of data values to insert (metadata values + the record's values)
        values = [0] * METADATA_COLUMNS
        values[INDIRECTION_COLUMN] = 0 # not needed but included for clarity
        rid = self.getNewRID()
        values[RID_COLUMN] = rid
        values[TIMESTAMP_COLUMN] = time.time()
        values[SCHEMA_ENCODING_COLUMN] = empty_schema
        values[BASE_RID_COLUMN] = None
        values += columns

        
        base_pages = page_range.base_pages
        base_idx = page_range.basePageToWrite
        if not base_pages[base_idx][0].has_capacity(ENTRY_SIZE): # len(values[i]) is future proofing lol -DH
            base_idx += 1 
            page_range.basePageToWrite = base_idx
       
        # if the page range is full, then allocate a new page range
            if base_idx >= MAX_BASE_PAGES:
                page_ranges.append(PageRange(total_columns))
                page_range = page_ranges[-1]
                base_pages = page_range.base_pages
                base_idx = page_range.basePageToWrite

        # ---- THIS NEEDS TO BE REVISITED as all milestones have all columns as 64 bit integers, no strings or anything
        # can safely write the entire base record into the base page of the selected page range
        
        page_offsets = [None] * total_columns # save the page offsets for each column for later
        
        for i in range(total_columns):
            page_offsets[i] = base_pages[base_idx][i].page_size
            base_pages[base_idx][i].write(values[i])
        # ----------------------------------------------------------------------

        # add the values to the index. for now just index the primary key
        #for i in range(self.num_columns):
        #    self.index.insert_record(values[RID_COLUMN], values[i + METADATA_COLUMNS], i)
        # -> for loop will take  O(n) time so longer needed, 
        #    since we just want to index primary key dont need the whole for loop so O(1) time
        self.index.insert_record(values[RID_COLUMN], values[self.key + METADATA_COLUMNS], self.key)

        # add the mapping to the page directory
        page_range_index = len(page_ranges)-1
        for i in range(total_columns):
            # the page range index is always the same.
            # the page is always the same since a record is aligned across the base page thus requiring them all to be written on the same page index in their columns
            # use the page offsets that were saved earlier
            page_directory[(i, rid)] = (page_range_index, base_idx, page_offsets[i])
        
        return True
    
    """
    # update a record. append a tail page, update indirection columns as needed
    # columns: an array of the columns with values we want the record to be updated to. does not include the 4 metadata columns so we need to calculate those ourselves
    """
    def update_record(self, primary_key, columns):
        if columns[self.key] is not None: # if we're trying to update the key value, which is not allowed
            return False
        RIDs = self.index.locate(self.key, primary_key) 
        if len(RIDs) == 0: # record does not exist
            return False 
        # if nothing just return early
        if not any(i != None for i in columns):
            return True
        # if the record exists there should only be one item in the list because primary keys are unique
        baseRID = RIDs[0]
        read = self.read
        replace = self.replace
        page_directory = self.page_directory
        page_ranges = self.page_ranges
        total_columns = self.total_columns

        # read the indirection value's location in the base pages
        # we do this because we need to access the old version of the record and also link up the new tail page with the old tail page pointer


        # initialize an array with the complete list of data values to insert (metadata values + the record's values)
        values = [None] * METADATA_COLUMNS
        values[INDIRECTION_COLUMN] = None # not needed but included for clarity
        values[RID_COLUMN] = self.getNewRID()
        values[TIMESTAMP_COLUMN] = time.time()
        values[BASE_RID_COLUMN] = baseRID

        # set the schema encoding bits
        # ensure schema_encoding has length equal to the table's number of columns
        #calls the same str multiple times each add on:
        #-> will just need to add to list rather than reproduce and add
        schema_encoding = []
        num_cols_update = len(columns)
        for i in range(self.num_columns):
            # if the caller provided a value for this column and it's not None, mark as updated
            if i < num_cols_update and (columns[i] is not None):
                schema_encoding.append('1')
            # column not being updated
            else:
                schema_encoding.append('0')
        #join with '' since cant leave schema_encoding in list form, must be string
        values[SCHEMA_ENCODING_COLUMN] = ''.join(schema_encoding)

        base_schema = read(SCHEMA_ENCODING_COLUMN, baseRID)
        #base_schema = base_schema.decode()

        # --------- check if first time update, if so, insert that tail record -----------
       
        #edit: bring vars from loop//removes duplicated reads in loop
        new_indirection = read(INDIRECTION_COLUMN, baseRID)
        page_range_index = page_directory.get((RID_COLUMN, baseRID))[0] # uses base RID, RID_COLUMN as a random column to access the page range index of the base record
        page_range = page_ranges[page_range_index]
        
        first_update_cols = []
        for i in range(num_cols_update):
            if base_schema[i] == '0' and columns[i] != None: # check if this column has ever been updated before, and that we are trying to update it
                first_update_cols.append(i)

        if first_update_cols:
            first_update = [None] * (METADATA_COLUMNS + len(columns))
            first_update[RID_COLUMN] = self.getNewRID()
            first_update[BASE_RID_COLUMN] = baseRID

            if new_indirection == None or new_indirection == 0: # base has no updates yet
                first_update[INDIRECTION_COLUMN] = baseRID # (1) base rid becomes tail's indirection value
            else: # base already had an update
                first_update[INDIRECTION_COLUMN] = new_indirection  # (1) whatever was in the indirection column of base goes into first_updates indirection
            replace(baseRID, INDIRECTION_COLUMN, first_update[RID_COLUMN]) # (2) update base to newest tail
            
            # first_update_schema must reflect all columns (length = num_columns)
            #-> to not have it as a string and skip a for loop
            first_update_schema = ['0'] * self.num_columns
            for i in first_update_cols:
                first_update_schema[i] = "1"
            #convert to string so there isnt an error
            first_update[SCHEMA_ENCODING_COLUMN] = ''.join(first_update_schema)

            first_update[TIMESTAMP_COLUMN] = read(TIMESTAMP_COLUMN, baseRID)
            for i in first_update_cols:
                first_update[i + METADATA_COLUMNS] = read(i + METADATA_COLUMNS, baseRID)

            # insert this first update into the tail page
            
            last_tail_page = len(page_range.tail_pages) - 1

            # check for space; tail pages are aligned, so one column check is sufficient
            if not page_range.tail_pages[last_tail_page][0].has_capacity(8): # len(values[i]) is future proofing  -DH
                page_range.allocate_new_tail_page() 
                # FIXED: tail_page should be tail_pages
                last_tail_page = len(page_range.tail_pages) - 1 # using new tail page
            
            # can safely write the entire tail record into the tail page of the selected page range
            page_offsets = [None] * total_columns # save the page offsets for each column for later
            for j in range(total_columns):
                page_offsets[j] = page_range.tail_pages[last_tail_page][j].page_size # done so since page sizes might differ due to None values
                page_range.tail_pages[last_tail_page][j].write(first_update[j]) #write record to the last tail page
           
            # add the mapping to the page directory
            for j in range(total_columns):
                        # the page range index is the one our base record being updated is in
                        # the page is the first available tail page, so the last one 
                        # use the page offsets that were saved earlier 
                        # add MAX_BASE_PAGES offset to distinguish tail pages from base pages
                page_directory[(j, first_update[RID_COLUMN])] = (page_range_index, last_tail_page + MAX_BASE_PAGES, page_offsets[j])

        # change base record's schema encoding value
        #-> change to list from str for consistency        
        base_schema_encoding = []
        for i in range(num_cols_update):
#           checks if new update updates x column or previous updates have previously done so             
            if schema_encoding[i] == '1' or base_schema[i] == '1':
                base_schema_encoding.append('1')
            else:
                base_schema_encoding.append('0')
        replace(baseRID, SCHEMA_ENCODING_COLUMN, ''.join(base_schema_encoding))



        # ready the tail record with new values 
        values += columns
        #---- adding actual update ----------------------------
        
        #new_indirection == current_base_indirection so it is repetitive to reread:
        #->just use new_indirection rather than repeat call for current_base_indirection
        if new_indirection == None or new_indirection == 0:
            values[INDIRECTION_COLUMN] = baseRID  # Point to base if no previous updates
        else:
            values[INDIRECTION_COLUMN] = new_indirection  # Point to previous tail
        

        # update base to point to this new tail record
        replace(baseRID, INDIRECTION_COLUMN, values[RID_COLUMN])
        
        # get the page range from the base page
        page_range_index = page_directory.get((RID_COLUMN, baseRID))[0] # uses base RID, RID_COLUMN as a random column to access the page range index of the base record
        page_range = page_ranges[page_range_index]
        last_tail_page = len(page_range.tail_pages) - 1
        
        
        # check if the last tail page fully has room for the record (aligned pages)
        if not page_range.tail_pages[last_tail_page][0].has_capacity(8): # len(values[i]) is future proofing  -DH
            page_range.allocate_new_tail_page() 
            last_tail_page = len(page_range.tail_pages) - 1 # using new tail page

        # can safely write the entire tail record into the tail page of the selected page range
        page_offsets = [None] * total_columns # save the page offsets for each column for later
        for i in range(total_columns):
            page_offsets[i] = page_range.tail_pages[last_tail_page][i].page_size # done so since page sizes might differ due to None values
            page_range.tail_pages[last_tail_page][i].write(values[i]) #write record to the last tail page       
        
        # add the values to the index. for now just index the primary key, no secondary keys right now
        # self.index.insert_record(values[RID_COLUMN], values[self.key], self.key)

        # add the mapping to the page directory
        for i in range(total_columns):
            # the page range index is the one our base record being updated is in
            # the page is the first available tail page, so the last one 
            # use the page offsets that were saved earlier 
            # add MAX_BASE_PAGES offset to distinguish tail pages from base pages
            page_directory[(i, values[RID_COLUMN])] = (page_range_index, last_tail_page + MAX_BASE_PAGES, page_offsets[i])

        #------------------------------------
        # add to merge
        self.merge_set_lock.acquire()
        self.merge_set.append(Record(values[RID_COLUMN], values[self.key], values))
        if (len(self.merge_set) >= self.merge_threshold_pages):
            self.merge_queue.put(self.merge_set)
        self.merge_set_lock.release()

        return True

    def delete_record(self, primary_key):
        index = self.index
        key_col = self.key
        read = self.read
        replace = self.replace
        RIDs = index.locate(key_col, primary_key) 
        if len(RIDs) == 0: # record does not exist
            return False 
        baseRID = RIDs[0]
        record_to_delete = read(INDIRECTION_COLUMN, baseRID) # get the newest tail
        
        while 1:
            # check for both 0 and None since we write 0 for "no indirection"
            if record_to_delete == None or record_to_delete == 0: # if no tail records
                break
            next_record = read(INDIRECTION_COLUMN, record_to_delete) # save the next record down the pointer stream
            rid_value = read(RID_COLUMN, record_to_delete)
            replace(record_to_delete, RID_COLUMN, rid_value * -1) # multiply by -1 to mark for deletion
            record_to_delete = next_record # move onto the next record
            if next_record == baseRID: # if we reach base record
                break
        replace(baseRID, RID_COLUMN, baseRID * -1) # mark base record for death.

        return True

    # replaces value in specified column and RID
    def replace(self, RID, column_for_replace, value): 
        #save time calling these
        self.page_directory_lock.acquire()
        location = self.page_directory.get((column_for_replace, RID))
        self.page_directory_lock.release()
        page_range_index = location[0]
        page_index = location[1]
        page_offset = location[2]
        
        page_range = self.page_ranges[page_range_index]
        # Check if this is a base page (page_index < MAX_BASE_PAGES) or tail page
        if page_index < MAX_BASE_PAGES:
            page_range.base_pages[page_index][column_for_replace].replace(value, page_offset)
        else:
            # For tail pages, need to adjust the index
            tail_page_index = page_index - MAX_BASE_PAGES #if page_index >= MAX_BASE_PAGES else page_index//repetitive
            page_range.tail_pages[tail_page_index][column_for_replace].replace(value, page_offset)

    # passes in column and RID desired, gets address of page range, base page, page offset, returns value 
    def read(self, column_to_read, RID):
        #save time calling these
        self.page_directory_lock.acquire()
        page_directory = self.page_directory
        page_ranges = self.page_ranges
        location = page_directory.get((column_to_read, RID))
        
        if location == None:
            self.page_directory_lock.release()
            return None
        
        page_range_index = location[0]
        page_index = location[1]
        page_offset = location[2]
        page_range = page_ranges[page_range_index]
        
        if page_index < MAX_BASE_PAGES:
            read_value = page_range.base_pages[page_index][column_to_read].read(page_offset)
        else:
            tail_page_index = page_index - MAX_BASE_PAGES #if page_index >= MAX_BASE_PAGES else page_index//repetitive
            read_value = page_range.tail_pages[tail_page_index][column_to_read].read(page_offset)
        self.page_directory_lock.release()
        #if it isnt there just return now
        if read_value == None:

            return None
        #reduce unneeded assignem,ents    
        if column_to_read == SCHEMA_ENCODING_COLUMN:
            return read_value.decode()
        if column_to_read == TIMESTAMP_COLUMN:
            return struct.unpack('<d', read_value)[0]
        return int.from_bytes(read_value, byteorder='little')

    def getNewRID(self):
        RID = self.RID_counter
        self.RID_counter += 1
        return RID


    def merge(self):
        
        
        print("thread is starting")
        while (1):
            #print("MERGE START!!\n\n\n\n\n\n\n\n\n\n")
            
                # self.page_directory_lock.acquire()
                # print("merge is starting")
                batch_tail_records = self.merge_queue.get()
                self.merge_set = []
                batch_cons_page = self.getBasePageCopy(batch_tail_records) # this is 2D ARRAY base pages have MANY pages

                # for i in range(len(batch_cons_page)):
                #     self.decompress_page(batch_cons_page[i])
                    

                seenUpdates = {}

                # Find the newest updates
                for tail_record in reversed(batch_tail_records):
                    
                    # Figure out what columns to update
                    columns_to_update = []
                    for value in tail_record.columns:
                        if value != None:
                            columns_to_update.append(True)
                        else:
                            columns_to_update.append(False)

                    # Make sure that we get the latest values
                    base_RID = self.read(BASE_RID_COLUMN, tail_record.rid)

                    for i in range(len(columns_to_update)):
                        
                        if columns_to_update[i] is True and (base_RID, i) not in seenUpdates:
                            write_val = self.read(i + METADATA_COLUMNS, tail_record.rid)
                            seenUpdates.update({(base_RID, i) : write_val})
                
                # overwrite the base pages
                # not sure if this is the most efficient way tbh
                self.page_directory_lock.acquire()
                for base_page in batch_cons_page:


                    
                    new_base_page_info = base_page
                    new_base_page = new_base_page_info[0] # base page 
                    base_RID_to_update = new_base_page_info[1]
                    
                    for i in range(self.num_columns):
                        col_value = seenUpdates.get((base_RID_to_update, i))
                        # if it's None we don't need to do anything
                        if col_value != None:
                            
                            location = self.page_directory.get((i, base_RID_to_update))

                            page_offset = location[2]
                            new_base_page[i].replace(col_value, page_offset)  
                    # swap the page locations. NEEDS TO BE LOCKED
                while len(batch_cons_page) > 0:

                    new_base_page_info = batch_cons_page.pop()
                    new_base_page = new_base_page_info[0] # base page 
                    base_RID_to_update = new_base_page_info[1]

                    location = self.page_directory.get((0, base_RID_to_update))
                    page_range_index = location[0]
                    page_index = location[1]
                    page_range = self.page_ranges[page_range_index] 

                    
                    old_base_page = page_range.base_pages[page_index]
                    self.deallocation_queue.put(old_base_page)
                    page_range.base_pages[page_index] = new_base_page
                self.page_directory_lock.release()
                        
            #print("MERGE END!!\n\n\n\n\n\n\n\n\n\n")
           
           


    # @param [Record] tail_pages: list of tail records that aren't merged yet (THIS SHOULD BE LOOKED INTO AS L STORE PAPER ITSELF CONFLICTS ON TOPIC 4.1)
    # @return base_page_copy: 2d ARRAY of compressed (untranslated) base pages that need to be merged, no duplicates
    def getBasePageCopy(self, tail_records):

        # Create an empty set to fill 
        base_page_copy = []

        # Go through each tail record given 
        for tail_record in tail_records:

            # Get the base RID that we want to from the tail reocrd
            base_RID = self.read(BASE_RID_COLUMN, tail_record.rid)

            # Add the base page to the set, using INDIRECTION for placeholder

        #     location = self.page_directory.get((column_for_replace, RID))
        # page_range_index = location[0]
        # page_index = location[1]
        # page_offset = location[2]
        
        # page_range = self.page_ranges[page_range_index]

            self.page_directory_lock.acquire()
            location = self.page_directory.get((RID_COLUMN, base_RID))
            self.page_directory_lock.release()

            page_range_index = location[0]
            page_index = location[1]
            page_range = self.page_ranges[page_range_index]     
            
            if (page_range.base_pages[page_index], base_RID) not in base_page_copy:
                base_page_copy.append((page_range.base_pages[page_index], base_RID))
                #base_page_copy.add((page_range.base_pages[page_index], base_RID))
        

        return base_page_copy
        
        def decompress_page(self):
            pass

        def compress_page(self):
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
    
    """
        Efficiently retrieves multiple column values for any RID.
    """
    # @:param rid: the base RID of the record
    # @:param col_indices: list of user column indicies
    # @:param relative_version: 0 for latest, (-) for previous versions
    def get_values_by_rid(self, rid, col_indices, relative_version):
        """
        Efficiently retrieves multiple column values for any RID.

        :param rid: the base RID of the record
        :param col_indices: list of user column indicies
        :param relative_version: 0 for latest, (-) for previous versions
        """
        result =[]  # list of values in same order as col_indices
        read = self.read

        if relative_version == 0:
            # latest version, read base schema
            base_schema = read(SCHEMA_ENCODING_COLUMN, rid)
            indirection_rid = read(INDIRECTION_COLUMN, rid)    # read base indirection once to get latest values from newest tail record

            # for each requested column, decide whether we should read from base, or from latest tail record
            for col_idx in col_indices:
                physical_col_idx = col_idx + METADATA_COLUMNS
                # check if the column was ever updated
                if base_schema[col_idx] == '0':
                    # never updated, so read from the base
                    result.append(read(physical_col_idx, rid))
                # if the schema says it was updated, but no tail chain, continue to read from base
                elif indirection_rid is None or indirection_rid == 0:
                    # schema is updated but no tail exists, so read from base
                    result.append(read(physical_col_idx, rid))
                else:
                    # check if the latest tail has the column
                    tail_schema = read(SCHEMA_ENCODING_COLUMN, indirection_rid)
                    # if newest tail has the column, it becomes the latest value
                    if tail_schema[col_idx] == '1':
                        result.append(read(physical_col_idx, indirection_rid))
                    else:
                        # case where newest tail doesn't have the column, latest value stays from base record
                        result.append(read(physical_col_idx, rid))
        else:
            # using rabbit_hunt with the base_rid
            for col_idx in col_indices:
                result.append(self.rabbit_hunt(col_idx, 0, relative_version, base_rid = rid))   # primary_key is unused when base_rid is given, 0 is placeholder
        return result
