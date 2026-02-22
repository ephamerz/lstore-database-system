from lstore.index import Index
import time
import struct
import threading
from queue import Queue 
from lstore.page import Page, PageRange
from lstore.config import INDIRECTION_COLUMN, RID_COLUMN, TIMESTAMP_COLUMN, SCHEMA_ENCODING_COLUMN, BASE_RID_COLUMN, MAX_BASE_PAGES, METADATA_COLUMNS, ENTRY_SIZE
import pickle
import os

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
    :param bufferpool:          #lets table use bufferpool
    :param disk_manager:        #lets table use disk_manager
    """
    def __init__(self, name, num_columns, key, bufferpool, disk_manager):
        self.name = name
        self.key = key
        self.num_columns = num_columns 
        self.total_columns = num_columns + METADATA_COLUMNS # + 4 for rid, indirection, schema, timestamping. these 4 columns are internal to table
        self.empty_schema = '0' * self.num_columns #replaces repeated '0' * self.num_cols during inserts
        self.page_directory = {} # key: column, RID --> value: page_range, page, page_offset
        self.page_ranges = []
        self.index = Index(self)
        self.bufferpool = bufferpool
        self.disk_manager = disk_manager
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


    #helper funcs for integrating buffer and disk into table:

    #tuple format used in bufferpool and disk manager so convert table info to it
        #page_key: (table, page_range, is_tail, page_idx, col_idx)
    #@page_range_index: which page range this page is part of
    #@table_page_index: table's page slot; its tail has offset(tail_page_index = last_tail_page + MAX_BASE_PAGES)
    #@column_index: which col pg in the pg slot
    def _page_key(self, page_range_index, table_page_index, column_index):
        #convert pg index into base or tail boolean
        is_tail = table_page_index >= MAX_BASE_PAGES
        
        #if tail then need do tail numbering rather than base numbering since theyre diff
        if is_tail:
            page_index = table_page_index - MAX_BASE_PAGES
        else:
            page_index = table_page_index
        #return page_key
        return (self.name, page_range_index, is_tail, page_index, column_index)


    #uses parameters to find  where to access and gets the page object for that location
    def _get_page(self, page_range_index, page_index, column_index):
        #gets page_key from table in the tuple format w helper
        page_key = self._page_key(page_range_index, page_index, column_index)
        #return page and page_key since it will help simplify code for managing dirty and unpinning
        return self.bufferpool.get_page(page_key), page_key


    #read from page and unpin after done w read transaction
    #param: page_offset is the page offset in the page where the cell value is//used in page_directory
    def _read_page(self, page_range_index, page_index, column_index, page_offset):
        #will do pinning through this route(w get_page) and get the page we are checking
        page, page_key = self._get_page(page_range_index, page_index, column_index)
        #w page from buffer, read the specific info wanted from it w page.read
        value = page.read(page_offset)
        #dont need page anymore so unpin
        self.bufferpool.unpin(page_key)
        #return the cell value from the page
        return value


    #write/append in a page and return the page_offset so the page_directory knows where to find it
    def _write_page(self, page_range_index, page_index, column_index, value):
        #will do pinning through this route(w get_page) and get the page we are checking
        page, page_key = self._get_page(page_range_index, page_index, column_index)
        #current size before the append to know where to access after
        page_offset = page.page_size
        #append page with the value at the end of the original page 
        page.write(value)
        
        #since updated need to mark dirty now and then unpin:
        self.bufferpool.mark_dirty(page_key)
        self.bufferpool.unpin(page_key)
        #for page_directory since it needs the start position of value in order to find it
        return page_offset


    #deals with updating a page for the update_record so it goes through buffer and does the needed pinning and marking dirty
    def _update_page(self, page_range_index, page_index, column_index, value, page_offset):
        #will do pinning through this route(w get_page) and get the page we are checking
        page, page_key = self._get_page(page_range_index, page_index, column_index)
        #go to page_offset in the targetted page and update the old value with this new one
        page.replace(value, page_offset)

        #once done with the update:
        self.bufferpool.mark_dirty(page_key)
        self.bufferpool.unpin(page_key)


    #whening checking has_capacity it goes through page which is now supposed to be dealt w buffer 
        # so adding this to make it know that a page is being accessed and pinning and unpinning accordingly
    def _has_capacity(self, page_range_index, page_index, column_index, size):
        #will do pinning through this route(w get_page) and get the page we are checking
        page, page_key = self._get_page(page_range_index, page_index, column_index)
        #returns a boolean that will be true if there is capacity and false if there isnt
        capacity = page.has_capacity(size)
        #done w checking
        self.bufferpool.unpin(page_key)
        return capacity

  
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

        #add so that buffer can use this
        page_range_index = len(page_ranges) - 1
        #base_pages = page_range.base_pages // no longer needed bc of the helper
        base_idx = page_range.basePageToWrite

        #adjusted base_pages[base_idx][0].has_capacity(ENTRY_SIZE) to use the table to buffer helper
        if not self._has_capacity(page_range_index,base_idx, 0, ENTRY_SIZE): # len(values[i]) is future proofing lol -DH
            base_idx += 1 
            page_range.basePageToWrite = base_idx
       
        # if the page range is full, then allocate a new page range
            if base_idx >= MAX_BASE_PAGES:
                page_ranges.append(PageRange(total_columns))
                page_range = page_ranges[-1]
                #base_pages = page_range.base_pages // not needed
                base_idx = page_range.basePageToWrite
                #add so that pg_range_index is consistent w the changes in page_range so that page_directory and helper arent diff
                page_range_index = len(page_ranges) -1 

        # ---- THIS NEEDS TO BE REVISITED as all milestones have all columns as 64 bit integers, no strings or anything
        # can safely write the entire base record into the base page of the selected page range
        
        page_offsets = [None] * total_columns # save the page offsets for each column for later

        for i in range(total_columns):
        #    page_offsets[i] = base_pages[base_idx][i].page_size
        #    base_pages[base_idx][i].write(values[i])
        #change so that it goes through helper to deal with the pinning and marking instead of using .write
            page_offsets[i] = self._write_page(page_range_index, base_idx, i, values[i])


        # ----------------------------------------------------------------------

        # add the values to the index. for now just index the primary key
        #for i in range(self.num_columns):
        #    self.index.insert_record(values[RID_COLUMN], values[i + METADATA_COLUMNS], i)
        # -> for loop will take  O(n) time so longer needed, 
        #    since we just want to index primary key dont need the whole for loop so O(1) time
        for i in range(self.num_columns):
            self.index.insert_record(values[RID_COLUMN], values[i + METADATA_COLUMNS], i)

        # add the mapping to the page directory
        page_range_index = len(page_ranges)-1
        self.page_directory_lock.acquire()
        for i in range(total_columns):
            # the page range index is always the same.
            # the page is always the same since a record is aligned across the base page thus requiring them all to be written on the same page index in their columns
            # use the page offsets that were saved earlier
            page_directory[(i, rid)] = (page_range_index, base_idx, page_offsets[i])
        self.page_directory_lock.release()
        
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
        self.page_directory_lock.acquire()
        page_range_index = page_directory.get((RID_COLUMN, baseRID))[0] # uses base RID, RID_COLUMN as a random column to access the page range index of the base record
        self.page_directory_lock.release()
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
            #add for table to buffer helper parameter
            page_index = last_tail_page + MAX_BASE_PAGES

            # check for space; tail pages are aligned, so one column check is sufficient
            if not self._has_capacity(page_range_index, page_index, 0, 8): #page_range.tail_pages[last_tail_page][0].has_capacity(8): # len(values[i]) is future proofing  -DH
                page_range.allocate_new_tail_page() 
                # FIXED: tail_page should be tail_pages
                last_tail_page = len(page_range.tail_pages) - 1 # using new tail page
                #update page_index with the new last_tail_page
                page_index = last_tail_page + MAX_BASE_PAGES

            # can safely write the entire tail record into the tail page of the selected page range
            page_offsets = [None] * total_columns # save the page offsets for each column for later
            for j in range(total_columns):
                page_offsets[j] = self._write_page(page_range_index, page_index, j, first_update[j]) #page_range.tail_pages[last_tail_page][j].page_size # done so since page sizes might differ due to None values
                #removed: page_range.tail_pages[last_tail_page][j].write(first_update[j]) #write record to the last tail page
           
            # add the mapping to the page directory
            self.page_directory_lock.acquire()
            for j in range(total_columns):
                        # the page range index is the one our base record being updated is in
                        # the page is the first available tail page, so the last one 
                        # use the page offsets that were saved earlier 
                        # add MAX_BASE_PAGES offset to distinguish tail pages from base pages
                page_directory[(j, first_update[RID_COLUMN])] = (page_range_index, page_index, page_offsets[j]) #last_tail_page + MAX_BASE_PAGES
            self.page_directory_lock.release()

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
        self.page_directory_lock.acquire()
        page_range_index = page_directory.get((RID_COLUMN, baseRID))[0] # uses base RID, RID_COLUMN as a random column to access the page range index of the base record
        self.page_directory_lock.release()
        page_range = page_ranges[page_range_index]
        last_tail_page = len(page_range.tail_pages) - 1
        #for helper
        page_index = last_tail_page + MAX_BASE_PAGES
        
        
        # check if the last tail page fully has room for the record (aligned pages) //use has capacity helper for buffer use
        if not self._has_capacity(page_range_index, page_index, 0, 8): #page_range.tail_pages[last_tail_page][0].has_capacity(8): # len(values[i]) is future proofing  -DH
            page_range.allocate_new_tail_page() 
            last_tail_page = len(page_range.tail_pages) - 1 # using new tail page
            page_index = last_tail_page + MAX_BASE_PAGES #need to update this as well

        # can safely write the entire tail record into the tail page of the selected page range
        page_offsets = [None] * total_columns # save the page offsets for each column for later
        for i in range(total_columns):
            #routed to helper instead
            page_offsets[i] = self._write_page(page_range_index, page_index, i, values[i]) #page_range.tail_pages[last_tail_page][i].page_size # done so since page sizes might differ due to None values
            #removed: page_range.tail_pages[last_tail_page][i].write(values[i]) #write record to the last tail page       
        
        # add the values to the index. for now just index the primary key, no secondary keys right now
        # self.index.insert_record(values[RID_COLUMN], values[self.key], self.key)

        # add the mapping to the page directory
        self.page_directory_lock.acquire()
        for i in range(total_columns):
            # the page range index is the one our base record being updated is in
            # the page is the first available tail page, so the last one 
            # use the page offsets that were saved earlier 
            # add MAX_BASE_PAGES offset to distinguish tail pages from base pages
            page_directory[(i, values[RID_COLUMN])] = (page_range_index, page_index, page_offsets[i]) #last_tail_page + MAX_BASE_PAGES
        self.page_directory_lock.release()

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
        
       #page_range = self.page_ranges[page_range_index]
        # Check if this is a base page (page_index < MAX_BASE_PAGES) or tail page
        #if page_index < MAX_BASE_PAGES:
        #    page_range.base_pages[page_index][column_for_replace].replace(value, page_offset)
        #else:
            # For tail pages, need to adjust the index
         #   tail_page_index = page_index - MAX_BASE_PAGES #if page_index >= MAX_BASE_PAGES else page_index//repetitive
        #    page_range.tail_pages[tail_page_index][column_for_replace].replace(value, page_offset)
        
        #replace above with helper so buffer involved// base or tail done w page_key var is_tail 
        self._update_page(page_range_index,page_index, column_for_replace, value, page_offset)

        

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
        
        #if page_index < MAX_BASE_PAGES:
        #    read_value = page_range.base_pages[page_index][column_to_read].read(page_offset)
        #else:
        #    tail_page_index = page_index - MAX_BASE_PAGES #if page_index >= MAX_BASE_PAGES else page_index//repetitive
        #    read_value = page_range.tail_pages[tail_page_index][column_to_read].read(page_offset)
        self.page_directory_lock.release()
        #isntead of above commened out code, route w helper so buffer funcs included
        read_value = self._read_page(page_range_index, page_index, column_to_read, page_offset)

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
            if (self.merge_queue.qsize()) > 0:
                # print("merge is starting")
                batch_tail_records = self.merge_queue.get()
                self.merge_set_lock.acquire()
                self.merge_set = []
                self.merge_set_lock.release()
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
                            seenUpdates.update({(base_RID, i) : (write_val)})
                
                # overwrite the base pages
                # not sure if this is the most efficient way tbh
                self.page_directory_lock.acquire()
                for base_page in batch_cons_page:

                    new_base_page_info = base_page
                    new_base_page = new_base_page_info[0] # base page 
                    base_RID_to_update = new_base_page_info[1]


                    # TPS = -1 # TPS can never be -1
                    
                    # for i in range(self.num_columns):
                    #     val = seenUpdates.get((base_RID_to_update, i))
                    #     col_value = val[0]
                    #     tempTPS = val[1]

                    #     # we want the maximum tail record RID
                    #     if tempTPS > TPS:
                    #         TPS = tempTPS

                    #     # if it's None we don't need to do anything
                    #     if col_value != None:
                            
                    #         location = self.page_directory.get((i, base_RID_to_update))

                    #         page_offset = location[2]
                    #         new_base_page[i].replace(col_value, page_offset)  
                    #         # change TPS
                    #         # new_base_page.updateTPS(TPS, page_index)

                # swap the page locations. NEEDS TO BE LOCKED
                for base_page in batch_cons_page:

                    new_base_page_info = base_page
                    new_base_page = new_base_page_info[0] # base page 
                    base_RID_to_update = new_base_page_info[1]

                    location = self.page_directory.get((0, base_RID_to_update))
                    page_range_index = location[0]
                    page_index = location[1]
                    page_range = self.page_ranges[page_range_index] 

                    
                    #swaps directly so changing it to use helper for buffer
                    #old_base_page = page_range.base_pages[page_index]
                    #self.deallocation_queue.put(old_base_page)
                    #page_range.base_pages[page_index] = new_base_page

                    #merges the col values to base going through buffer
                    for i in range(METADATA_COLUMNS, self.total_columns): #skip the metadatacols
                        #newest merged value for rid and in col
                        newest_value = seenUpdates.get((base_RID_to_update, i))
                        #if theres no update end it earlier
                        if newest_value == None:
                            continue
                        #where to store newest_value in page_directory
                        value_location = self.page_directory.get((i, base_RID_to_update))
                        #get the location
                        page_offset = value_location[2]
                        #use the helper to involve buffer
                        self._update_page(page_range_index, page_index,i, newest_value, page_offset)


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
            
            #if (page_range.base_pages[page_index], base_RID) not in base_page_copy:
            #    base_page_copy.append((page_range.base_pages[page_index], base_RID))
                #base_page_copy.add((page_range.base_pages[page_index], base_RID))

            #change it to have helper to use buffer
            #just changing page_range.base_pages[page_index], base_RID to base_page and base_key
            base_page, base_key = self._get_page(page_range_index, page_index, RID_COLUMN)
            #done needing it pinned so unpin
            self.bufferpool.unpin(base_key)
            #changing base_RID to base_key crashed so dont do that
            if (base_page, base_RID) not in base_page_copy:
                base_page_copy.append((base_page, base_RID))




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
                    continue #edit
                # if the schema says it was updated, but no tail chain, continue to read from base
                if indirection_rid is None or indirection_rid == 0: #elif it if
                    # schema is updated but no tail exists, so read from base
                    result.append(read(physical_col_idx, rid))
                    continue #edit
                '''
                else:
                    # check if the latest tail has the column
                    tail_schema = read(SCHEMA_ENCODING_COLUMN, indirection_rid)
                    # if newest tail has the column, it becomes the latest value
                    if tail_schema[col_idx] == '1':
                        result.append(read(physical_col_idx, indirection_rid))
                    else:
                        # case where newest tail doesn't have the column, latest value stays from base record
                        result.append(read(physical_col_idx, rid))
                '''
                latest_value = None
                current = indirection_rid

                while current != None and current != 0 and current != rid:
                    tail_schema = read(SCHEMA_ENCODING_COLUMN, current)
                    if tail_schema == None:
                        break
                    if len(tail_schema) < self.num_columns:
                        tail_schema = tail_schema + ('0' * (self.num_columns - len(tail_schema)))
                    
                    if tail_schema[col_idx] == '1':
                        latest_value = read(physical_col_idx, current)
                        break

                    current = read(INDIRECTION_COLUMN, current)
                
                #if no tail then base still there
                if latest_value == None:
                    latest_value = read(physical_col_idx, rid)
                
                result.append(latest_value)
                    
        else:
            # using rabbit_hunt with the base_rid
            for col_idx in col_indices:                    
                result.append(self.rabbit_hunt(col_idx, 0, relative_version, base_rid = rid))   # primary_key is unused when base_rid is given, 0 is placeholder
        return result

    """
    Saves table to disk.

    :param path: string     #Path to save table to

    NOTE: PICKLE IS ALLOWED FOR SAVING DATA OTHER THAN PAGES
    """
    def save(self, path):
        os.makedirs(path, exist_ok=True)

        # save table-level metadata
        with open(os.path.join(path, 'table_metadata.bin'), 'wb') as f:
            name_bytes = self.name.encode('utf-8')
            f.write(struct.pack('<q', len(name_bytes)))
            f.write(name_bytes)
            
            f.write(struct.pack('<q', self.num_columns))
            f.write(struct.pack('<q', self.key))
            f.write(struct.pack('<q', self.RID_counter))
        
        # save page directory
        with open(os.path.join(path, 'page_directory.pkl'), 'wb') as f:
            pickle.dump(self.page_directory, f)
        
        # save page ranges
        for i, page_range in enumerate(self.page_ranges):
            page_range.save(os.path.join(path, f'page_range_{i}'))

    """
    Load table from disk.

    :param path: string     #Path to load table from
    """
    def load(self, path):
        # load table-level metadata
        with open(os.path.join(path, 'table_metadata.bin'), 'rb') as f:
            name_length = struct.unpack('<q', f.read(ENTRY_SIZE))[0]
            self.name = f.read(name_length).decode('utf-8')

            self.num_columns = struct.unpack('<q', f.read(ENTRY_SIZE))[0]
            self.key = struct.unpack('<q', f.read(ENTRY_SIZE))[0]
            self.RID_counter = struct.unpack('<q', f.read(ENTRY_SIZE))[0]
        
        # reconstruct num_columns-dependant variables
        self.total_columns = self.num_columns + METADATA_COLUMNS
        self.empty_schema = '0' * self.num_columns

        # load page directory
        with open(os.path.join(path, 'page_directory.pkl'), 'rb') as f:
            self.page_directory = pickle.load(f)

        # reconstruct index
        self.index = Index(self) # assuming i can move it here MUST BE B4 PAGE_RANGE.load()
        
        # reconstruct index, create index for each column
        self.index = Index(self)

        # load page ranges
        '''edit to make sure it doesnt get mixed up w any preexisitng page_ranges to prevent many to many'''
        self.page_ranges = []
        i = 0
        print(os.path.join(path, f'pr_{i}'))
        while os.path.exists(os.path.join(path, f'pr_{i}')):
            page_range = PageRange(self.total_columns)
            page_range.load(os.path.join(path, f'page_range_{i}'), self)
            self.page_ranges.append(page_range)
            i += 1

            
        