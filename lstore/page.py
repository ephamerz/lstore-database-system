# from lstore.table import Record
import struct
import os
from lstore.config import CAPACITY, METADATA_COLUMNS, MAX_BASE_PAGES, ENTRY_SIZE


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
        self.page_number = 0
        self.page_size = 0
        
    def has_capacity(self, size):
        return self.page_size + size < CAPACITY

    def write(self, value):
        if self.has_capacity(ENTRY_SIZE):
            # if none write 0 
            if value is None:
                value = 0
            
            if isinstance(value, str):
                value_bytes = value.encode()
                value_bytes = value_bytes[:ENTRY_SIZE].ljust(ENTRY_SIZE, b'0')
                self.data[self.page_size:self.page_size + ENTRY_SIZE] = value_bytes
                self.page_size += ENTRY_SIZE
            elif isinstance(value, float):
                value_bytes = struct.pack('<d', value)
                self.data[self.page_size:self.page_size + ENTRY_SIZE] = value_bytes
                self.page_size += ENTRY_SIZE
            else:
                self.data[self.page_size:self.page_size + ENTRY_SIZE] = value.to_bytes(ENTRY_SIZE, byteorder="little", signed=True)
                self.page_size += ENTRY_SIZE
            return True
        return False
    
    def readWholePage(self):
        return self.data
    
    def read(self, lower_index):
        upper_index = lower_index + ENTRY_SIZE
        # this should always pass since we don't write unless we have the full capacity needed, but just in case
        if upper_index < CAPACITY:
            return(self.data[lower_index:upper_index])
        else:
            return None
        
    """
    # replace the value of an entry already within the page. mostly just for indirection pointers
    # value - the new value of the entry (as an integer)
    # index - the index of the entry that will be replaced
    """
    def replace(self, value, index):

        if isinstance(value, int):
            value_bytes = value.to_bytes(ENTRY_SIZE, byteorder="little", signed=True)
        elif isinstance(value, str):
            value_bytes = value.encode()
            value_bytes = value_bytes[:ENTRY_SIZE].ljust(ENTRY_SIZE, b'0')
        else:
            value_bytes = value  # assume it's already bytes
        
        self.data[index:index+ENTRY_SIZE] = value_bytes

    """
    Saves page to disk.

    param path: string     #Path to save page to
    """
    def save(self, path):
        with open(path, 'wb') as f:
            f.write(struct.pack('<q', self.num_records))
            f.write(struct.pack('<q', self.page_size))
            f.write(self.data)
            #I never write page_number anywhere, but it's never used. - NB

    """
    Loads page from disk.
    
    param path: string     #Path to load page from
    """
    def load(self, path):
        with open(path, 'rb') as f:
            self.num_records = struct.unpack('<q', f.read(ENTRY_SIZE))[0]
            self.page_size = struct.unpack('<q', f.read(ENTRY_SIZE))[0]
            self.data = bytearray(f.read(CAPACITY))

class PageRange:

    def __init__(self, num_columns): # initialize 16 base pages indexed at 0
        self.num_columns = num_columns # this includes the 4 metadata columns when we pass it in from table. page range doesn't need to be concerned about this
        self.basePageToWrite = 0 # a variable that keeps count of the current base page we should write to, I feel like this implementation might need to be revisited when deletion comes along - DH

        self.base_pages = []
        self.tps = [0] * MAX_BASE_PAGES # one tps value for every base page
        for base_page in range(MAX_BASE_PAGES): # make 16 base pages
            self.base_pages.append([])
            for page in range(0, (self.num_columns)): 
                self.base_pages[base_page].append(Page()) # create a page for each column

        self.tail_pages = []
        self.allocate_new_tail_page()

    def allocate_new_tail_page(self):
        newTailPage = []
        for page in range(self.num_columns): # create one tail page
            newTailPage.append(Page())
        self.tail_pages.append(newTailPage)

    def insert_to_tail_page(self):
        #reminder to possibly implement if we decide to do so
        pass

    def updateTPS(self, new_TPS_value, base_page_index):
        self.tps[base_page_index] = new_TPS_value


    """
    Saves page range to disk.

    param path: string     #Path to save page range to
    """
    def save(self, path):
        os.makedirs(path, exist_ok=True) # create path if it doesn't exist

        # save base pages with naming convention base_page_{base_page_index}_col_{column_index}.bin
        for base_page_index, base_page in enumerate(self.base_pages):
            for column_index, page in enumerate(base_page):
                page_path = os.path.join(path, f'base_page_{base_page_index}_col_{column_index}.bin')
                page.save(page_path)

        # save tail pages with similar naming convention 
        for tail_page_index, tail_page in enumerate(self.tail_pages):
            for column_index, page in enumerate(tail_page):
                page_path = os.path.join(path, f'tail_page_{tail_page_index}_col_{column_index}.bin')
                page.save(page_path)

        # remaining metadata of base/tail pages
        # num_columns is saved within Page.write()
        with open(os.path.join(path, 'page_range_metadata.bin'), 'wb') as f:
            f.write(struct.pack('<q', self.basePageToWrite))
            f.write(struct.pack('<q', len(self.tail_pages)))
    
    """
    Loads page range from disk.

    param path: string     #Path to load page range from
    """
    def load(self, path, table):
        # load metadata
        with open(os.path.join(path, 'page_range_metadata.bin'), 'rb') as f:
            self.basePageToWrite = struct.unpack('<q', f.read(ENTRY_SIZE))[0]
            num_tail_pages = struct.unpack('<q', f.read(ENTRY_SIZE))[0]

        # load base pages
        self.base_pages = [] # reset base pages before loading
        for base_page_index in range(MAX_BASE_PAGES):
            new_base_page = []

            for column_index in range(self.num_columns):
                page_path = os.path.join(path, f'base_page_{base_page_index}_col_{column_index}.bin')
                page = Page()
                page.load(page_path)
                new_base_page.append(page)

            # populate Index(obj) for each record
            size = new_base_page[0].size
            for i in range(size, step = ENTRY_SIZE):
                for column_index in range(table.METADATA_COLUMNS, self.num_columns):
                    rid = new_base_page[table.RID_COLUMN].read(i)
                    value = new_base_page[column_index].read(i)
                    table.index.insert_record(rid, value, (column_index - table.METADATA_COLUMNS))     

            self.base_pages.append(new_base_page)

        # load tail pages
        self.tail_pages = [] # reset tail pages before loading
        for tail_page_index in range(num_tail_pages):
            new_tail_page = []
            for column_index in range(self.num_columns):
                page_path = os.path.join(path, f'tail_page_{tail_page_index}_col_{column_index}.bin')
                page = Page()
                page.load(page_path)
                new_tail_page.append(page)
            self.tail_pages.append(new_tail_page)
            