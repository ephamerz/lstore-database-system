# from lstore.table import Record
import struct

CAPACITY = 4096
MANDATORY_COLUMNS = 5
MAX_BASE_PAGES = 16
ENTRY_SIZE = 8 # 8 bytes

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
