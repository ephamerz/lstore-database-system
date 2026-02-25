import os                                 
from lstore.page import Page            
from lstore.config import CAPACITY      

class DiskManager:
    '''
    Deals with persistence; maps (table, range, tail, page, column) keys to physical binary file
    '''

    def __init__(self, db_path: str):
        self.db_path = db_path
        os.makedirs(self.db_path, exist_ok=True)

    def _get_path(self, page_key):
        table_name, page_range_index, is_tail, page_index, column_index = page_key

        table_directory = os.path.join(self.db_path, table_name)

        range_directory = os.path.join(table_directory, f"page_range_{page_range_index}")

        # creates directories if they DNE
        if not os.path.exists(range_directory):
            os.makedirs(range_directory, exist_ok=True)

        kind = "tail" if is_tail else "base"
        return os.path.join(range_directory, f"{kind}_page_{page_index}_col_{column_index}.bin")

    # functions to convert integer to 8 bytes, and then reverse
    def _int_to_8(self, x: int) -> bytes:
        # have to convert an integer into exactly 8 bytes
        return int(x).to_bytes(8, byteorder="little", signed=True)
    def _int_from_8(self, b: bytes) -> int:
        return int.from_bytes(b, byteorder="little", signed=True)

    # reading a page from disk and returning a page object
    def read_page(self, page_key):
        path = self._get_path(page_key)
        page = Page()

        if not os.path.exists(path):    # returning a blank page if one hasn't been created
            return page
        
        # safe defaults
        page.num_records = 0
        page.page_size = 0
        page.data = bytearray(CAPACITY)
        
        with open(path, "rb") as f:
            header = f.read(16)     # header has num_records (8 bytes) + page_size (8 bytes)
            # if the file is too small, make as empty page
            if len(header) < 16:
                data = f.read(CAPACITY)
                page.data[:] = data.ljust(CAPACITY, b"\x00")[:CAPACITY]
                return page

            page.num_records = self._int_from_8(header[0:8])
            page.page_size = self._int_from_8(header[8:16])


            data = f.read(CAPACITY)

        # making sure data is exactly CAPACITY bytes
        data = data.ljust(CAPACITY, b"\x00")[:CAPACITY]

        page.data[:] = data

        if page.num_records < 0:
            page.num_records = 0
        if page.page_size < 0:
            page.page_size = 0
        if page.page_size > CAPACITY:
            page.page_size = CAPACITY

        return page

    # write a page object to disk to overwrite any other file
    def write_page(self, page_key, page: Page):
        path = self._get_path(page_key)

        data = bytes(page.data)

        data = data.ljust(CAPACITY, b"\x00")[:CAPACITY]

        header = self._int_to_8(page.num_records) + self._int_to_8(page.page_size)

        with open(path, "wb") as f:
            # metadata first
            f.write(header)
            # page data after
            f.write(data)
