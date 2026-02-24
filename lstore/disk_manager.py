import os                                 
from lstore.page import Page            # page object that holds page data in memory
from lstore.config import CAPACITY      # fixed page size in bytes (4096)

class DiskManager:
    # DiskManager is responsible for disk I/O of pages, converts a logical page_key into a 
    # physical file on disk, and reads/writes Page objects to/from that file.

    def __init__(self, db_path: str):
        # main directory
        self.db_path = db_path
        # making sure it exists
        os.makedirs(self.db_path, exist_ok=True)

    def _page_path(self, page_key):
        # converting a page_key into a path on disk
        table_name, page_range_index, is_tail, page_index, column_index = page_key
        # directory for the table
        table_dir = os.path.join(self.db_path, table_name)
        # directory for specific page range of that table
        page_range_dir = os.path.join(table_dir, f"pr_{page_range_index}")
        # create directories if they do not exist
        os.makedirs(page_range_dir, exist_ok=True)
        # decide whether base or tail
        kind = "tail" if is_tail else "base"
        # file name uniquely identifies page position and column
        filename = f"{kind}_p{page_index}_c{column_index}.bin"
        # return full path to the page file
        return os.path.join(page_range_dir, filename)

    # needed functions to convert integer to 8 bytes, and then reverse
    def _int_to_8(self, x: int) -> bytes:
        # have to convert an integer into exactly 8 bytes
        # allowing negative values (used to delete markers)
        return int(x).to_bytes(8, byteorder="little", signed=True)

    def _int_from_8(self, b: bytes) -> int:
        # converting the 8 bytes back into an integer
        return int.from_bytes(b, byteorder="little", signed=True)

    # reading page
    def read_page(self, page_key):
        # reading a page from disk and returning a page object
        # finding page path
        path = self._page_path(page_key)
        # creating a new empty page object
        page = Page()
        # initializing safe defaults (in case file DNE)
        page.num_records = 0
        page.page_size = 0
        page.data = bytearray(CAPACITY)
        # if the page file DNE, return empty page
        if not os.path.exists(path):
            return page
        # open the page file in binary
        with open(path, "rb") as f:
            # reading the first 16 bytes
            header = f.read(16)
            # if the file is too small, make as empty page
            if len(header) < 16:
                data = f.read(CAPACITY)
                page.data[:] = data.ljust(CAPACITY, b"\x00")[:CAPACITY]
                return page
            # extracting num_records from the first 8 bytes
            page.num_records = self._int_from_8(header[0:8])
            # extracting page_size from next 8 bytes
            page.page_size = self._int_from_8(header[8:16])
            # reading the raw page data
            data = f.read(CAPACITY)
        # making sure data is exactly CAPACITY bytes
        data = data.ljust(CAPACITY, b"\x00")[:CAPACITY]
        # storing data inside the page object
        page.data[:] = data
        # checking for invalid values
        if page.num_records < 0:
            page.num_records = 0
        if page.page_size < 0:
            page.page_size = 0
        if page.page_size > CAPACITY:
            page.page_size = CAPACITY
        # return the new page
        return page

    # writing page
    def write_page(self, page_key, page: Page):
        # write a page object to disk to overwrite any other file
        # resolve the file path from page_key
        path = self._page_path(page_key)
        # convert page data to bytes
        data = bytes(page.data)
        # make sure exactly CAPACITY bytes
        data = data.ljust(CAPACITY, b"\x00")[:CAPACITY]
        # constructing a header, num_records (8 bytes) & page_size (8 bytes)
        header = self._int_to_8(page.num_records) + self._int_to_8(page.page_size)
        # open the file to write in binary
        with open(path, "wb") as f:
            # writing metadata first
            f.write(header)
            # writing page data after
            f.write(data)