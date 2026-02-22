import os
from lstore.page import Page
from lstore.config import CAPACITY

"""
DiskManager is needed to read and write pages to the disk. Each page is uniquely
identified by its page_key: (table_name, page_range_index, is_tail, logical_page_index,
column_index)
"""

class DiskManager:
    def __init__(self, db_path: str):
        # here we initialize the DiskManager
        # db_path is where all database files are going to be stored
        self.db_path = db_path

        # make sure that the database root directory exists
        os.makedirs(self.db_path, exist_ok=True)

    def _page_path(self, page_key):
        # here we covert a page_key into a concrete file path on the disk
        # page_key: (table_name, page_range_index, is_tail, logical_page_index, column_index)
        table_name, page_range_index, is_tail, page_index, column_index = page_key

        # directory for this table
        table_dir = os.path.join(self.db_path, table_name)

        # directory for this page range
        page_range_dir = os.path.join(table_dir, f"pr_{page_range_index}")

        # create directories if they do not exist
        os.makedirs(page_range_dir, exist_ok=True)

        # determine whether this is the base or tail page
        kind = "tail" if is_tail else "base"

        # creating the filename
        filename = f"{kind}_p{page_index}_c{column_index}.bin"

        # return the full path to page file
        return os.path.join(page_range_dir, filename)

    def read_page(self, page_key):
        # here we read a page from disk
        # if the page file does not exist, return an empty page
        # page_key is a unique identifier for the page
        # resolving page file path
        path = self._page_path(page_key)

        # creating a new empty page object
        page = Page()

        # if the page does not exist on disk, return an empty page
        if not os.path.exists(path):
            return page

        # open the page file in binary
        with open(path, "rb") as f:
            data = f.read(CAPACITY)

        # making sure the page has same # bytes as CAPACITY
        if len(data) < CAPACITY:
            # zero bytes for if the file is too small
            data = data + bytes(CAPACITY - len(data))
        elif len(data) > CAPACITY:
            # truncate if the file is too large (just as a safety feature)
            data = data[:CAPACITY]

        # loading raw bytes into page object
        page.data[:] = data

        # note that page.page_size is not restored yet
        # this is currently safe for reads and replaces
        # we'll fix this later on when inserts go through the bufferpool
        page.page_size = 0

        return page

    def write_page(self, page_key, page: Page):
        # this writes a page to disk, overwriting existing page files
        # page_key is unique identifier for the page
        # page is page object containing the data to write
        # Resolve page file path
        path = self._page_path(page_key)

        # Open file in binary write mode (creates or overwrites)
        with open(path, "wb") as f:
            # Write exactly CAPACITY bytes
            f.write(page.data)