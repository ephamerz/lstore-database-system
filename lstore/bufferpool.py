from collections import OrderedDict # part of standard Python library to help us with LRU

class BufferFrame:
    # bufferframe represents a singule cached page inside the bufferpool
    # stores the actual page object, dirty flags, and pin count
    __slots__ = ("page", "dirty", "pin_count")

    def __init__(self, page):
        self.page = page
        self.dirty = False
        self.pin_count = 0

class Bufferpool:
    # bufferpool cashes a fixed number of pages into memory
    # for example, if a requested page isn't in memory, we have to load it from disk
    # if it is full, we get rid of an unpinned page using LRU (least recently used)

    def __init__(self, disk_manager, capacity_pages: int):
        # disk_manager: DiskManager instance (handles the actual disk I/O)
        # capacity_pages: max # of pages allowed in memory
        self.disk = disk_manager
        self.capacity = capacity_pages

        # page_key --> BufferFrame
        self.frames = {}

        # order tracking for LRU
        # OrderedDict tracks insertion order, move keys to the end when recently used
        # the oldest key = first item, the newest = last item
        self.lru = OrderedDict()

    def get_page(self, page_key):
        # get a page from the bufferpool (pinning it)
        # if page is already cached: return it --> increase pin --> update LRU
        # if not cached: load from disk --> get rid of unpinned page --> pin it --> return it
        # page_key: (table, page_range, is_tail, page_idx, col_idx)

        # if cached
        if page_key in self.frames:
            frame = self.frames[page_key]

            # pin the page
            frame.pin_count += 1

            # update the LRU, mark as most recently used
            if page_key not in self.lru:
                self.lru[page_key] = None
            self.lru.move_to_end(page_key)

            return frame.page
            
        # if bufferpool is full, get rid of one unpinned page
        if len(self.frames) >= self.capacity:
            self.remove_one()

        # loading the page from disk, or an empty page if it doesn't exist
        page = self.disk.read_page(page_key)

        # creating a frame for the page
        frame = BufferFrame(page)

        # pinning it since it is being currently used
        frame.pin_count = 1

        # inserting it into bufferpool structures
        self.frames[page_key] = frame
        self.lru[page_key] = None  # val doesn't matter since OrderedDict keys store order

        return frame.page

    def unpin(self, page_key):
        # decreasing the pin count when we are done using the page
        # a page can only be removed if the pin_count == 0
        # page_key: key of the page being unpinned
        frame = self.frames.get(page_key)
        if frame is None:
            return

        # preventing the pin_count from going negative
        if frame.pin_count > 0:
            frame.pin_count -= 1

    def mark_dirty(self, page_key):
        # a page is dirty when it has been modified in memory
        # page_key: key of the page that is now dirty
        frame = self.frames.get(page_key)
        if frame is not None:
            frame.dirty = True

    def flush_page(self, page_key):
        # writing a dirty page back to the disk
        # page_key: key of the page to flush
        frame = self.frames.get(page_key)
        if frame is None:
            return

        # only write if dirty
        if frame.dirty:
            self.disk.write_page(page_key, frame.page)
            frame.dirty = False

    def flush_all(self):
        # flush all the dirty pages to disk
        for key in list(self.frames.keys()):
            self.flush_page(key)

    def remove_one(self):
        # remove exactly one page from the bufferpool using LRU
        # can only remove pages with a pin_count == 0
        # if the page is dirty, must flush it to the disk before removed
        # LRU iterates from oldest to newest:
        for victim_key in list(self.lru.keys()):
            frame = self.frames[victim_key]

            # only remove if not pinned
            if frame.pin_count == 0:
                # flush if dirty
                if frame.dirty:
                    self.disk.write_page(victim_key, frame.page)
                    frame.dirty = False

                # remove from both maps
                del self.frames[victim_key]
                del self.lru[victim_key]
                return

        # safety, all pages are pinned and we cannot remove anymore
        raise RuntimeError("Bufferpool is now full. No unpinned pages available to remove.")