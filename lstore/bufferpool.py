from collections import OrderedDict # to help us with LRU

class BufferFrame:
    '''
    Represents a singule cached page inside the bufferpool, stores the actual page object, dirty flags, and pin count
    '''
    
    __slots__ = ("page", "dirty", "pin_count")

    def __init__(self, page):
        self.page = page
        self.dirty = False
        self.pin_count = 0

class Bufferpool:
    '''
    Cashes a fixed number of pages into memory, for example, if a requested page isn't in memory, we have to load it from disk
    If it is full, we get rid of an unpinned page using LRU (least recently used)
    '''

    def __init__(self, disk_manager, capacity_pages: int):
        self.disk = disk_manager
        self.capacity = capacity_pages

        # page_key -> BufferFrame
        self.frames = {}
        self.lru = OrderedDict()

    def get_page(self, page_key):
        '''
        Gets a page from the bufferpool (pinning it)
        If page is already cached: return it -> increase pin -> update LRU
        Else: load from disk -> get rid of unpinned page -> pin it -> return it
        '''

        if page_key in self.frames:
            frame = self.frames[page_key]

            frame.pin_count += 1

            if page_key not in self.lru:
                self.lru[page_key] = None
            self.lru.move_to_end(page_key)

            return frame.page
            
        # remove an unpinned page if bufferpool is full
        if len(self.frames) >= self.capacity:
            self.remove_one()

        page = self.disk.read_page(page_key)

        frame = BufferFrame(page)

        frame.pin_count = 1

        self.frames[page_key] = frame
        self.lru[page_key] = None  # val doesn't matter since OrderedDict keys store order

        return frame.page

    def unpin(self, page_key):
        '''
        Decreasing the pin count when we are done using the page
        A page can only be removed if the pin_count == 0
        '''
        frame = self.frames.get(page_key)
        if frame is None:
            return

        if frame.pin_count > 0:
            frame.pin_count -= 1

    def mark_dirty(self, page_key):
        '''
        Page is dirty when it has been modified in memory
        '''
        frame = self.frames.get(page_key)
        if frame is not None:
            frame.dirty = True

    def flush_page(self, page_key):
        '''
        Writing a dirty page back to the disk
        '''
        frame = self.frames.get(page_key)
        if frame is None:
            return

        if frame.dirty:
            self.disk.write_page(page_key, frame.page)
            frame.dirty = False

    def flush_all(self):
        for key in list(self.frames.keys()):
            self.flush_page(key)

    def remove_one(self):
        '''
        Remove exactly one page from the bufferpool using LRU
        Can only remove pages with a pin_count == 0
        If page is dirty, flush it to the disk before removed
        '''
        for victim_key in list(self.lru.keys()):
            # skip keys in lru but not in frames cause of desync
            if victim_key not in self.frames:
                try:
                    del self.lru[victim_key]
                except KeyError:
                    pass
                continue
            
            frame = self.frames[victim_key]

            if frame.pin_count == 0:
                if frame.dirty:
                    self.disk.write_page(victim_key, frame.page)
                    frame.dirty = False

                # remove from both maps with error handling
                try:
                    del self.frames[victim_key]
                except KeyError:
                    pass  # already removed, continue
                try:
                    del self.lru[victim_key]
                except KeyError:
                    pass  # already removed, continue
                return

        # all pages are pinned and we cannot remove anymore
        raise RuntimeError("bufferpool is full, no unpinned pages available to remove.")