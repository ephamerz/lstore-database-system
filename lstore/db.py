from lstore.table import Table
from lstore.bufferpool import Bufferpool
from lstore.disk_manager import DiskManager
import os
import struct
import pickle

class Database():

    def __init__(self):
        self.tables = {} # dictionary for faster lookup
        self.path = None

        self.bufferpool = None
        self.disk_manager = None

    """
    Load a database (all tables) from disk. If the database does not exist, create a new one.
    If it does exist, load all tables and their metadata.

    :param path: string     #Path to load database from
    """
    def open(self, path):
        self.path = path
        

        # create path if it doesn't exist
        os.makedirs(path, exist_ok=True)

        #since path is set now can initialize bufferpool disk_manager
        self.disk_manager = DiskManager(path)
        self.bufferpool = Bufferpool(self.disk_manager, capacity_pages = 2000) #what num is capacity pages?


        metadata_path = os.path.join(path, 'db_metadata.pkl')

        if os.path.exists(metadata_path): # if db metadata exists, load it
            # load metadata (table names)
            with open(metadata_path, 'rb') as f:
                table_names = pickle.load(f)

            # load tables
            for table_name in table_names:
                table_path = os.path.join(path, table_name)

                #get the inputs needed
                with open(os.path.join(table_path, 'table_metadata.bin'), 'rb') as f:
                    #dont want it to be dummy so include these
                    #gives length of name so name knows how much to read and makes it into an int form for name
                    #8 was used in table's save() so using it here
                    length_of_name = struct.unpack('<q', f.read(8))[0]
                    #decode bytes into string
                    name = f.read(length_of_name).decode('utf-8')
                    #read as int
                    num_columns = struct.unpack('<q', f.read(8))[0]
                    #read as int
                    key = struct.unpack('<q', f.read(8))[0]

                #(not using just dummy Table since the Table needs real values and None caused it to error)
                table = Table(name, num_columns, key, self.bufferpool, self.disk_manager) # create dummy Table to call load() on // include buffer and disk
                table.load(table_path)
                self.tables[table_name] = table

    """
    Save the database (all tables) to disk.

    :param path: string     #Path to save database to
    """
    def close(self):
        if self.path is None: # guarantee open() has been called
            raise Exception("Database path DNE. Call open(path) first.")
        
        #along w close need to flush all now so include here
        self.bufferpool.flush_all()

        # save metadata (table names)
        table_names = list(self.tables.keys())
        with open(os.path.join(self.path, 'db_metadata.pkl'), 'wb') as f:
            pickle.dump(table_names, f)
        
        # save tables
        for table_name, table in self.tables.items():
            table_path = os.path.join(self.path, table_name)
            table.save(table_path)

    """
    # Creates a new table

    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key_index: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        # Prevent duplicate table names 
        if self.get_table(name) is not None:
            print(f"dupe table name: '{name}' already exists")
            return None

        table = Table(name, num_columns, key_index, self.bufferpool, self.disk_manager) #include buffer and disk
        self.tables[name] = table

        return table # assuming it wants the table returned instead of boolean

    
    """
    # Deletes the specified table

    :param name: string     #Table name to delete
    """
    def drop_table(self, name):
        table = self.get_table(name)
        if table is None: # prevent ValueError in case of non-existant table
            print(f"table '{name}' DNE")
            return False

        del self.tables[name]

        return True

    
    """
    # Returns table with the passed name

    :param name: string     #Table name to return
    """
    def get_table(self, name):
        return self.tables.get(name, None)