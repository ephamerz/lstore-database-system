"""
A data strucutre holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well.
"""

B_TREE_DEGREE = 100 # can be adjusted later, controls the amount of children a BTreeNode can have

class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        for i in range(table.num_columns):
            self.create_index(i)

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        RIDs = [] # will contain the list of all RIDs associated with the value
        btree = self.indices[column]
        root = btree.root
        btree.BtreeSearch(root, value, RIDs)
        return RIDs

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        RIDs = [] # will contain the list of all RIDs associated with the values in the range of begin and end, inclusive to both
        btree = self.indices[column]
        root = btree.root
        btree.BtreeSearchRange(root, begin, end, RIDs)
        return RIDs

    """
    # insert a record from a given column into the index
    """
    def insert_record(self, RID, value, column):
        key = (value, RID)
        self.indices[column].insert(key)

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        self.indices[column_number] = BTree(B_TREE_DEGREE)

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass

    """
    # delete a record from a given column in the index
    """
    def delete_record(self, RID, value, column):
        #print('this got called')
        key = (value, RID)
        #print(f'key is {key}')
        btree = self.indices[column]
        root = btree.root
        #print('starting btree delete')
        btree.delete(root, key)

# Source: https://www.geeksforgeeks.org/dsa/b-tree-in-python/. Code is a modified version of GeeksforGeeks's.
class BTreeNode:
    def __init__(self, leaf=False):
        self.leaf = leaf
        # store a tuple in the format (value, RID) in keys
        self.keys = []
        self.child = []

    def display(self, level=0):
        print(f"Level {level}: {self.keys}")
        if not self.leaf:
            for child in self.child:
                child.display(level + 1)

class BTree:
    def __init__(self, t):
        self.root = BTreeNode(True)
        self.t = t

    def display(self):
        self.root.display()

    # k is the key we want to insert. k is a tuple of the format (value, RID)
    def insert(self, k):
        root = self.root
        if len(root.keys) == (2 * self.t) - 1:
            temp = BTreeNode()
            self.root = temp
            temp.child.append(root)
            self.split_child(temp, 0)
            self.insert_non_full(temp, k)
        else:
            self.insert_non_full(root, k)

    # x is the node we want to insert the key k into
    def insert_non_full(self, x, k):
        i = len(x.keys) - 1
        if x.leaf:
            x.keys.append(None)  # Make space for the new key
            while i >= 0 and k < x.keys[i]:
                x.keys[i + 1] = x.keys[i]
                i -= 1
            x.keys[i + 1] = k
        else:
            while i >= 0 and k < x.keys[i]:
                i -= 1
            i += 1
            if len(x.child[i].keys) == (2 * self.t) - 1:
                self.split_child(x, i)
                if k > x.keys[i]:
                    i += 1
            self.insert_non_full(x.child[i], k)

    # Split the child
    def split_child(self, x, i):
        t = self.t
        y = x.child[i]
        z = BTreeNode(y.leaf)
        x.child.insert(i + 1, z)
        x.keys.insert(i, y.keys[t - 1])
        z.keys = y.keys[t: (2 * t) - 1]
        y.keys = y.keys[0: t - 1]
        if not y.leaf:
            z.child = y.child[t: 2 * t]
            y.child = y.child[0: t]
    
    # Delete a node
    # x: the node we are currently searching for the key
    # k: the key we want to delete (tuple)
    def delete(self, x, k):
        t = self.t
        i = 0
        key_value = int(k[0])  # comparison uses only the value portion of the tuple
        while i < len(x.keys) and key_value > int(x.keys[i][0]):
            i += 1
        if x.leaf:
            if i < len(x.keys) and x.keys[i] == k:
                x.keys.pop(i)
                return
            return

        if i < len(x.keys) and x.keys[i] == k:
            return self.delete_internal_node(x, k, i)
        elif len(x.child[i].keys) >= t:
            self.delete(x.child[i], k)
        else:
            if i != 0 and i + 2 < len(x.child):
                if len(x.child[i - 1].keys) >= t:
                    self.delete_sibling(x, i, i - 1)
                elif len(x.child[i + 1].keys) >= t:
                    self.delete_sibling(x, i, i + 1)
                else:
                    self.delete_merge(x, i, i + 1)
            elif i == 0:
                if len(x.child[i + 1].keys) >= t:
                    self.delete_sibling(x, i, i + 1)
                else:
                    self.delete_merge(x, i, i + 1)
            elif i + 1 == len(x.child):
                if len(x.child[i - 1].keys) >= t:
                    self.delete_sibling(x, i, i - 1)
                else:
                    self.delete_merge(x, i, i - 1)
                #edited to add to check if deleting issue
                if i >= len(x.child):
                    i = len(x.child) - 1
                    
            self.delete(x.child[i], k)

    # Delete internal node
    def delete_internal_node(self, x, k, i):
        t = self.t
        if x.leaf:
            if x.keys[i] == k:
                x.keys.pop(i)
                return
            return

        if len(x.child[i].keys) >= t:
            x.keys[i] = self.delete_predecessor(x.child[i])
            return
        elif len(x.child[i + 1].keys) >= t:
            x.keys[i] = self.delete_successor(x.child[i + 1])
            return
        else:
            self.delete_merge(x, i, i + 1)
            self.delete_internal_node(x.child[i], k, self.t - 1)

    # Delete the predecessor
    def delete_predecessor(self, x):
        if x.leaf:
            return x.keys.pop()
        n = len(x.keys) - 1
        if len(x.child[n].keys) >= self.t:
            self.delete_sibling(x, n + 1, n)
        else:
            self.delete_merge(x, n, n + 1)
        return self.delete_predecessor(x.child[n])

    # Delete the successor
    def delete_successor(self, x):
        if x.leaf:
            return x.keys.pop(0)
        if len(x.child[1].keys) >= self.t:
            self.delete_sibling(x, 0, 1)
        else:
            self.delete_merge(x, 0, 1)
        return self.delete_successor(x.child[0])

    # Delete resolution
    def delete_merge(self, x, i, j):
        if j >= len(x.child):
            j = len(x.child) - 1
        if i >= len(x.child):
            i = len(x.child) - 1

        cnode = x.child[i]
        if j > i:
            rsnode = x.child[j]
            cnode.keys.append(x.keys[i])
            cnode.keys.extend(rsnode.keys)
            if len(rsnode.child) > 0:
                cnode.child.extend(rsnode.child)
            x.keys.pop(i)
            x.child.pop(j)
            new = cnode
        else:
            lsnode = x.child[j]
            lsnode.keys.append(x.keys[j])
            lsnode.keys.extend(cnode.keys)
            if len(cnode.child) > 0:
                lsnode.child.extend(cnode.child)
            x.keys.pop(j)
            x.child.pop(i)
            new = lsnode

        if x == self.root and len(x.keys) == 0:
            self.root = new

    def delete_sibling(self, x, i, j):
        cnode = x.child[i]
        if i < j:
            rsnode = x.child[j]
            cnode.keys.append(x.keys[i])
            x.keys[i] = rsnode.keys.pop(0)
            if len(rsnode.child) > 0:
                cnode.child.append(rsnode.child.pop(0))
        else:
            lsnode = x.child[j]
            cnode.keys.insert(0, x.keys[i - 1])
            x.keys[i - 1] = lsnode.keys.pop()
            if len(lsnode.child) > 0:
                cnode.child.insert(0, lsnode.child.pop())

    # Source: https://www.geeksforgeeks.org/dsa/introduction-of-b-tree-2/. Made modifications to fit the project.
    # x: node we are searching
    # k: the key we are looking for (int value)
    # RIDs: the running list of all RIDs
    # finds all RIDs associated to the value k (not a tuple)
    def BtreeSearch(self, x, k, RIDs):
        i = 0 # the index of the child to recurse into
        # look for the first key >= k
        while i < len(x.keys) and k > int(x.keys[i][0]):
            i += 1

        # collect matching keys stored in this node
        for j in range(len(x.keys)):
            if k == int(x.keys[j][0]):
                RIDs.append(x.keys[j][1])

        if x.leaf:
            return None

        # duplicates can be split across multiple children when separators equal k.
        # search the whole child span that can still contain k.
        left = i
        while left > 0 and int(x.keys[left - 1][0]) == k:
            left -= 1

        right = i
        while right < len(x.keys) and int(x.keys[right][0]) == k:
            right += 1

        for child_index in range(left, right + 1):
            if child_index < len(x.child):
                self.BtreeSearch(x.child[child_index], k, RIDs)
    
    # x: node we are searching
    # k: the key we are looking for (int value)
    # RIDs: the running list of all RIDs
    # finds all RIDs with values in the interval [begin, end] (inclusive to both begin and end)
    # note: can probably be optimized further for performance
    def BtreeSearchRange(self, x, begin, end, RIDs):
        j = 0
        # find the first key that is >= begin
        while j < len(x.keys) and int(x.keys[j][0]) < begin:
            j += 1
        for i in range(j, len(x.keys)):
            if not x.leaf:
                self.BtreeSearchRange(x.child[i], begin, end, RIDs) # need to search every child that is also within the range
            if begin <= int(x.keys[i][0]) <= end:
                RIDs.append(x.keys[i][1]) # store the RID
        if not x.leaf:
            return self.BtreeSearchRange(x.child[len(x.child)-1], begin, end, RIDs) # need to go one past the last node for some cases