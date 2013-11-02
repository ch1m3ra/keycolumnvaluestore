#! /usr/bin/python

import pickle
from bintrees import RBTree

__author__ = 'Arlen Haftevani'
__version__ = '1.1'

class KeyColumnValueStore(object):
  """Basic key column value store

  """
  def __str__(self):
    """Returns a list of column / value tuples for each key in the store."""
    return "\n".join(["%s -> %s" % (key, self.get_key(key)) for key in self.cache.keys()])

  __repr__ = __str__

  def __init__(self, **kwargs):
    """If the path is passed in via the 'path' kwarg then use it to initialize the store, otherwise start with an empty store.

    keyword arguments:
       path -- Optional: Value at a specific key / col combination.
    return       -- An instantiation of the KeyColumnValueStore.
    exceptions   -- None expected, except possibly OutOfMemory error.
    side effects -- If path is passed in and file is successfully open and read, then when the instantiated object is deleted it will attempt to persist at the same path that was passed in. 

    """
    self.path = None
    try:
      self.path = kwargs['path']
      with open(self.path, 'r+b') as reader:
        # Try to load data from the file
        self.cache = pickle.load(reader)
    except:
      self.cache = {}

  def __del__(self):
    """If the path was set on initialization then persist to the path."""
    if(self.path):
      try:
        with open(self.path, 'wb') as writer:
          pickle.dump(self.cache, writer)
      except:
        raise Exception("Problem persisting store to disk")

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    self.close()

  def close(self):
    """Shutdown the store."""
    del(self)

  def set(self, key, col, val):
    """Sets the value at the given key/column

    keyword arguments:
       key -- Unique value that must be hashable. 
       col -- Column within the key.
       val -- Value at a specific key / col combination.
    return       -- void
    exceptions   -- None expected, except possibly MemoryError.
    side effects -- If key doesn't exist in the store then it is created. If the column doesn't exist for the key then it is created with the value. If the column does exist for the key then it's value is updated.

    """
    if key not in self.cache:
      self.cache[key] = RBTree([(col, val)])
    else:
      self.cache[key].insert(col, val)

  def get(self, key, col):
    """Get the value at the specified key/column.

    keyword arguments:
       key -- Unique value that must be hashable. 
       col -- Column within the key.
    return       -- Value stored at key / column or None if it doesn't exist.
    exceptions   -- None
    side effects -- None

    """
    try:
      result = self.cache[key][col]
    except:
      result = None
    return result

  def get_key(self, key):
    """Returns a sorted list of column/value tuples.

    keyword arguments:
       key -- Unique value that must be hashable. 
    return       -- List of column/value tuples stored at key, or an empty list if it doesn't exist.
    exceptions   -- None
    side effects -- None

    """
    try:
      result = list(self.cache[key].items())
    except:
      result = []
    return result

  def get_keys(self):
    """Returns a set containing all of the keys in the store.

    keyword arguments:
       None
    return       -- List of column/value tuples stored at key, or an empty list if it doesn't exist.
    exceptions   -- None
    side effects -- None

    """
    return self.cache.keys()

  def delete(self, key, col):
    """Removes a column/value from the given key.

    keyword arguments:
       key -- Unique value that must be hashable.
       col -- Column within the key.
    return       -- None
    exceptions   -- None
    side effects -- If key / column exists then delete the column, value tuple from the key.

    """
    try:
      del(self.cache[key][col])
    except: pass

  def delete_key(self, key):
    """Removes all data associated with the given key.

    keyword arguments:
       key -- Unique value that must be hashable.
    return       -- List of column/value tuples stored at key, or an empty list if it doesn't exist.
    exceptions   -- None
    side effects -- If key exists then delete the key and all tuples associated with it.

    """
    try:
      del(self.cache[key])
    except: pass

  def get_slice(self, key, start, stop):
    """Returns a sorted list of column/value tuples where the column
    values are between the start and stop values, inclusive of the
    start and stop values. Start and/or stop can be None values, 
    leaving the slice open ended in that direction.

    keyword arguments:
       key   -- Unique value that must be hashable.
       start -- Lower bound of the return list, inclusive.
       stop  -- Upper bound of the return list, inclusive.
    return       -- List of column/value tuples stored at key, or an empty list if it doesn't exist.
    exceptions   -- None
    side effects -- None

    """ 
    result = []
    try:
      if stop is not None:
        for node in list(self.cache[key][start:].items()):
          result.append(node)
          # Check the node value after it has been appended to the list so that the search is upper-bound inclusive
          if(node[0] == stop): break
      else:
        result = list(self.cache[key][start:].items())
    except:
      result = []
    return result
