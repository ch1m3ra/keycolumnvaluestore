#! /usr/bin/python

from pprint import pprint
import pickle
from bintrees import RBTree

class KeyColumnValueStore(object):
  def __str__(self):
    return "\n".join(["%s -> %s" % (key, self.get(key)) for key in self.cache.keys()])

  def __init__(self, **kwargs):
    self.path = None
    try:
      self.path = kwargs['path']
      with open(self.path, 'r') as reader:
        if('path' in kwargs): self.cache = pickle.load(reader)
        else: raise Exception
      pprint(self.cache)
    except:
      self.cache = {}

  def __del__(self):
    if(self.path):
      try:
        with open(self.path, 'w+') as writer:
          pickle.dump(self.cache, writer)
      except:
        raise Exception("Problem persisting store to disk")

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    self.close()

  def close(self):
    del(self)

  def set(self, key, col, val):
    """ sets the value at the given key/column """
    if key not in self.cache:
      self.cache[key] = RBTree([(col, val)])
    else:
      self.cache[key].insert(col, val)

  def get(self, key, col = None):
    """ return the value at the specified key/column """
    result = None
    if col is not None:
      if key in self.cache and col in self.cache[key]:
        result = self.cache[key][col]
        #result = [element for element in self.cache[key] if element[0] == col][0][1]
    else:
      result = self.get_key(key)
    return result

  def get_key(self, key):
    """ returns a sorted list of column/value tuples """
    result = []
    if key in self.cache:
      result = list(self.cache[key].items())
    return result

  def get_keys(self):
    """ returns a set containing all of the keys in the store """
    return self.cache.keys()

  def delete(self, key, col):
    """ removes a column/value from the given key """
    del(self.cache[key][col])

  def delete_key(self, key):
    """ removes all data associated with the given key """
    del(self.cache[key])

  def get_slice(self, key, start, stop):
    """
    returns a sorted list of column/value tuples where the column
    values are between the start and stop values, inclusive of the
    start and stop values. Start and/or stop can be None values, 
    leaving the slice open ended in that direction
    """ 
    result = []
    if stop is not None:
      for node in list(self.cache[key][start:].items()):
        result.append(node)
        # Check the node value after it has been appended to the list so that the search is upper-bound inclusive
        if(node[0] == stop): break
    else:
      result = list(self.cache[key][start:].items())
    return result

def level1():
  # given this store and dataset
  with KeyColumnValueStore() as store:
    store.set('a', 'aa', 'x')
    store.set('a', 'ab', 'x')
    store.set('c', 'cc', 'x')
    store.set('c', 'cd', 'x')
    store.set('d', 'de', 'x')
    store.set('d', 'df', 'x')


    pprint(store.cache)

    # the statements below will evaluate to True
    assert(store.get('a', 'aa') == 'x')
    assert(store.get_key('a') == [('aa', 'x'), ('ab', 'x')])

    # nonexistent keys/columns, the statements below
    # will evaluate to True
    assert(store.get('z', 'yy') is None)
    assert(store.get('z') == [])

    # if we set different values on the 'a' key:
    store.set('a', 'aa', 'y')
    store.set('a', 'ab', 'z')

    pprint(store.cache)

    # the statements below will evaluate to True
    assert(store.get('a', 'aa') == 'y')
    assert(store.get_key('a') == [('aa', 'y'), ('ab', 'z')])

    assert(store.get_keys() == ['a', 'c', 'd'])

    # deleting
    store.delete('d', 'df')

    # this will evaluate to True
    assert(store.get_key('d') == [('de', 'x')])

    # delete an entire key
    store.delete_key('c')

    # this will evaluate to True
    assert(store.get_key('c') == [])

    """
      print('#---------------------#')
      pprint(self.cache[key])
      pprint(self.cache[key][:])
      print('#---------------------#')
    """


def level2():
  # given this store and dataset
  with KeyColumnValueStore() as store:
    store.set('a', 'aa', 'x')
    store.set('a', 'ab', 'x')
    store.set('a', 'ac', 'x')
    store.set('a', 'ad', 'x')
    store.set('a', 'ae', 'x')
    store.set('a', 'af', 'x')
    store.set('a', 'ag', 'x')

    pprint(store.cache)

    # the following statements will evaluate to True 
    assert(store.get_slice('a', 'ac', 'ae') == [('ac', 'x'), ('ad', 'x'), ('ae', 'x')])
    assert(store.get_slice('a', 'ae', None) == [('ae', 'x'), ('af', 'x'), ('ag', 'x')])
    assert(store.get_slice('a', None, 'ac') == [('aa', 'x'), ('ab', 'x'), ('ac', 'x')])

def level3():
  with KeyColumnValueStore(path='/tmp/codetestdata') as store:
    """
    store.set('a', 'aa', 'x')
    store.set('a', 'ab', 'x')
    store.set('a', 'ac', 'x')
    store.set('a', 'ad', 'x')
    store.set('a', 'ae', 'x')
    store.set('a', 'af', 'x')
    store.set('a', 'ag', 'x')

    store.set('c', 'cc', 'x')
    store.set('c', 'cd', 'x')
    store.set('d', 'de', 'x')
    store.set('d', 'df', 'x')
    """

    pprint("level3: %s" % store.get('a'))
    pprint("level3: %s" % store.get('c'))
    pprint("level3: %s" % store.get('c', 'cd'))
    pprint("level3: %s" % store.get('d', 'de'))

    print(store)

if __name__ == '__main__':
  #level1()
  #level2()
  level3()

