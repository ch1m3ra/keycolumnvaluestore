#! /usr/bin/python2.7

import urllib2
import json
from nosql import KeyColumnValueStore

"""Unit tests for KeyColumnValueStore"""

__author__ = 'Arlen Haftevani'
__version__ = '1.1'

def level1_unittest():
  """Unit tests for level1 of the KeyColumnValueStore"""
  print("\nStart testing for level1 : Basic KeyColumnValueStore")

  # given this store and dataset
  with KeyColumnValueStore() as store:
    store.set('a', 'aa', 'x')
    store.set('a', 'ab', 'x')
    store.set('c', 'cc', 'x')
    store.set('c', 'cd', 'x')
    store.set('d', 'de', 'x')
    store.set('d', 'df', 'x')

    # the statements below will evaluate to True
    assert(store.get('a', 'aa') == 'x')
    assert(store.get_key('a') == [('aa', 'x'), ('ab', 'x')])

    # nonexistent keys/columns, the statements below
    # will evaluate to True
    assert(store.get('z', 'yy') is None)
    assert(store.get_key('z') == [])

    # if we set different values on the 'a' key:
    store.set('a', 'aa', 'y')
    store.set('a', 'ab', 'z')

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

    store.set('ee', 'a', '23')
    store.set('ee', 'ccc', 'x')
    store.set('ee', 'bb', 'a')
    store.set('ee', 'eeeass', '9')
    store.set('ee', 'bc', 'c')
    store.set('ee', 'd', 'z')

    assert(store.get('ee', 'a') == '23')
    assert(store.get('ee', 'asdfasfd') is None)
    assert(store.get_key('asdf') == [])
    assert(store.get_key('ee') == [('a', '23'), ('bb', 'a'), ('bc', 'c'), ('ccc', 'x'), ('d', 'z'), ('eeeass', '9')])

def level2_unittest():
  """Unit tests for level2 of the KeyColumnValueStore"""
  print("\nStart testing for level2 : Added the ability to get a slice from a specific key")

  # given this store and dataset
  with KeyColumnValueStore() as store:
    store.set('a', 'aa', 'x')
    store.set('a', 'ab', 'x')
    store.set('a', 'ac', 'x')
    store.set('a', 'ad', 'x')
    store.set('a', 'ae', 'x')
    store.set('a', 'af', 'x')
    store.set('a', 'ag', 'x')

    # the following statements will evaluate to True 
    assert(store.get_slice('a', 'ac', 'ae') == [('ac', 'x'), ('ad', 'x'), ('ae', 'x')])
    assert(store.get_slice('a', 'ae', None) == [('ae', 'x'), ('af', 'x'), ('ag', 'x')])
    assert(store.get_slice('a', None, 'ac') == [('aa', 'x'), ('ab', 'x'), ('ac', 'x')])

    store.set('ee', 'a', '23')
    store.set('ee', 'ccc', 'x')
    store.set('ee', 'bb', 'a')
    store.set('ee', 'eeeass', '9')
    store.set('ee', 'bc', 'c')
    store.set('ee', 'd', 'z')

    assert(store.get_slice('ee', 'bb', 'd') == [('bb', 'a'), ('bc', 'c'), ('ccc', 'x'), ('d', 'z')])

def level3_unittest():
  """Unit tests for level3 of the KeyColumnValueStore"""
  print("\nStart testing for level3 : Added persistance")

  with KeyColumnValueStore(path='/tmp/codetestdata') as store:
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

    assert(store.get_key('a') == [('aa', 'x'), ('ab', 'x'), ('ac', 'x'), ('ad', 'x'), ('ae', 'x'), ('af', 'x'), ('ag', 'x')])
    assert(store.get_key('c') == [('cc', 'x'), ('cd', 'x')])
    assert(store.get('c', 'cd') == 'x')
    assert(store.get('d', 'de') == 'x')

    store.set('ee', 'a', '23')
    store.set('ee', 'ccc', 'x')
    store.set('ee', 'bb', 'a')
    store.set('ee', 'eeeass', '9')
    store.set('ee', 'bc', 'c')
    store.set('ee', 'd', 'z')

    assert(store.get_slice('ee', 'bb', 'd') == [('bb', 'a'), ('bc', 'c'), ('ccc', 'x'), ('d', 'z')])

  """
  with KeyColumnValueStore(path='/tmp/codetestdata') as store:
    assert(store.get_key('a') == [('aa', 'x'), ('ab', 'x'), ('ac', 'x'), ('ad', 'x'), ('ae', 'x'), ('af', 'x'), ('ag', 'x')])
    assert(store.get_key('c') == [('cc', 'x'), ('cd', 'x')])
    assert(store.get('c', 'cd') == 'x')
    assert(store.get('d', 'de') == 'x')
    assert(store.get_slice('ee', 'bb', 'd') == [('bb', 'a'), ('bc', 'c'), ('ccc', 'x'), ('d', 'z')])

    #store.set('xy', 'yaryar', 'flex')

    assert(store.get('xy', 'yaryar'))
  """

def level4_unittest():
  """Unit tests for level4 of the KeyColumnValueStore"""
  print("\nStart testing for level4 : Test REST API for KeyColumnValueStore")

  def rest_get(path):
    return json.loads(urllib2.urlopen('%s%s' % (localhost, path)).read())

  def rest_put(path, data=None):
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request('%s%s' % (localhost, path))
    if data is not None:
      request.data = data
    request.add_header('Content-Type', 'application/json')
    request.get_method = lambda: 'PUT'
    url = opener.open(request)
    return request

  def rest_delete(path):
    opener = urllib2.build_opener(urllib2.HTTPHandler)
    request = urllib2.Request('%s%s' % (localhost, path))
    request.get_method = lambda: 'DELETE'
    url = opener.open(request)
    return request

  localhost = 'http://localhost:8088/store/keys/'

  assert(rest_get('a') == [["aa", "x"], ["ab", "x"], ["ac", "x"], ["ad", "x"], ["ae", "x"], ["af", "x"], ["ag", "x"]])
  assert(rest_get('a/aa') == 'x')

  rest_put('t/aa/yar')
  assert(rest_get('t/aa') == 'yar')
  assert(rest_get('ee?start=bb&stop=d') == [['bb', 'a'], ['bc', 'c'], ['ccc', 'x'], ['d', 'z']])

  rest_delete('t/aa')
  assert(rest_get('t/aa') is None)
  assert(rest_get('t') == [])

  rest_put('t/aa/yar')
  rest_put('t/bad/est')
  assert(rest_get('t') == [['aa', 'yar'], ['bad', 'est']])

  rest_delete('t')
  assert(rest_get('t') == [])

if __name__ == '__main__':
  level1_unittest()
  level2_unittest()
  level3_unittest()
  level4_unittest()

