from wsgiref.simple_server import make_server
from pyramid.config import Configurator
from pyramid.response import Response

from nosql import KeyColumnValueStore

from pprint import pprint

store = KeyColumnValueStore(path='/tmp/codetestdata')

def get_keys(request):
  response = store.get_keys()
  return response

def get_key(request):
  key = request.matchdict['key']
  start = request.GET.get('start', None)
  stop = request.GET.get('stop', None)
  
  if start is not None or stop is not None:
    response = store.get_slice(key, start, stop)
  else:
    response = store.get(key)
  return response

def get_key_column(request):
  key = request.matchdict['key']
  column = request.matchdict['column']
  response = store.get(key, column)
  return response

def set_key_column(request):
  try:
    key = request.matchdict['key']
    column = request.matchdict['column']
    value = request.matchdict['value']
    store.set(key, column, value)
    response = Response('OK', 200)
  except:
    response = Response('Unable to set the value [%s] for the key [%s] / column [%s]' % (value, key, column), 500)
  return response

def delete_key_column(request):
  try:
    key = request.matchdict['key']
    column = request.matchdict['column']
    store.delete(key, column)
    response = Response('OK', 200)
  except:
    response = Response('Unable to delete the column [%s] for key [%s]' % (column, key), 500)
  return response

def delete_key(request):
  try:
    key = request.matchdict['key']
    store.delete_key(key)
    response = Response('OK', 200)
  except:
    response = Response('Unable to delete the key [%s]' % (key), 500)
  return response


if __name__ == '__main__':
  config = Configurator()

  config.add_route('get_keys', '/store/keys', request_method='GET')
  config.add_view(get_keys, route_name='get_keys', renderer='json')

  config.add_route('get_key', '/store/keys/{key}', request_method='GET')
  config.add_view(get_key, route_name='get_key', renderer='json')

  config.add_route('get_key_column', '/store/keys/{key}/{column}', request_method='GET')
  config.add_view(get_key_column, route_name='get_key_column', renderer='json')

  config.add_route('set_key_column', '/store/keys/{key}/{column}/{value}', request_method='PUT')
  config.add_view(set_key_column, route_name='set_key_column', renderer='json')

  config.add_route('delete_key', '/store/keys/{key}', request_method='DELETE')
  config.add_view(delete_key, route_name='delete_key', renderer='json')

  config.add_route('delete_key_column', '/store/keys/{key}/{column}', request_method='DELETE')
  config.add_view(delete_key_column, route_name='delete_key_column', renderer='json')

  app = config.make_wsgi_app()
  server = make_server('0.0.0.0', 8088, app)
  server.serve_forever()



