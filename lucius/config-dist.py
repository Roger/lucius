PROJECT_NAME = 'lucius'
# use restricted python?
RESTRICT = True
DEBUG = False
TESTING = False
LOGGER_NAME = '%s_log' % PROJECT_NAME

COUCHDB_SERVER = 'http://user:pass@localhost:5984'
LUCIUS_PREFIX = '/_ftl/'
DISABLE_AUTO_SYNC = True

INDEX_PATH = '/tmp/couchdb_index/'

PORT = 4242
