from util import makedir

DOWNLOAD = True
DEBUG = True
FORCE_REPOPULATE = False
FORCE_REPICKLE = True

ETFS_FILE = "etfs.json"
DATETIME_FORMAT = '%m%d%y'
ENDPOINTS = ['api/etfs', 'api/etfs/{id}']
__ETFS = []

CSV_DIR = makedir('csv')
PICKLE_DIR = makedir('pickle')
OUT_DIR = makedir('out')


def find_etf_by_name(name):
  for etf in ETFS:
    if etf.name == name:
      return etf


def find_etf_by_csv(csv):
  for etf in ETFS:
    if etf.csv == csv:
      return etf


def add_etf(etf):
  if etf not in __ETFS:
    __ETFS.append(etf)


def get_etfs():
  return __ETFS
