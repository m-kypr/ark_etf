import pandas as pd
import os
import settings


class ETF():
  def __init__(self, name, link, *args, **kwargs):
    self.name = name
    self.link = link
    self._df = pd.DataFrame()

  def to_dict(self):
    return self.__dict__

  def __repr__(self):
    return str(self.to_dict())

  @staticmethod
  def from_dict(dict):
    return ETF(dict['name'], dict['link'])

  def merge_df(self, df):
    pd.concat([self._df, df])

  def __eq__(self, other):
    return self.name == other.name


class ETFDataframe(pd.DataFrame):
  def extract_column(self, column, etf, date, name=None):
    if not name:
      name = column
    col_df = self.filter([name])
    col_df = col_df.T
    col_df.index = [pd.to_datetime(date)]
    col_df = col_df.drop(['NAN'], axis=1, errors='ignore')

    all_col_path = os.path.join(
        settings.PICKLE_DIR, etf.name+f'-{column}.pickle')
    try:
      all_col_df = pd.read_pickle(all_col_path)
    except FileNotFoundError as e:
      all_col_df = pd.DataFrame()
    all_col_df = pd.concat([all_col_df, col_df])
    all_col_df.sort_index(inplace=True)
    all_col_df.to_pickle(all_col_path)

  def prepare(self, etf, date):
    self.drop(self.tail(3).index, inplace=True)
    self.ticker = self.ticker.astype(str)
    self.ticker = self.ticker.apply(lambda x: x.upper())
    self.set_index('ticker', inplace=True)

    self.extract_column('shares', etf, date)
    self.extract_column('marketcap', etf, date, 'market value($)')


def read_etfs():
  import json
  jj = json.loads(open(settings.ETFS_FILE, 'r').read())
  for supergroup in jj:
    for dd in supergroup['modules']:
      settings.add_etf(ETF.from_dict(dd))
