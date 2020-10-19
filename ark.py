import os
import math
import datetime
import requests
import pandas as pd
from etf import *
import settings
from download import download
from exception import *


def update_etf_with_new_data(etf, date_dir):
  date_dir_path = os.path.join(settings.CSV_DIR, date_dir)
  for new_csv in os.listdir(date_dir_path):
    if etf.link.split('/')[-1] in new_csv:
      df = ETFDataframe(pd.read_csv(os.path.join(date_dir_path, new_csv)))
      df.prepare(etf, date_dir)
      # etf.merge_df(df)
      break


def get_latest(etf):
  for f in os.listdir(settings.PICKLE_DIR):
    if len(f.split('.')) < 2:
      # print(f.split('-'))
      if f.split('-')[0] == etf.name:
        return datetime.datetime.strptime(f.split('-')[-1], settings.DATETIME_FORMAT)
  return None


def set_latest(etf):
  open(os.path.join(settings.PICKLE_DIR,
                    etf.name + "-" + sorted(os.listdir(settings.CSV_DIR))[-1]), 'a')


def download_all():
  if settings.DEBUG:
    print('downloading...')

  # Download new data to csv/ directory
  for etf in settings.get_etfs():
    if latest := get_latest(etf):
      if datetime.datetime.now() - latest < datetime.timedelta(days=1):
        continue
        # No data on Saturdays and Sundays
        # if datetime.datetime.now().weekday() < 6:
        #   continue
    download(etf)

  # Populate ETFs with new data
  for etf in settings.get_etfs():
    for date_dir in os.listdir(settings.CSV_DIR):
      if not settings.FORCE_REPOPULATE:
        if latest := get_latest(etf):
          if datetime.datetime.strptime(date_dir, settings.DATETIME_FORMAT) <= latest:
            continue
      update_etf_with_new_data(etf, date_dir)


def calc_changes(etf):
  if settings.DEBUG:
    print(f"calculating {etf.name}")
  # Shares
  try:
    shares_df = pd.read_pickle(os.path.join(
        settings.PICKLE_DIR, etf.name+'-shares.pickle'))
    shares_df_diff = shares_df.diff().dropna(how='all')
  except FileNotFoundError as e:
    return
  print(shares_df)
  # Remove zeros
  #shares_df_diff = shares_df_diff.loc[~(shares_df_diff==0).all(axis=1)]
  shares_df_diff = shares_df_diff[(shares_df_diff.T != 0).all()]

  # Marketcap
  marketcap_df = pd.read_pickle(os.path.join(
      settings.PICKLE_DIR, etf.name+'-marketcap.pickle'))
  # marketcap_df_diff = marketcap_df.diff().dropna()
  # Remove zeros
  # marketcap_df_diff = marketcap_df_diff[(marketcap_df_diff.T != 0).any()]
  print(shares_df_diff)
  if not shares_df_diff.empty:
    changes_path = os.path.join(
        settings.PICKLE_DIR, etf.name + '-changes.pickle')
    try:
      changes_df = pd.read_pickle(changes_path)
    except FileNotFoundError as e:
      changes_df = pd.DataFrame()
    for ticker in shares_df_diff:
      for date, diff in shares_df_diff[ticker].iteritems():
        if diff == 0:
          continue

        tmp_df = pd.DataFrame(data={
            'date': date,
            'ticker': [ticker],
            'diff': diff,
            'd2mc': diff/marketcap_df[ticker][date]
        })
        tmp_df.set_index('date', inplace=True)
        changes_df = pd.concat([
            changes_df,
            tmp_df
        ])
    changes_df.to_pickle(changes_path)
    print(changes_df)


def calc_all():
  if settings.DEBUG:
    print('calculating changes...')

  for etf in settings.get_etfs():
    if latest := get_latest(etf):
      if datetime.datetime.now() - latest > datetime.timedelta(days=1):
        continue
    calc_changes(etf)


def populate_out():
  # Convert pickles to csv
  for pickle in os.listdir(settings.PICKLE_DIR):
    if 'changes.pickle' in pickle.split('-'):
      df = pd.read_pickle(os.path.join(settings.PICKLE_DIR, pickle))
      df.to_csv(os.path.join(settings.OUT_DIR, pickle.split('.')[0]+'.csv'))


if __name__ == "__main__":
  import time
  start = time.time()
  read_etfs()
  if settings.DOWNLOAD:
    download_all()
  calc_all()
  #populate_out()
  for etf in settings.get_etfs():
    set_latest(etf)
  print(time.time()-start)
