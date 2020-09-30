import os
import math
import datetime
import requests
import pandas as pd

"""
This script tracks changes in the portfolio of the 5 ARK ETFs specified in links.txt
It also relates the changes to the marketcap of the stock -> diff2marketcap
Output in csv format in the out/ directory

copyright is with m-kypr 

You can use, redistribute, do whatever with the code.

Have fun.
"""


# KEEP_DAYS = 3 # this is stupid 
REPICKLE = True # YOU PROBABLY DONT WANT TO CHANGE THIS
DOWNLOAD = True # SAME HERE DONT CHANGE IT IF YOU DONT KNOW WHAT IT DOES
DEBUG = True

# ----- DONT CHANGE ANYHING BELOW THIS LINE ------


def mkdir(dirname):
    dir = os.path.join(os.getcwd(), dirname)
    try:
      os.mkdir(dir)
    except FileExistsError as e:
      pass
    return dir


TARGETS = 'links.json'
out_dir = mkdir('out')
csv_dir = mkdir('csv')
pickle_dir = mkdir('pickle')

DATETIME_FORMAT = '%m%d%y'

if DOWNLOAD:
  import json
  tt = json.loads(open(TARGETS).read())
  for trgt in tt:
    if trgt:
      date = datetime.datetime.strftime(datetime.datetime.utcnow(), DATETIME_FORMAT)
      date_dir = mkdir(os.path.join(csv_dir, date))
      open(os.path.join(date_dir, trgt['name'] + '.csv'), "wb").write(requests.get(trgt['csv']).content)
  try:
    latest = datetime.datetime.strptime(open(os.path.join(pickle_dir, 'latest.txt'), 'r').read(), DATETIME_FORMAT)
  except FileNotFoundError:
    latest = None
  for date_dir in os.listdir(csv_dir):
    if latest:
      #dtbuf =  datetime.datetime.strptime(date_dir, DATETIME_FORMAT) 
      #if dtbuf<latest-datetime.timedelta(hours=24*KEEP_DAYS):
      #  import shutil
      #  shutil.rmtree(date_dir)
      #  if DEBUG: print(f'Removing {date_dir} because old')
      #  continue
      if datetime.datetime.strptime(date_dir, DATETIME_FORMAT) <= latest:
        continue
    date_dir_path = os.path.join(csv_dir, date_dir)
    for csv_file in os.listdir(date_dir_path):
      csv_file_path = os.path.join(date_dir_path, csv_file)
      csv_name = csv_file.split('.')[0]
      csv_df = pd.read_csv(csv_file_path)
      csv_df.drop(csv_df.tail(3).index, inplace=True)
      csv_df['ticker'] = csv_df['ticker'].astype(str)
      csv_df['ticker'] = csv_df['ticker'].apply(lambda x: x.upper())
      csv_df.set_index('ticker', inplace=True)

      shares_df = csv_df.filter(['shares'])
      shares_df = shares_df.T
      shares_df.index = [pd.to_datetime(date_dir)]

      all_shares_path = os.path.join(pickle_dir, csv_name+'-shares.pickle')
      try:
        all_shares_df = pd.read_pickle(all_shares_path)
      except FileNotFoundError as e:
        all_shares_df = pd.DataFrame()

      shares_df = shares_df.drop(['NAN'], axis=1, errors='ignore')

      all_shares_df = pd.concat([all_shares_df, shares_df])
      all_shares_df.sort_index(inplace=True)

      all_shares_df.to_pickle(all_shares_path)

      marketcap_df = csv_df.filter(['market value($)'])
      marketcap_df = marketcap_df.T
      marketcap_df.index = [pd.to_datetime(date_dir)]

      all_marketcap_path = os.path.join(pickle_dir, csv_name+'-marketcap.pickle')

      try:
        all_marketcap_df = pd.read_pickle(all_marketcap_path)
      except FileNotFoundError as e:
        all_marketcap_df = pd.DataFrame()

      marketcap_df = marketcap_df.drop(['NAN'], axis=1, errors='ignore')

      all_marketcap_df = pd.concat([all_marketcap_df, marketcap_df])
      all_marketcap_df.sort_index(inplace=True)

      all_marketcap_df.to_pickle(all_marketcap_path)

etfs = set()
for e in os.listdir(pickle_dir):
  esp = e.split('-')
  if len(esp) > 1:
    etfs.add(esp[0])
for etf in etfs:
  # sbuf = etf.split('.')
  shares_df = pd.read_pickle(os.path.join(pickle_dir, etf+'-shares.pickle'))
  marketcap_df = pd.read_pickle(os.path.join(
      pickle_dir, etf+'-marketcap.pickle'))
  shares_df_diff = shares_df.diff().to_dict()
  marketcap_df_diff = marketcap_df.diff().to_dict()
  for ticker in shares_df_diff:
    lbuf = list(shares_df_diff[ticker].values())
    for i in range(0, len(lbuf)):
      if lbuf[i] != 0 and not math.isnan(lbuf[i]):
        marketcap_diff = list(marketcap_df_diff[ticker].values())[i]
        etf_path = os.path.join(pickle_dir, etf+'-changes.pickle')
        try:
          etf_df = pd.read_pickle(etf_path)
        except FileNotFoundError as e:
          etf_df = pd.DataFrame()

        data = {'date': [list(shares_df_diff[ticker].keys())[i]], 'ticker': [ticker], 'diff': [lbuf[i]], 'diff2marketcap': [lbuf[i]/marketcap_df[ticker].to_list()[i]*100]}
        tmp_df = pd.DataFrame(data=data)

        etf_df = pd.concat([etf_df, tmp_df])

        etf_df.to_pickle(etf_path)

# Copy changes to out dir
for pickle in os.listdir(pickle_dir):
  if 'changes.pickle' in pickle.split('-'):
    df = pd.read_pickle(os.path.join(pickle_dir, pickle))
    df.to_csv(os.path.join(out_dir, pickle.split('.')[0]+'.csv'))

# Update latest.txt
open(os.path.join(pickle_dir, 'latest.txt'), 'w').write(os.listdir(csv_dir)[-1])
