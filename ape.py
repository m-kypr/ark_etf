import os
import math
import datetime
import requests
import pandas as pd

DOWNLOAD = True
# DOWNLOAD = False

REPICKLE = True
DEBUG = True

# ----- DONT CHANGE ANYHING BELOW THIS LINE ------


def makedir(dirname):
  dir = os.path.join(os.getcwd(), dirname)
  try:
    os.mkdir(dir)
  except FileExistsError as e:
    pass
  return dir


out_dir = makedir('out')
csv_dir = makedir('csv')
pickle_dir = makedir('pickle')

DATETIME_FORMAT = '%m%d%y'


def readcsv(file_path, name, date_dir):
  csv_df = pd.read_csv(file_path)
  csv_df.drop(csv_df.tail(3).index, inplace=True)
  csv_df['ticker'] = csv_df['ticker'].astype(str)
  csv_df['ticker'] = csv_df['ticker'].apply(lambda x: x.upper())
  csv_df.set_index('ticker', inplace=True)

  shares_df = csv_df.filter(['shares'])
  shares_df = shares_df.T
  shares_df.index = [pd.to_datetime(date_dir)]

  all_shares_path = os.path.join(
      pickle_dir, name+'-shares.pickle')
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

  all_marketcap_path = os.path.join(
      pickle_dir, name+'-marketcap.pickle')

  try:
      all_marketcap_df = pd.read_pickle(all_marketcap_path)
  except FileNotFoundError as e:
      all_marketcap_df = pd.DataFrame()

  marketcap_df = marketcap_df.drop(['NAN'], axis=1, errors='ignore')

  all_marketcap_df = pd.concat([all_marketcap_df, marketcap_df])
  all_marketcap_df.sort_index(inplace=True)

  all_marketcap_df.to_pickle(all_marketcap_path)


def download():
  if DEBUG:
    print('downloading...')
  # Download links.txt to csv/
  files = open('links.txt').read().split('\n')
  for link in files:
    if link:
      content = requests.get(link).content
      date = datetime.datetime.strptime(content[66:80].decode('utf-8').split(',')[0], "%m/%d/%Y")
      date = datetime.datetime.strftime(date, DATETIME_FORMAT)
      date_dir = makedir(os.path.join(csv_dir, date))
      open(os.path.join(date_dir, link.split('/')[-1]), "wb").write(content)

  latest = None
  for file in os.listdir(pickle_dir):
    if len(file.split('.')) < 2:
      latest = datetime.datetime.strptime(file, DATETIME_FORMAT)
      break

  for date_dir in os.listdir(csv_dir):
    if latest and datetime.datetime.strptime(date_dir, DATETIME_FORMAT) <= latest:
      continue
    date_dir_path = os.path.join(csv_dir, date_dir)
    # Read csv to pandas dataframe
    for csv_file in os.listdir(date_dir_path):
      readcsv(os.path.join(date_dir_path, csv_file), csv_file.split('.')[0], date_dir)
      

def calc_changes():
  if DEBUG:
    print('calculating changes')
  etfs = set()
  for e in os.listdir(pickle_dir):
    esp = e.split('-')
    if len(esp) > 1:
      etfs.add(esp[0])
  for etf in etfs:
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
          etf_path = os.path.join(
              pickle_dir, etf+'-changes.pickle')
          try:
            etf_df = pd.read_pickle(etf_path)
          except FileNotFoundError as e:
            etf_df = pd.DataFrame()

          data = {
            'date': [list(shares_df_diff[ticker].keys())[i]], 
            'ticker': [ticker], 
            'diff': [lbuf[i]], 
            'diff2marketcap': [lbuf[i]/marketcap_df[ticker].to_list()[i]*100]}
          tmp_df = pd.DataFrame(data=data)

          etf_df = pd.concat([etf_df, tmp_df])

          etf_df.to_pickle(etf_path)

  """
  for file in os.listdir(pickle_dir):
      if len(file.split('.')) < 2:
          os.remove(os.path.join(pickle_dir, file))
  """

  # Convert pickles to csv
  for pickle in os.listdir(pickle_dir):
      if 'changes.pickle' in pickle.split('-'):
          df = pd.read_pickle(os.path.join(pickle_dir, pickle))
          df.to_csv(os.path.join(out_dir, pickle.split('.')[0]+'.csv'))

  indicator = os.path.join(pickle_dir, os.listdir(csv_dir)[-1])
  open(indicator, 'a')



if __name__ == "__main__":
  if DOWNLOAD:
    download()
  calc_changes()

