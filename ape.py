import os
import math
import datetime
import requests
import pandas as pd

DOWNLOAD = True
# DOWNLOAD = False

REPICKLE = True

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

if DOWNLOAD:
    files = open('links.txt').read().split('\n')
    for file in files:
        if file:
            date = datetime.datetime.strftime(
                datetime.datetime.utcnow(), DATETIME_FORMAT)
            date_dir = makedir(os.path.join(csv_dir, date))
            open(os.path.join(date_dir, file.split('/')[-1]), "wb").write(
                requests.get(file).content)

    latest = None
    for file in os.listdir(pickle_dir):
        if len(file.split('.')) < 2:
            latest = datetime.datetime.strptime(file, DATETIME_FORMAT)
            break

    for date_dir in os.listdir(csv_dir):
        if latest:
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
            csv_df = csv_df.filter(['shares'])
            csv_df = csv_df.T
            csv_df.index = [pd.to_datetime(date_dir)]

            pickle_path = os.path.join(pickle_dir, csv_name+'.pickle')
            try:
                all_df = pd.read_pickle(pickle_path)
            except FileNotFoundError as e:
                all_df = pd.DataFrame()

            csv_df = csv_df.drop(['NAN'], axis=1, errors='ignore')

            all_df = pd.concat([all_df, csv_df])
            all_df.sort_index(inplace=True)

            all_df.to_pickle(pickle_path)

    for file in os.listdir(pickle_dir):
        if len(file.split('.')) < 2:
            os.remove(os.path.join(pickle_dir, file))

    for pickle in os.listdir(pickle_dir):
        df = pd.read_pickle(os.path.join(pickle_dir, pickle))
        df.to_csv(os.path.join(out_dir, pickle.split('.')[0]+'.csv'))

    indicator = os.path.join(pickle_dir, os.listdir(csv_dir)[-1])
    open(indicator, 'a')

for etf in os.listdir(pickle_dir):
    sbuf = etf.split('.')
    if len(sbuf) > 1:
        df = pd.read_pickle(os.path.join(pickle_dir, etf))
        df = df.diff().to_dict()
        for ticker in df:
            lbuf = list(df[ticker].values())
            for i in range(0, len(lbuf)):
                if lbuf[i] != 0 and not math.isnan(lbuf[i]):
                    text = f"{sbuf[0]}\n{list(df[ticker].keys())[i]}\n{ticker, lbuf[i]}"
                    open(os.path.join(
                        out_dir, sbuf[0]+'.log'), 'a').write(text)
