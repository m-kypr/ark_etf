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

            shares_df = csv_df.filter(['shares'])
            shares_df = shares_df.T
            shares_df.index = [pd.to_datetime(date_dir)]

            all_shares_path = os.path.join(
                pickle_dir, csv_name+'-shares.pickle')
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
                pickle_dir, csv_name+'-marketcap.pickle')

            try:
                all_marketcap_df = pd.read_pickle(all_marketcap_path)
            except FileNotFoundError as e:
                all_marketcap_df = pd.DataFrame()

            marketcap_df = marketcap_df.drop(['NAN'], axis=1, errors='ignore')

            all_marketcap_df = pd.concat([all_marketcap_df, marketcap_df])
            all_marketcap_df.sort_index(inplace=True)

            all_marketcap_df.to_pickle(all_marketcap_path)

    # for file in os.listdir(pickle_dir):
    #     if len(file.split('.')) < 2:
    #         os.remove(os.path.join(pickle_dir, file))

    # for pickle in os.listdir(pickle_dir):
    #     df = pd.read_pickle(os.path.join(pickle_dir, pickle))
    #     df.to_csv(os.path.join(out_dir, pickle.split('.')[0]+'.csv'))

    # indicator = os.path.join(pickle_dir, os.listdir(csv_dir)[-1])
    # open(indicator, 'a')

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
                etf_path = os.path.join(
                    pickle_dir, etf+'-changes.pickle')
                try:
                    etf_df = pd.read_pickle(etf_path)
                except FileNotFoundError as e:
                    etf_df = pd.DataFrame()

                data = {'date': [list(shares_df_diff[ticker].keys())[
                    i]], 'ticker': [ticker], 'diff': [lbuf[i]], 'diff2marketcap': [lbuf[i]/marketcap_df[ticker].to_list()[i]*100]}
                tmp_df = pd.DataFrame(data=data)

                etf_df = pd.concat([etf_df, tmp_df])

                etf_df.to_pickle(etf_path)

for file in os.listdir(pickle_dir):
    if len(file.split('.')) < 2:
        os.remove(os.path.join(pickle_dir, file))

for pickle in os.listdir(pickle_dir):
    if 'changes.pickle' in pickle.split('-'):
        df = pd.read_pickle(os.path.join(pickle_dir, pickle))
        df.to_csv(os.path.join(out_dir, pickle.split('.')[0]+'.csv'))

indicator = os.path.join(pickle_dir, os.listdir(csv_dir)[-1])
open(indicator, 'a')
# for date in date_dirs:
#     date_datetime = datetime.datetime.strptime(date, DATETIME_FORMAT)
#     if latest is None | date_datetime > latest:
#         print(date)
#         date_dir = os.path.join(csv_dir, date)
#         if os.path.isdir(date_dir):
#             for csv_file in os.listdir(date_dir):
#                 csv_path = os.path.join(date_dir, csv_file)
#                 # ft_pickle_path = os.path.join(
#                 #     pickle_dir, csv_file.split('.')[0]+'-'+datetime.datetime.strftime(date_datetime)+'.pickle')
#                 latest_pickle = os.path.join(pickle_dir, csv_file.split('.')[
#                     0]+'-'+datetime.datetime.strftime(latest, DATETIME_FORMAT)+'.pickle')
#                 if os.path.isfile(latest_pickle):
#                     ftdf = pd.read_pickle(latest_pickle)
#                 else:
#                     ftdf = pd.DataFrame()
#                 csvdf = pd.read_csv(csv_path)
#                 csvdf.drop(csvdf.tail(3).index, inplace=True)
#                 csvdf['ticker'] = csvdf['ticker'].astype(str)
#                 csvdf.set_index('ticker', inplace=True)
#                 csvdf = csvdf.filter(['shares'])
#                 csvdf = csvdf.T
#                 csvdf.index = [pd.to_datetime(date)]
#                 for ticker in csvdf.columns.to_list():
#                     if ticker not in ftdf and ticker in csvdf.columns.to_list():
#                         if ticker.lower() not in ['nan']:
#                             ftdf[ticker] = ""
#                         else:
#                             del csvdf[ticker]

#             #         | TSLA | AAPL | ...
#             # 200820  | 1111 |   69 | ...
#             # 210820  |   11 |   12 | ...
#             #   .     |    . |    . | ...
#             #   .     |    . |    . | ...

#             ftdf = pd.concat([ftdf, csvdf])
#             ftdf.to_pickle(os.path.join(
#                 pickle_dir, csv_file.split('.')[0]+'-'+date+'.pickle'))
#             ftdf.to_csv(csv_file)
# done = set()
# for csv in csv_files:
#     csv_name = csv.split('-')[0]
#     if csv_name not in done:
#         done.add(csv_name)
#         csv_path = os.path.join(csv_dir, csv)
#         pickle_path = os.path.join(pickle_dir, csv_name+'.pickle')
#         date = datetime.datetime.strptime(
#             csv.split('-')[-1].split('.')[0], DATETIME_FORMAT)
#         if not os.path.isfile(pickle_path):
#             df = pd.DataFrame()
#             df.to_pickle(pickle_path)
#         else:
#             df = pd.read_pickle(pickle_path)
# df['ticker'] =
# df = pd.read_csv(csv_path)
# df.drop(df.tail(3).index, inplace=True)
# print(df)
# pdfs = [x.split("/")[-1] for x in files]
# symbols = set()
# for pdf in pdfs:
#     pdf_path = os.path.join(tmp_dir, pdf)
#     if dl:
#         raw = parser.from_file(pdf_path)
#         txt = raw['content']
#         open(pdf_path.split('.')[0] + '.txt', 'w').write(txt)
#     else:
#         txt = open(pdf_path.split('.')[0] + '.txt', 'r').read()
#     txt = [x for x in txt.split('\n')[42:-27] if x]
#     from datetime import datetime
#     date = datetime.strptime(txt[0].split(' ')[-1], '%m/%d/%Y')
#     titles = ['Ticker', 'CUSIP', 'Shares', 'Market Value', 'Weight']
#     txt = txt[2:]
#     dataset = [row.split("   ")[1].split(" ")[-5:] for row in txt]
#     df = pd.DataFrame(data=dataset, columns=titles)
#     df['Shares'] = df['Shares'].str.replace(',', '').astype(int)
#     df['Market Value'] = df['Market Value'].str.replace(
#         ',', '').astype(float)
#     df['Weight'] = df['Weight'].astype(float)
#     df['S2MV'] = df['Shares']/df['Market Value']
#     if dl_stock_csv:
#         for sym in list(df['Ticker'].to_dict().values()):
#             if sym not in symbols:
#                 symbols.add(sym)
#                 import time
#                 now = int(time.time())
#                 then = now - 31622400
#                 url = f"https://query1.finance.yahoo.com/v7/finance/download/{sym}?period1={then}&period2={now}&interval=1d&events=history"
#                 r = requests.get(url)

#                 csv_folder = os.path.join(tmp_dir, "stock_csv")
#                 try:
#                     os.mkdir(csv_folder)
#                 except FileExistsError as e:
#                     pass
#                 sym_csv = os.path.join(csv_folder, sym) + "-" + \
#                     str(then) + "-"+str(now)+".csv"
#                 open(sym_csv, "wb").write(r.content)
#                 dfsym = pd.read_csv(sym_csv)
#                 if not dfsym.empty:
#                     # dfsym.to_csv(sym_csv)
#                     dfsym.index = pd.to_datetime(dfsym['Date'])
#                 else:
#                     os.remove(sym_csv)
#                     print(f"empty: {sym}    {url}")

#     csv_path = os.path.join(
#         csv_dir, f"{pdf.split('.')[0]}-{datetime.strftime(date,'%m%d%y')}.csv")
#     open(csv_path, 'a').close()
#     df.to_csv(path_or_buf=csv_path)
