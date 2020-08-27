from tika import parser
import os
import requests
import pandas as pd

dl = True
dl_stock_csv = False

# ----- DONT CHANGE ANYHING BELOW THIS LINE ------

tmp_dir = os.path.join(os.getcwd(), 'tmp')
csv_dir = os.path.join(os.getcwd(), 'csv')
try:
    os.mkdir(tmp_dir)
except FileExistsError as e:
    pass
try:
    os.mkdir(csv_dir)
except FileExistsError as e:
    pass
files = open('links.txt').read().split('\n')
for file in files:
    path = os.path.join(tmp_dir, file.split("/")[-1])
    if dl:
        r = requests.get(file, allow_redirects=True)
        open(path, "wb").write(r.content)

pdfs = [x.split("/")[-1] for x in files]
symbols = set()
for pdf in pdfs:
    pdf_path = os.path.join(tmp_dir, pdf)
    if dl:
        raw = parser.from_file(pdf_path)
        txt = raw['content']
        open(pdf_path.split('.')[0] + '.txt', 'w').write(txt)
    else:
        txt = open(pdf_path.split('.')[0] + '.txt', 'r').read()
    txt = [x for x in txt.split('\n')[42:-27] if x]
    from datetime import datetime
    date = datetime.strptime(txt[0].split(' ')[-1], '%m/%d/%Y')
    titles = ['Ticker', 'CUSIP', 'Shares', 'Market Value', 'Weight']
    txt = txt[2:]
    dataset = [row.split("   ")[1].split(" ")[-5:] for row in txt]
    df = pd.DataFrame(data=dataset, columns=titles)
    df['Shares'] = df['Shares'].str.replace(',', '').astype(int)
    df['Market Value'] = df['Market Value'].str.replace(
        ',', '').astype(float)
    df['Weight'] = df['Weight'].astype(float)
    df['S2MV'] = df['Shares']/df['Market Value']
    if dl_stock_csv:
        for sym in list(df['Ticker'].to_dict().values()):
            if sym not in symbols:
                symbols.add(sym)
                import time
                now = int(time.time())
                then = now - 31622400
                url = f"https://query1.finance.yahoo.com/v7/finance/download/{sym}?period1={then}&period2={now}&interval=1d&events=history"
                r = requests.get(url)

                csv_folder = os.path.join(tmp_dir, "stock_csv")
                try:
                    os.mkdir(csv_folder)
                except FileExistsError as e:
                    pass
                sym_csv = os.path.join(csv_folder, sym) + "-" + \
                    str(then) + "-"+str(now)+".csv"
                open(sym_csv, "wb").write(r.content)
                dfsym = pd.read_csv(sym_csv)
                if not dfsym.empty:
                    # dfsym.to_csv(sym_csv)
                    dfsym.index = pd.to_datetime(dfsym['Date'])
                else:
                    os.remove(sym_csv)
                    print(f"empty: {sym}    {url}")

    csv_path = os.path.join(
        csv_dir, f"{pdf.split('.')[0]}-{datetime.strftime(date,'%m%d%y')}.csv")
    open(csv_path, 'a').close()
    df.to_csv(path_or_buf=csv_path)
