import settings
from util import makedir
import os
import datetime
import requests


def download(etf):
  if settings.DEBUG:
    print(f'download {etf.link}')
  if not etf:
    return
  content = requests.get(etf.link).content
  # Parse date of csv
  date = datetime.datetime.strptime(
      content[66:80].decode('utf-8').split(',')[0], "%m/%d/%Y")
  date = datetime.datetime.strftime(date, settings.DATETIME_FORMAT)

  # Make date directory
  date_dir = makedir(os.path.join(settings.CSV_DIR, date))

  # Write content to file
  open(os.path.join(date_dir, etf.link.split('/')[-1]), "wb").write(content)
  return 1
