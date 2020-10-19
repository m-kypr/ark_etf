import os
import pandas as pd
import settings
import etf
import json
from ark import *
from flask import Flask, render_template, Response, request, jsonify
app = Flask(__name__)

# "ip": "timestamp"
LATEST_QUERIES = {}


def build_json(string):
  return Response(string, mimetype='application/json')


def build_csv(string):
  return Response(string, mimetype='text/csv')


@app.route('/api/')
def api():
  return jsonify({'ENDPOINTS': settings.ENDPOINTS})


@app.route('/api/etfs/')
def etfs():
  return jsonify([etf.to_dict() for etf in settings.get_etfs()])


@app.route('/api/etfs/<int:id>/')
def etf_id(id):
  return jsonify(settings.get_etfs()[id].to_dict())


@app.route('/api/etfs/<int:id>/changes/')
def etf_changes(id):
  if date := request.args.get('date'):
    # Old data
    return "not implemented"
  # latest data can only be queried once because it reduces work on frontend
  try:
    print(LATEST_QUERIES)
    if request.remote_addr in LATEST_QUERIES.keys():
      # 1 Check per day
      t = time.time() - LATEST_QUERIES[request.remote_addr]
      if t < 60 * 60 * 24:
        return f"Come back in {datetime.datetime(1,1,1) + datetime.timedelta(seconds=t)}s"
    print("here")
    changes_df = pd.read_pickle(os.path.join(
        settings.PICKLE_DIR, settings.get_etfs()[id].name + "-changes.pickle"))
    print(changes_df)
    changes_df = etf.ETFDataframe(changes_df)
    LATEST_QUERIES[request.remote_addr] = int(time.time())
    print(changes_df.serialize())
    return jsonify(changes_df.serialize())
  except FileNotFoundError as e:
    return jsonify({'error': 'not available'})


@app.route('/')
def graph():
  return render_template('index.html')


def update():
  import time
  while True:
    download_all()
    calc_all()
    for etf in settings.get_etfs():
      set_latest(etf)
    # Wait 1 day
    time.sleep(60 * 60 * 24)


if __name__ == '__main__':
  read_etfs()
  from threading import Thread
  t = Thread(target=update)
  t.start()
  app.run(host='0.0.0.0', port=3334, debug=settings.DEBUG)
  t.join()
