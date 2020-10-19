import os
import pandas as pd
import settings
import etf
import json
from ark import *
from flask import Flask, render_template, Response, request, jsonify
app = Flask(__name__)


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


@app.route('/api/etfs/<int:id>/changes/')
def etf_changes(id):
  date = request.args.get('date')
  try:
    changes_df = pd.read_pickle(os.path.join(
        'pickle', settings.get_etfs()[id].name + "-changes.pickle"))
  except FileNotFoundError as e:
    return jsonify({'error': 'not available'})
  changes_df = etf.ETFDataframe(changes_df)
  return jsonify(changes_df.serialize())


@app.route('/api/etfs/<int:id>/')
def etf_id(id):
  return jsonify(settings.get_etfs()[id].to_dict())


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
  app.run(host='0.0.0.0', port=3334, debug=True)
  t.join()
