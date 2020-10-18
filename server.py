from etf import ETF
import os
import json
import pandas as pd
import datetime
from ape import *
import settings
from flask import Flask, render_template, Response, request, jsonify
app = Flask(__name__)


def build_json(dict):
  return Response(json.dumps(dict), mimetype='application/json')


def build_csv(string):
  return Response(string, mimetype='text/csv')


def update_changes(int: id):
  calc_changes(ETFS[id])


@app.route('/api')
def api():
  return jsonify({'ENDPOINTS': settings.ENDPOINTS})


@app.route('/api/etfs')
def etfs():
  [calc_changes(i) for i in range(len(settings.ETFS))]
  return jsonify([etf.to_dict() for etf in settings.ETFS])


@app.route('/api/etfs/<int:id>/changes')
def etf_changes(id):
  etf = settings.ETFS[id]
  if l := os.listdir('out'):
    print(l)
    print(etf.name)
    for csv in l:
      if f"{etf.name}" in csv:
        return build_csv(open(os.path.join('out', csv)).read())
  return "na"


@app.route('/api/etfs/<int:id>')
def etf_id(id):
  return jsonify(settings.ETFS[id].to_dict())

# @app.route('/json')
# def api():
#   etf = 'all'
#   if etf := request.args.get('etf'):
#     pass
#   if changes_in := request.args.get('changes'):
#     if changes_in == 'latest':
#     latest = datetime.datetime.strptime(
#         [x for x in os.listdir('pickle') if x.split('-') > 2][0], DATETIME_FORMAT)
#     for f in [x for x in os.listdir('pickle') if '-changes.pickle' in x]:
#       changes_df = pd.read_pickle(os.path.join('pickle', f))
#       if
#       return build_csv(changes_df.to_csv())
#       os.listdir('out')


@app.route('/')
def graph():
  return render_template('index.html')


if __name__ == '__main__':
  app.run(host='0.0.0.0', port=3334, debug=True)
