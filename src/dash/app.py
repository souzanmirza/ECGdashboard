import time
import numpy as np
import dash
import dash_core_components as dcc
from dash.dependencies import Output, Event
import dash_html_components as html
from data_util import DataUtil
import flask
from flask_caching import Cache

# TODO: Deploy with app with Heroku.


# Setup flask server
server = flask.Flask(__name__)
app = dash.Dash(__name__, server=server)
cache = Cache(app.server, config={
    'CACHE_TYPE': 'filesystem',
    'CACHE_DIR': 'cache-directory'
})
TIMEOUT = 2

app.config['suppress_callback_exceptions'] = True

# Connect query object to database
postgres_config_infile = '../../.config/postgres.config'
query_helper = DataUtil(postgres_config_infile)


@cache.memoize(timeout=TIMEOUT)
def update():
    signames_ecg, signals = query_helper.getLastestECGSamples(10)
    signames_hr, hrvariability, latesthr = query_helper.getHRSamples()
    return signames_ecg, signals, signames_hr, hrvariability, latesthr


@app.callback(
    Output('hr-output', 'children'),
    events=[Event('refresh', 'interval')])
def get_hr_graph():
    """
    Plots heart rate variability for each patient input.
    """
    signames_ecg, signals, signames_hr, hrvariability, latesthr = update()
    return html.Div(style={'height': '30vh'}, className='hr', children=[dcc.Graph(
        id='hr-' + signame,
        style={'width': '100%'},
        figure={
            'data': [
                {'x': np.array(hrvariability[signame])[:, 0],
                 'y': np.array(hrvariability[signame])[:, 1],
                 'type': 'line', 'name': signame}
            ],
            'layout': {
                'title': signame + ' hr',
                'xaxis': {'title': 'time'},
                'yaxis': {'title': 'HR (beats/min'}
            }
        }) for signame in signames_hr])


@app.callback(
    Output('ecg-output', 'children'),
    events=[Event('refresh', 'interval')])
def get_ecg_graph():
    """
    Plots ECG signals for each patient input.
    """
    titles = ['ecg1', 'ecg2', 'ecg3']
    signames_ecg, signals, signames_hr, hrvariability, latesthr = update()
    return html.Div(className='ecg', children=[
        html.Div(style={'display': 'flex', 'height': '30vh', 'border-top': '1px solid grey'},
                 children=[dcc.Graph(
                     id=title + signame,
                     style={'width': '100%'},
                     figure={
                         'data': [
                             {'x': signals[signame]['time'],
                              'y': signals[signame][title],
                              'type': 'line', 'name': signame}
                         ],
                         'layout': {
                             'title': '{}-{}'.format(signame, title),
                             'xaxis': {'title': 'time'},
                             'yaxis': {'title': 'voltage (mv)', 'range': np.linspace(-2.5, 2.5, 10)}
                         }
                     }
                 ) for title in titles]
                          +
                          [html.Div(
                              style={'justify-content': 'center', 'display': 'flex',
                                     'align-items': 'center', 'width': '10vh', 'font-size': '30pt'},
                              children=['{}'.format(latesthr[signame][0])])
                          ]
                 ) for signame in signames_ecg])


# app layout with separate tabs for ECG and HR graphs.
app.layout = html.Div(className='main-app', style={'fontFamily': 'Sans-Serif',
                                                   'margin-top': 'auto'},
                      children=[
                          html.H1(children='ECGdashboard For Monitored Patients'),
                          dcc.Tabs(className="tabs", children=[
                              dcc.Tab(label='ECG Signals', children=html.Div(id='ecg-output')),
                              dcc.Tab(label='HR Variability', children=html.Div(id='hr-output'))
                          ], style={
                              'width': '50vh',
                              'border-style': 'solid',
                              'border-color': 'thin lightgrey solid',
                              'textAlign': 'left',
                              'fontSize': '12pt'
                          }),
                          dcc.Interval(id='refresh', interval=2 * 1000)])

if __name__ == '__main__':
    # Run with sudo python app.py since using privileged port.
    app.run_server(host='0.0.0.0', port=80)
