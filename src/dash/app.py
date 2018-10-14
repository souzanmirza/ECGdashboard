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
TIMEOUT = 0

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
                 'mode': 'line', 'name': signame, 'line':{'color':'rgb(0,0,255)'}}
            ],
            'layout': {
                'font': {'color': '#fff'},
                'title': signame + ' hr',
                'xaxis': {'title': 'time', 'color': '#fff', 'showgrid': 'False'},
                'yaxis': {'title': 'HR (beats/min', 'color': '#fff', 'showgrid': 'False'},
                'paper_bgcolor': '#000', 'plot_bgcolor': '#000'
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
    colors = ['rgb(240,0,0)', 'rgb(0,240,0)', 'rgb(0,0,240)']
    signames_ecg, signals, signames_hr, hrvariability, latesthr = update()
    return html.Div(className='ecg', children=[
        html.Div(style={'display': 'flex', 'height': '40vh'},
                 children=[dcc.Graph(
                     id=titles[i] + signame,
                     style={'width': '100%'},
                     figure={
                         'data': [
                             {'x': signals[signame]['time'],
                              'y': signals[signame][titles[i]],
                              'mode': 'line', 'name': signame, 'line': {'color':colors[i]}}
                         ],
                         'layout': {
                             'font': {'color':'#fff'},
                             'title': '{}-{}'.format(signame, titles[i]),
                             'xaxis': {'title': 'time', 'color': '#fff', 'showgrid': 'False'},
                             'yaxis': {'title': 'voltage (mv)', 'color': '#fff', 'showgrid': 'False', 'range': np.linspace(-2.5, 2.5, 10)},
                             'paper_bgcolor':'#000', 'plot_bgcolor':'#000'
                         }
                     }
                 ) for i in range(len(titles))]
                          +
                          [html.Div(
                              style={'justify-content': 'center', 'display': 'flex',
                                     'align-items': 'center', 'width': '10vh', 'font-size': '30pt', 'color': 'white'},
                              children=['{}'.format(latesthr[signame][0])])
                          ]
                 ) for signame in signames_ecg])


# app layout with separate tabs for ECG and HR graphs.
app.layout = html.Div(className='main-app', style={'fontFamily': 'Sans-Serif',
                                                   'backgroundColor':'black', 'color': 'white'},
                      children=[
                          html.H1(children='ECGdashboard For Monitored Patients', style={'margin-top':'0'}),
                          dcc.Tabs(className="tabs", children=[
                              dcc.Tab(label='ECG Signals', style={'backgroundColor':'black',
                              'color':'white'}, children=html.Div(id='ecg-output')),
                              dcc.Tab(label='HR Variability', style={'backgroundColor':'black',
                              'color':'white'}, children=html.Div(id='hr-output'))
                          ], style={
                              'width': '50vh',
                              'textAlign': 'left',
                              'fontSize': '12pt',
                          }),
                          dcc.Interval(id='refresh', interval=2 * 1000)])

if __name__ == '__main__':
    # Run with sudo python app.py since using privileged port.
    app.run_server(debug=True,host='0.0.0.0', port=80)


