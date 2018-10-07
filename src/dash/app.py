import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import dash
import dash_core_components as dcc
from dash.dependencies import Output, Event
import dash_html_components as html
from data_util import DataUtil

UPDADE_INTERVAL = 5

app = dash.Dash()
app.config['suppress_callback_exceptions'] = True
postgres_config_infile = '../../.config/postgres.config'
query_helper = DataUtil(postgres_config_infile)


def update(period=UPDADE_INTERVAL):
    signames_ecg, signals = query_helper.getLastestECGSamples(10)
    signames_hr, hrvariability, latesthr = query_helper.getHRSamples()
    return signames_ecg, signals, signames_hr, hrvariability, latesthr
    # time.sleep(period)


@app.callback(
    Output('hr-output', 'children'),
    events=[Event('refresh', 'interval')])
def get_hr_graph():
    signames_hr, hrvariability, latesthr = query_helper.getHRSamples()
    try:
        print(latesthr['mghdata_ts/mgh001'])
    except:
        pass
    return html.Div(className='hr', children=[dcc.Graph(animate=True,
                                   id='hr1-' + signame,
                                   style={'width': '100%'},
                                   figure={
                                       'data': [
                                           {'x': np.array(hrvariability[signame])[:, 0],
                                            'y': np.array(hrvariability[signame])[:, 1],
                                            'type': 'line', 'name': signame}
                                       ],
                                       'layout': {
                                           'title': signame + ' hr1'
                                       }
                                   })for signame in signames_hr])


@app.callback(
    Output('ecg-output', 'children'),
    events=[Event('refresh', 'interval')])
def get_ecg_graph():
    signames_ecg, signals, signames_hr, hrvariability, latesthr = update()
    titles = ['ecg1', 'ecg2', 'ecg3']
    return html.Div(className = 'ecg', children=[
        html.Div(style={'display': 'flex', 'height': '50vh', 'border-top': '1px solid grey'},
                 children=[dcc.Graph(animate=True,
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
                                             'yaxis': {'title': 'voltage (mv)'}
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


app.layout = html.Div(className='main-app', style={'fontFamily': 'Sans-Serif',
                                                   'margin-top': 'auto'},
                      children=[
                          html.H1(children='ECGdashboard For Monitored Patients'),
                          dcc.Tabs(className="tabs", children=[
                              dcc.Tab(label='ECG Signals', children=html.Div(id='ecg-output')),
                                dcc.Tab(label='HR Variability', children=html.Div(id='hr-output'))
                          ],  style={
                     'width': '50vh',
                     'border-style': 'solid',
                     'border-color': 'thin lightgrey solid',
                     'textAlign': 'left',
                     'fontSize': '12pt'
                 }),
                          dcc.Interval(id='refresh', interval=2000)])

if __name__ == '__main__':
    app.run_server()