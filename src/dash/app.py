import numpy as np
import dash
import dash_core_components as dcc
import dash_html_components as html
from data_util import DataUtil

postgres_config_infile = '../../.config/postgres.config'
# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
# app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app = dash.Dash()
app.config['suppress_callback_exceptions'] = True

query_helper = DataUtil(postgres_config_infile)


# @app.callback(Output('ecg-output', 'children'), [Input('tabs', 'value')])
def get_hr_graph():
    def _get_hr_graph(name, hrvar, heartrate):
        return html.Div(className='hr',
                        style={'display': 'flex', 'height': '30vh'},
                        children=[
                            dcc.Graph(
                                id='hr1-' + name,
                                style={'width': '100%'},
                                figure={
                                    'data': [
                                        {'x': query_helper.getTimestampBounds(signal),
                                         'y': hrvar['ecg1'],
                                         'type': 'line', 'name': name}
                                    ],
                                    'layout': {
                                        'title': name + ' hr1'
                                    }
                                }),
                            html.Div(
                                style={'height': '30vh', 'justify-content': 'center', 'display': 'flex',
                                       'align-items': 'center'},
                                children=['{}'.format(query_helper.getAverageHR(heartrate))])
                        ])
    children = []
    signames, hrvariability, alllatesthr = query_helper.getHRSamples()
    for signame in signames:
        children.append(_get_hr_graph(signame, hrvariability[signame], alllatesthr[signame]))
    return children


# @app.callback(Output('ecg-output', 'children'), [Input('tabs', 'value')])
def get_ecg_graph():
    def _get_ecg_graph(name, signal, latesthr):
        return html.Div(className='ecg',
                        style={'display': 'flex', 'height': '30vh'},
                        children=[
                            dcc.Graph(
                                id='ecg1-' + name,
                                style={'width': '100%'},
                                figure={
                                    'data': [
                                        {'x': query_helper.getTimestampBounds(signal),
                                         'y': signal['ecg1'],
                                         'type': 'line', 'name': name}
                                    ],
                                    'layout': {
                                        'title': name + ' ecg1'
                                    }
                                }
                            ),
                            dcc.Graph(
                                id='ecg2-' + name,
                                style={'width': '100%'},
                                figure={
                                    'data': [
                                        {'x': query_helper.getTimestampBounds(signal),
                                         'y': signal['ecg2'],
                                         'type': 'line', 'name': name}
                                    ],
                                    'layout': {
                                        'title': name + ' ecg2'
                                    }
                                }
                            ),
                            dcc.Graph(
                                id='ecg3-' + name,
                                style={'width': '100%'},
                                figure={
                                    'data': [
                                        {'x': query_helper.getTimestampBounds(signal),
                                         'y': signal['ecg3'],
                                         'type': 'line', 'name': name}
                                    ],
                                    'layout': {
                                        'title': name + ' ecg3'
                                    }
                                }
                            ),
                            html.Div(
                                style={'height': '30vh', 'justify-content': 'center', 'display': 'flex',
                                       'align-items': 'center'},
                                children=['{}'.format(query_helper.getAverageHR(latesthr))])
                        ])

    signames, signals = query_helper.getLastestECGSamples(10)
    children = []
    for signame in signames:
        children.append(_get_ecg_graph(signame, signals[signame], alllatesthr[signame]))
    return children


app.layout = html.Div(className='main-app', children=[
    html.H1(children='ECGdashboard For Monitored Patients'),
    dcc.Tabs(className="tabs", children=[
        dcc.Tab(label='ECG Signals', children=get_ecg_graph())],
             style={
                 'width': '50vh',
                 'border-style': 'solid',
                 'border-color': 'thin lightgrey solid',
                 'textAlign': 'left',
                 'fontSize': '12pt'
             })],
                      style={'fontFamily': 'Sans-Serif',
                             'margin-top': 'auto'}
                      )

# app.layout = html.Div(className='main-app',
#                       children=[
#                           html.H1(children='ECGdashboard For Monitored Patients'),
#                           html.Div(
#                               dcc.Tabs(id="tabs", children=[
#                                   dcc.Tab(label='ECG Signals', children=get_ecg_graph())],
#                                   #dcc.Tab(label='HR Variability', children=[get_ecg_graph()])],
#                                        style={
#                                            'width': '50vh',
#                                            'border-style': 'solid',
#                                            'border-color': 'thin lightgrey solid',
#                                            'textAlign': 'left',
#                                            'fontSize': '12pt'
#                                        }))],
#                       style={
#                           'fontFamily': 'Sans-Serif',
#                           'margin-left': 'auto',
#                           'margin-right': 'auto'}
#                       )

# app.layout =html.Div(className='main-app',
#                  children=[get_ecg_graph()])


if __name__ == '__main__':
    app.run_server()
