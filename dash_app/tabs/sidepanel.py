import dash
import plotly
from dash import dcc
from dash import html
import dash_bootstrap_components as dbc
from dash import dash_table
import pandas
from dash.dependencies import Input, Output
from datetime import date


from app import app

from tabs import tab1, tab2
from database import transforms

output_dictionary = transforms.output_dictionary
start_date = output_dictionary['start_date']
end_date = output_dictionary['end_date']
df = output_dictionary['dataframe']
min_p = df.Year.min()
max_p = df.Year.max()

layout = html.Div([
    html.H1('Scrobble Visualisation')
    , dbc.Row([dbc.Col(
        html.Div([
            html.H2('Filters')
            ,html.Div([html.P() ,html.H5('Grouping'), dcc.RadioItems(id = 'Grouping-select',
                        options=[
                            {'label': 'Song', 'value': 'Song'},
                            {'label': 'Album', 'value': 'Album'},
                            {'label': 'Artist', 'value': 'Artist'}
                        ],
                        value='Song',
                        labelStyle={'display': 'inline-block'}
                    )])
            ,html.Div([html.P() ,html.H5('Sort by'), dcc.RadioItems(id = 'sort-by-select',
                        options=[
                            {'label': 'Plays', 'value': 'Scrobbles'},
                            {'label': 'Total Time', 'value': 'Time'}
                        ],
                        value='Scrobbles',
                        labelStyle={'display': 'inline-block'}
                    )])
            ,html.Div([html.P() ,html.H5('Artist'), dcc.Dropdown(id = 'Artist-drop'
                        ,options=[
                             {'label': i, 'value': i} for i in df['Artist'].fillna('ERROR').unique()],
                        value=[],
                        multi=True
                    )])
            ,html.Div([html.P() ,html.H5('Album'), dcc.Dropdown(id = 'Album-drop'
                        ,options=[
                             {'label': i, 'value': i} for i in df['Album'].fillna('ERROR').unique()],
                        value=[],
                        multi=True
                    )])
            ,html.Div([html.P() ,html.H5('Date Filter'), dcc.DatePickerRange(
                        id='Date-filter',
                        min_date_allowed=start_date,
                        max_date_allowed=end_date,
                        start_date=start_date,
                        end_date=end_date,
                        display_format='D-M-Y'
                    )])                    
            , html.Div([html.H5('Year Slider')
                           , dcc.RangeSlider(id='year-slider'
                                             , min=min_p
                                             , max=max_p
                                             , marks={1950+i*10: str(1950+i*10) for i in range(0,8)}
                                             , value=[1910, 2200]
                                             , tooltip = {"placement": "bottom", 'always_visible': True }
                                             )

                        ])

        ], style={'marginBottom': 30, 'marginTop': 25, 'marginLeft': 0, 'marginRight': 0})
        , width=3)

        , dbc.Col(html.Div([
            dcc.Tabs(id="tabs", value='tab-1', children=[
                dcc.Tab(label='Data Table', value='tab-1'),
                dcc.Tab(label='Scatter Plot', value='tab-2'),
            ])
            , html.Div(id='tabs-content')
        ]), width=9)])

])

def update_options(filters, column_filter, column_return):
    if len(filters)==0:
        return [{'label': i, 'value': i} for i in df[column_return].fillna('ERROR').unique()]
    return [{'label': i, 'value': i} for i in df.loc[df[column_filter].isin(filters),column_return].fillna('ERROR').unique()]

@app.callback(
    Output('Album-drop', 'options'),
    Input('Artist-drop', 'value')
)
def update_albums(artists):
    return update_options(artists, 'Artist','Album')

@app.callback(
    Output('Artist-drop', 'options'),
    Input('Album-drop', 'value')
)
def update_albums(albums):
    return update_options(albums, 'Album','Artist')