import dash
from dash import dcc
from dash import html
import dash_bootstrap_components as dbc
import pandas as pd
import plotly.graph_objs as go
from dash.dependencies import Input, Output
from dash import dash_table
from app import app
from database import transforms
from database.transforms import plotting_function
import plotly.express as px


output_dictionary = transforms.output_dictionary
df = output_dictionary['plotting_df']

layout = html.Div(
    [dcc.Graph(id='line-chart')],
    id='table-paging-with-graph-container',
    className="five columns",
)

@app.callback(
    Output('line-chart', "figure")
    , [Input('Grouping-select','value'),Input('sort-by-select','value'),Input('year-slider', 'value'), Input('Artist-drop', 'value'), Input('Album-drop', 'value'), Input('Date-filter','start_date'), Input('Date-filter','end_date')])
def update_graph(group_type, sort_type, years, artists,albums,start_date,end_date):
    output_dictionary = plotting_function(start_date=start_date, end_date=end_date, sort_type=sort_type, artists=artists, albums=albums, group_type=group_type, years=range(years[0],years[1]+1))
    plotting = output_dictionary['plotting_df']

    labels = {'Name':'Song'}
    sort_dict = {'Scrobbles':'Plays','Time':'Total_Time'}
    group_dict = {'Song':'index','Album':'Album_ID','Artist':'Artist_ID'}

    fig = px.line(plotting, x="Date", y=sort_dict[sort_type], color='Name',line_group=group_dict[group_type],labels=labels)
    
    return fig