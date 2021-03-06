import dash
import plotly
from dash import dcc
from dash import html
import dash_bootstrap_components as dbc
from dash import dash_table
import pandas as pd
from dash.dependencies import Input, Output

from app import app
from database import transforms

output_dictionary = transforms.output_dictionary
df = output_dictionary['dataframe']

PAGE_SIZE = 50

layout =html.Div(dash_table.DataTable(
                            id='table-sorting-filtering',
                            columns=[
                                {'name': i, 'id': i, 'deletable': True} for i in df
                            ],
                            style_data={
                                'whiteSpace': 'normal',
                                'height': 'auto'},

                            style_table={'height':'750px'
                                ,'overflowX': 'auto'},

                            style_data_conditional=[
                                {
                                    'if': {'row_index': 'odd'},
                                    'backgroundColor': 'rgb(248, 248, 248)'
                                }
                            ],
                            style_cell={
                                'height': '90',
                                # all three widths are needed
                                'minWidth': '120px', 'width': '120px', 'maxWidth': '120px', 'textAlign': 'center'
                                ,'whiteSpace': 'normal'
                            }
                            ,style_cell_conditional=[
                                {'if': {'column_id': 'description'},
                                'width': '48%'},
                                {'if': {'column_id': 'title'},
                                'width': '18%'},
                            ]
                            , page_current= 0,
                            page_size= PAGE_SIZE,
                            page_action='custom',

                            filter_action='custom',
                            filter_query='',

                            sort_action='custom',
                            sort_mode='multi',
                            sort_by=[]
                        )
                        )