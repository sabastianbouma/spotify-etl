import dash
import plotly
from dash import dcc
from dash import html
from dash import dash_table
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc

from app import app
from tabs import sidepanel, tab1, tab2
import dash
from dash.dependencies import Input, Output
import pandas as pd

from database.transforms import plotting_function

app.layout = sidepanel.layout

@app.callback(Output('tabs-content', 'children'),
              [Input('tabs', 'value')])
def render_content(tab):
    if tab == 'tab-1':
        return tab1.layout
    elif tab == 'tab-2':
        return tab2.layout

@app.callback(
    Output('table-sorting-filtering', 'data')
    , [Input('Grouping-select','value'),Input('sort-by-select','value'),Input('table-sorting-filtering', "page_current"), Input('table-sorting-filtering', "page_size")
        , Input('table-sorting-filtering', 'sort_by'), Input('table-sorting-filtering', 'filter_query')
        , Input('year-slider', 'value'), Input('Artist-drop', 'value'), Input('Album-drop', 'value'), Input('Date-filter','start_date'), Input('Date-filter','end_date')])

def update_table(group_type, sort_type, page_current, page_size, sort_by, filter, years, artists,albums,start_date,end_date):
    output_dictionary = plotting_function(start_date=start_date, end_date=end_date, sort_type=sort_type, artists=artists, albums=albums, group_type=group_type, years=range(years[0],years[1]+1))
    dff = output_dictionary['dataframe']
    print("\n\nsort_by={}\nfilter={}\years={}\nartists={}\nalbums={}\ntype(start_date)={}\n\start_date={}\n\n".format(sort_by,filter,years,artists,albums,type(output_dictionary['start_date']),output_dictionary['start_date']))

    if len(sort_by):
        dff = dff.sort_values(
            [col['column_id'] for col in sort_by],
            ascending=[
                col['direction'] == 'asc'
                for col in sort_by
            ],
            inplace=False
        )
    
    page = page_current
    size = page_size
    return dff.iloc[page * size: (page + 1) * size].to_dict('records')

if __name__ == '__main__':
    app.run_server(debug = True)
