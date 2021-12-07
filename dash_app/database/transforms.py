import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import datetime
from sqlalchemy import create_engine
from dash.dependencies import Input, Output
import dash_table
import os


SQL_PASSWORD = ''
SQL_DB = ''
engine = create_engine('mysql+mysqlconnector://root:{password}@localhost:3306/{database}'.format(
    password=SQL_PASSWORD,
    database=SQL_DB
))
connection = engine.connect()

def plotting_function(start_date:str=None, end_date:str=None, sort_type:str='Scrobbles', artists:list=None, albums:list=None, limit:int=-1, offset:int=0, group_type:str='Song', years:list=range(0,9999), song_length:list=range(0,9999))->None:
    if not isinstance(artists, list):
        artists=[]
    if not isinstance(albums, list):
        albums=[]
    def find_dates(start_date:str=None, end_date:str=None)->list:
        req = connection.execute(
            """
            SELECT
                MIN(timestamp), MAX(timestamp)
            FROM
                scrobbles
            """
        )
        output = req.fetchall()[0]
        if start_date==None:
            start_date = output[0].date()
        else:
            start_date = datetime.datetime.strptime(start_date,'%Y-%m-%d').date()

        if end_date==None:
            end_date = output[1].date()
        else:
            end_date = datetime.datetime.strptime(end_date,'%Y-%m-%d').date()

        return start_date, end_date
    
    start_date, end_date = find_dates(start_date, end_date)
    group_by_dict = {
        'Song':{
            'group_by':'index',
            'group_fields_minor':"tracks.index, tracks.Name, tracks.Album_ID, tracks.Artist_ID, albums.Year",
            'group_fields_major':"A.Name, artists.Name as 'Artist', albums.Album, albums.Year, A.index",
            'join_statement':"""
            LEFT JOIN
                artists
                ON
                    A.Artist_ID=artists.index
            LEFT JOIN
                albums
                ON
                    A.Album_ID=albums.index
            """
            },
        'Album':{
            'group_by':'Album_ID',
            'group_fields_minor':"tracks.Album_ID, albums.Album as 'Name', albums.Album_Artist_ID, albums.Year",
            'group_fields_major':"A.Name, artists.Name as 'Artist', A.Year, A.Album_ID",
            'join_statement':"""
            LEFT JOIN
                artists
                ON
                    A.Album_Artist_ID=artists.index
            """
            },
        'Artist':{
            'group_by':'Artist_ID',
            'group_fields_minor':'tracks.Artist_ID, artists.Name',
            'group_fields_major':"A.Name as 'Artist', A.Artist_ID",
            'join_statement':''
            },

        }

    minor_filter = """
            DATE(scrobbles.timestamp) >= '{start}'
        AND
            DATE(scrobbles.timestamp) <= '{end}'
        AND
            Year IN ({years})
        AND
            TIME IN ({length})
        """.format(
            start = start_date.strftime('%Y/%m/%d'),
            end = end_date.strftime('%Y/%m/%d'),
            years = ",".join(str(i) for i in years),
            length = ",".join(str(i) for i in song_length)
        )
    if len(artists)>0:
        minor_filter += """

            AND
                artists.Name IN ('{artists}')
            """.format(
                artists = "','".join(artists)
            )
    if len(albums)>0:
        minor_filter+="""

                AND
                albums.Album IN ('{albums}')
            """.format(
                albums = "','".join(albums)
        )

  
    minor_query_temp = """
        SELECT
            DATE(scrobbles.timestamp) as 'Date',{group_fields}, SUM(tracks.Time) AS 'Total_Time', COUNT(*) AS 'Plays'
        FROM
            scrobbles
        LEFT JOIN
            tracks
            ON
                scrobbles.Song_ID = tracks.index
        LEFT JOIN
            albums
            ON
                tracks.Album_ID = albums.index
        LEFT JOIN
            artists
            ON
                tracks.Artist_ID = artists.index
        {join_statement}
        WHERE
            {minor_filter}
        GROUP BY
            tracks.{group_by}, DATE(scrobbles.timestamp)
        """
    minor_query = minor_query_temp.format(
        group_fields=group_by_dict[group_type]['group_fields_minor'],
        join_statement='',
        minor_filter=minor_filter,
        group_by = group_by_dict[group_type]['group_by']
    )

    sort_type_dict = {
        'Scrobbles':'Plays',
        'Time':'Total_Time'
    }

    if limit==-1:
        limit_filter='LIMIT 18446744073709551615'
        limit_filter_plot='LIMIT 10'
    else:
        limit_filter='LIMIT {}'.format(limit)
        limit_filter_plot='LIMIT {}'.format(limit)

    major_query_temp = """
        SELECT
            {group_fields}
        FROM
            ({minor_query}) AS A
        {join_statement}
        GROUP BY
            A.{group_by}
        ORDER BY
            SUM({sort_type})
            DESC
        {limit}
        OFFSET
            {offset}
        """

    major_query = major_query_temp.format(
        group_fields=group_by_dict[group_type]['group_fields_major']+", SUM(Plays) AS 'Plays', SUM(Total_Time) AS 'Total Time'",
        minor_query=minor_query,
        join_statement=group_by_dict[group_type]['join_statement'],
        # major_filter=major_filter,
        group_by = group_by_dict[group_type]['group_by'],
        sort_type=sort_type_dict[sort_type],
        limit=limit_filter,
        offset=offset
    )
    plotting_major_query = major_query_temp.format(
        group_fields='A.'+group_by_dict[group_type]['group_by'],
        minor_query=minor_query,
        join_statement=group_by_dict[group_type]['join_statement'],
        # major_filter=major_filter,
        group_by = group_by_dict[group_type]['group_by'],
        sort_type=sort_type_dict[sort_type],
        limit=limit_filter_plot,
        offset=offset
    )
    plotting_filter = """
        INNER JOIN
            ({top_ids}) AS B
            ON
                tracks.{group_by} = B.{group_by}
        """.format(
            group_by=group_by_dict[group_type]['group_by'],
            top_ids=plotting_major_query
        )
    plotting_query = minor_query_temp.format(
        group_fields=group_by_dict[group_type]['group_fields_minor'],
        join_statement=plotting_filter,
        minor_filter=minor_filter,
        group_by = group_by_dict[group_type]['group_by']
    )
    plotting = pd.read_sql_query(
        plotting_query,
        con=connection
    )
    output = pd.read_sql_query(
        major_query,
        con=connection
    )
    max_date = end_date
    for id in plotting[group_by_dict[group_type]['group_by']].unique():
        min_date = min(plotting.loc[plotting[group_by_dict[group_type]['group_by']]==id,'Date'])
        initial = plotting.loc[plotting[group_by_dict[group_type]['group_by']]==id].iloc[0]
        final = plotting.loc[plotting[group_by_dict[group_type]['group_by']]==id].iloc[0]

        initial['Date'] = min_date - datetime.timedelta(days=1)
        initial['Total_Time'] = 0
        initial['Plays'] = 0

        final['Date'] = max_date + datetime.timedelta(days=1)
        final['Total_Time'] = 0
        final['Plays'] = 0

        plotting = plotting.append([initial,final], ignore_index=True)

    plotting["order"] = plotting[group_by_dict[group_type]['group_by']].map({output[group_by_dict[group_type]['group_by']][i]:i for i in range(len(output[group_by_dict[group_type]['group_by']]))})
    plotting.sort_values(by=["order","Date"], ascending=True, inplace=True)

    plotting['Total_Time'] = plotting.groupby([group_by_dict[group_type]['group_by']]).cumsum()['Total_Time']
    plotting['Plays'] = plotting.groupby([group_by_dict[group_type]['group_by']]).cumsum()['Plays']

    output_dictionary = {
        'dataframe':output,
        'start_date':start_date,
        'end_date':end_date,
        'plotting_df':plotting
    }
    return output_dictionary


output_dictionary = plotting_function()
