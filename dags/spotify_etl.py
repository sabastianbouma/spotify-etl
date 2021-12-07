def run_extraction():
    import pandas as pd
    import requests
    import json
    import datetime
    from sqlalchemy import create_engine
    import time
    from IPython.core.display import clear_output
    import spotipy
    from spotipy.oauth2 import SpotifyClientCredentials
    import spotipy.util as util
    import os

    ### Last.fm Connection ###
    LASTFM_KEY = os.environ.get('LASTFM_KEY')
    LASTFM_SECRET = os.environ.get('LASTFM_SECRET')
    LASTFM_USERNAME = 'sabbouma10'
    LASTFM_NOW_PLAYING = '@attr'
    LASTFM_URL = 'https://ws.audioscrobbler.com/2.0/'


    ### SQL Connection ###
    SQL_PASSWORD = os.environ.get('SQL_PASSWORD')
    SQL_DB = os.environ.get('SQL_DATABASE')
    engine = create_engine('mysql+mysqlconnector://root:{password}@localhost:3306/{database}'.format(
        password=SQL_PASSWORD,
        database=SQL_DB
    ))
    connection = engine.connect()

    ### Spotify Connection ###
    SPOTIFY_CLIENT_ID = os.environ.get('SPOTIFY_CLIENT_ID')
    SPOTIFY_CLIENT_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET')
    SPOTIFY_USERNAME = os.environ.get('SPOTIFY_USERNAME')
    SPOTIFY_PLAYLIST_ID = os.environ.get('SPOTIFY_PLAYLIST_ID')

    scope = "playlist-read-private "
    scope += "playlist-modify-public "
    scope += "playlist-modify-private "
    scope += "user-read-playback-state "
    scope += "user-library-read "
    scope += "user-read-recently-played"

    token = util.prompt_for_user_token(SPOTIFY_USERNAME,scope,client_id=SPOTIFY_CLIENT_ID,client_secret=SPOTIFY_CLIENT_SECRET,redirect_uri='https://developer.spotify.com/dashboard/applications/09d24c54f26846ffa2d5a8558bec67ec') 
    sp = spotipy.Spotify(auth=token)

    def lastfm_get(payload):
        headers = {'user-agent': LASTFM_USERNAME}

        payload['api_key'] = LASTFM_KEY
        payload['format'] = 'json'

        response = requests.get(LASTFM_URL, headers=headers, params=payload)
        return response

    page = 1
    total_pages = 99999
    responses = []
    old_page = -1
    page_state = 0
    with open("current_page_lastfm.txt", "r") as f:
        try:
            old_page = int(f.read()) - 1
            page_state = 1
        except ValueError as e:
            raise ValueError('File does not contain a valid page number')

    while page <= total_pages:

        payload = {
            'method': 'user.getrecenttracks',
            'user':LASTFM_USERNAME,
            'limit':200,
            'page':page
        }

        # print some output so we can see the status
        print("Requesting page {}/{}".format(page, total_pages))
        # clear the output to make things neater
        clear_output(wait = True)

        # make the API call
        response = lastfm_get(payload)

        # if we get an error, print the response and halt the loop
        if response.status_code != 200:
            print(response.text)
            break
        if page_state == 1:
            total_pages = int(response.json()['recenttracks']['@attr']['totalPages'])
            total_pages = total_pages - old_page + 1
            page_state = 2
        elif page_state == 0:
            total_pages = int(response.json()['recenttracks']['@attr']['totalPages'])
            
        
        # extract pagination info
        page = int(response.json()['recenttracks']['@attr']['page'])
        

        # append response
        responses.append(response)

        # if it's not a cached result, sleep
        if not getattr(response, 'from_cache', False):
            time.sleep(0.25)

        # increment the page number
        page += 1

    with open("current_page_lastfm.txt", "w") as f:
        f.write("{}".format(page+old_page))

    
    # data = r.json()

    song_names = []
    artist_names = []
    album_names = []
    timestamps = []

    # Extracting only the relevant bits of data from the json object
    for response in responses:
        data = response.json()      
        for song in data['recenttracks']["track"]:
            if LASTFM_NOW_PLAYING in song.keys():
                continue
            song_names.append(song["name"])
            artist_names.append(song["artist"]["#text"])
            album_names.append(song["album"]["#text"])
            timestamps.append(datetime.datetime.fromtimestamp(int(song['date']['uts'])))
        
    # Prepare a dictionary in order to turn it into a pandas dataframe below       
    song_dict = {
        "song_name" : song_names[::-1],
        "artist_name": artist_names[::-1],
        "album_name" : album_names[::-1],
        "timestamp" : timestamps[::-1]
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "album_name", "timestamp"])

    while(len(song_df.loc[song_df.duplicated(subset=['timestamp']), 'timestamp'])>0):
        song_df.loc[song_df.duplicated(subset=['timestamp']), 'timestamp']+=datetime.timedelta(0,1)
    
    req = connection.execute(
        """
            SELECT
            MAX(timestamp)
        FROM
            scrobbles_raw
        """
    )
    song_df = song_df.loc[song_df['timestamp']>req.fetchall()[0][0]]
    song_df.to_sql('scrobbles_raw', con=connection, index=False, if_exists='append')

    NAMES = ['Exmilitary',
        'The Perfect Prescription',
        'Microphones in 2020',
        '2049 (DELUXE EDITION)',
        'Guruh Gipsy']
    LABELS = ['Third Worlds',
        'Glass',
        'P.W. Elverum & Sun',
        'Toasty Digital',
        'Pramaqua']
    ARTS = ['https://e.snmc.io/i/fullres/w/261b220ac09be6686d078e5f7e1a27b1/7024368',
        'https://e.snmc.io/i/fullres/w/21af1a0b9cce1179443bd4559ead6385/1448700',
        'https://e.snmc.io/i/fullres/w/2a91f1444ba652361e2d58d57c2b33d9/8449827',
        'https://e.snmc.io/i/fullres/w/67e3581a08d9378d52383cec624e0011/8438288',
        'https://cdn2.albumoftheyear.org/345x/album/78896-guruh-gipsy.jpg']
    YEARS = [2011, 1987, 2020, 2020, 1977]
    RELEASE_DATES = ["2011-05-27", "1987-09-01","2020-08-07","2020-09-07","1977-12-01"]
    N_TRACKS = [13, 8, 1, 18, 7]

    local_files = {
        NAMES[i]:{
            'Label':LABELS[i],
            'Year':YEARS[i],
            'Release Date':datetime.datetime.strptime(RELEASE_DATES[i], "%Y-%m-%d"),
            'Album Art':ARTS[i],
            'No. Tracks':N_TRACKS[i],
            'Popularity':-1,
            'Spotify ID':-1
        } for i in range(len(NAMES))
    }
    FIX_KEYS = ['Green', 'Zombie','New Mexico','Mambo Nassau Remastered','Souvlaki','Wait Long By the River and the Bodies of Your Enemies Will Float By', 'Paebiru']
    FIX_YEARS = [1986, 1977, 1982, 1981, 1993, 2005, 1975]
    non_local_files = {FIX_KEYS[i]:FIX_YEARS[i] for i in range(len(FIX_KEYS))}

    def add_albums(album_subset:list, all_albums:list):
        if len(album_subset)>0:
            response = sp.albums(album_subset)
            for album in response['albums']:
                all_albums.append(album)
            print("Completed requesting  {}/{} albums ".format(count, max_iter))
            clear_output(wait = True)
            album_subset = []
        return album_subset, all_albums

    page = 1
    total_pages = 9999
    responses = []
    limit_playlist = 100

    while page <= total_pages:
        print("Requesting playlist page {}/{}".format(page, total_pages))
        clear_output(wait = True)
        response = sp.playlist_tracks(SPOTIFY_PLAYLIST_ID, fields=None, limit=limit_playlist, offset=(page-1)*limit_playlist, market=None)
        if len(response['items']) == 0:
            print("Failed:\n{}".format(response))
            break
        total_pages = int(int(response['total'])/100)+1
        responses.append(response)
        page += 1

    album_ids = [
        album['track']['album']['id']
        if album['track']['album']['id']!=None
        else album['track']
        for response in responses
        for album in response['items']
    ]

    added_ats = [
        datetime.datetime.strptime(album['added_at'], "%Y-%m-%dT%H:%M:%SZ")
        for response in responses
        for album in response['items']
    ]

    max_iter = len(album_ids)
    count = 0
    lim=20
    all_albums = []
    album_subset = []

    while count<max_iter:
        album_id = album_ids[count]
        if type(album_id)!=str:
            album_subset, all_albums = add_albums(album_subset, all_albums)
            all_albums.append(album_id)
        elif len(album_subset)<lim:
            album_subset.append(album_id)
        else:
            album_subset, all_albums = add_albums(album_subset, all_albums)
            continue
        count+=1
        if count==max_iter:
            album_subset, all_albums = add_albums(album_subset, all_albums)


    
    album_names = []
    artist_names = []
    release_dates = []
    years = []
    n_tracks = []
    labels = []
    popularities = []
    spotify_ids = []
    album_arts = []

    for album in all_albums:
        if album['type']=='track':
            name = album['album']['name']
            album_names.append(name)
            artist_names.append(album['artists'][0]['name'])
            if name in local_files.keys():
                local_album = local_files[name]
                release_dates.append(local_album['Release Date'])
                years.append(local_album['Year'])
                n_tracks.append(local_album['No. Tracks'])
                labels.append(local_album['Label'])
                popularities.append(local_album['Popularity'])
                spotify_ids.append(local_album['Spotify ID'])
                album_arts.append(local_album['Album Art'])
        
        else:
            name = album['name']
            album_names.append(name)
            artist_names.append(album['artists'][0]['name'])
            date = album['release_date']
            try:
                release_date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
            except:
                try:
                    release_date = datetime.datetime.strptime(date, "%Y").date()
                except:
                    release_date = datetime.datetime.strptime(date, "%Y-%m").date()
            release_dates.append(release_date)
            if name in non_local_files.keys():
                years.append(non_local_files[name])     
            else:
                years.append(release_date.year)
            n_tracks.append(int(album['total_tracks']))
            labels.append(album['label'])
            popularities.append(int(album['popularity']))
            spotify_ids.append(album['id'])
            album_arts.append(album['images'][0]['url'])

    album_dict = {
        "Album":album_names,
        "Album_Artist":artist_names,
        "Release_Date":release_dates,
        "Year":years,
        "No._Tracks":n_tracks,
        "Label":labels,
        "Popularity":popularities,
        "Spotify_ID":spotify_ids,
        "Album_Art":album_arts,
        "Date_Added":added_ats
    }

    spotify_album_df = pd.DataFrame(album_dict)

    req = connection.execute(
        """
        SELECT
            Spotify_ID
        FROM
            spotify_albums_raw;
        """
    )
    ids = [i[0] for i in req.fetchall()]
    spotify_album_df = spotify_album_df.loc[~spotify_album_df['Spotify_ID'].astype(str).isin(ids)]

    req = connection.execute(
        """
        SELECT
            MAX(spotify_albums_raw.index)
        FROM
            spotify_albums_raw
        """
    )
    current_index = req.fetchall()[0][0]+1
    spotify_album_df.index = range(current_index, current_index +len(spotify_album_df))

    try:
        artists_df = pd.read_sql('artists_raw', con=connection)
        current_index = max(artists_df['index'])
        indexes = []
        new_indexes = []
        new_artists = []
        for i in spotify_album_df.index:
            try:
                indexes.append(artists_df.loc[artists_df['Name']==spotify_album_df.loc[i,'Album_Artist'], 'index'].iloc[0])
            except:
                current_index+=1
                indexes.append(current_index)
                new_indexes.append(current_index)
                new_artists.append(spotify_album_df.loc[i,'Album_Artist'])
        spotify_album_df['Album_Artist_ID'] = indexes
        new_artists_df = pd.DataFrame(new_artists, index=new_indexes, columns=['Name'])
    except:
        new_artists_df = pd.DataFrame(spotify_album_df['Album_Artist'].unique(), columns=['Name'])
        spotify_album_df['Album_Artist_ID'] = [artists_df.loc[artists_df['Name']==i].index[0] for i in spotify_album_df['Album_Artist']]

    new_artists_df.to_sql('artists_raw', con=connection, if_exists='append')
    spotify_album_df.drop('Album_Artist',axis=1, inplace=True)
    spotify_album_df.to_sql('spotify_albums_raw', con=connection, if_exists='append')

    try:
        connection.execute(
            """
            ALTER TABLE `spotify`.`artists_raw` 
            CHANGE COLUMN `index` `index` BIGINT NOT NULL ,
            CHANGE COLUMN `Name` `Name` TEXT NOT NULL ,
            ADD PRIMARY KEY (`index`),
            ADD UNIQUE INDEX `index_UNIQUE` (`index` ASC) VISIBLE;
            ;
            """
        )
        connection.execute(
            """
            ALTER TABLE `spotify`.`spotify_albums_raw` 
            CHANGE COLUMN `index` `index` BIGINT NOT NULL ,
            CHANGE COLUMN `Album` `Album` TEXT NOT NULL ,
            CHANGE COLUMN `Album_Artist_ID` `Album_Artist_ID` BIGINT NOT NULL ,
            CHANGE COLUMN `Release_Date` `Release_Date` DATETIME NOT NULL ,
            CHANGE COLUMN `Year` `Year` BIGINT NOT NULL ,
            CHANGE COLUMN `No._Tracks` `No._Tracks` BIGINT NOT NULL ,
            CHANGE COLUMN `Label` `Label` TEXT NOT NULL ,
            CHANGE COLUMN `Popularity` `Popularity` BIGINT NOT NULL ,
            CHANGE COLUMN `Spotify_ID` `Spotify_ID` TEXT NOT NULL ,
            CHANGE COLUMN `Album_Art` `Album_Art` TEXT NOT NULL ,
            CHANGE COLUMN `Date_Added` `Date_Added` DATETIME NOT NULL ,
            ADD PRIMARY KEY (`index`),
            ADD UNIQUE INDEX `index_UNIQUE` (`index` ASC) VISIBLE;
            ;
            """
        )
    except:
        None


    itunes_songs = pd.read_csv("all_itunes_tracks_spot")
    itunes_songs.loc[itunes_songs['Disk Number'].isna(), 'Disk Number'] = 1
    itunes_songs['Disk Number'] = itunes_songs['Disk Number'].astype(int)
    itunes_songs['Track Number'] = itunes_songs['Track Number'].astype(int)
    itunes_songs['Time'] = itunes_songs['Time'].astype(int)

    spotify_album_df = pd.read_sql('spotify_albums_raw', con=connection)
    artists_df = pd.read_sql('artists_raw', con=connection)

    track_names = []
    spotify_song_ids = []
    track_album_ids = []
    artist_names = []
    times = []
    explicits = []
    track_nos = []
    disc_nos = []

    for album in all_albums:
        if album['type']=='track':
            name = album['album']['name']
            artist = album['artists'][0]['name']
            tracks = itunes_songs.loc[itunes_songs['Album']==name]
            track_names+=list(tracks['Name'])
            spotify_song_ids+=[-1]*len(tracks)
            album_id = spotify_album_df.loc[(spotify_album_df['Album']==name)].index[0]
            track_album_ids+=[album_id]*len(tracks)
            artist_names+=[artist]*len(tracks)
            times+=list(tracks['Time'])
            explicits+=[-1]*len(tracks)
            track_nos+=list(tracks['Track Number'])
            disc_nos+=list(tracks['Disk Number'])
            
        else:
            album_id = spotify_album_df.loc[spotify_album_df['Spotify_ID']==album['id']].index[0]

            track_album_ids+=[album_id]*len(album['tracks']['items'])
            for track in album['tracks']['items']:
                track_names.append(track['name'])
                spotify_song_ids.append(track['id'])
                artist_names.append(track['artists'][0]['name'] )
                times.append(int(track['duration_ms']/1000))
                explicits.append(track['explicit'])
                track_nos.append(track['track_number'])
                disc_nos.append(track['disc_number'])
                

    spotify_songs_dict = {
        "Name":track_names,
        "Artist":artist_names,
        "Time":times,
        "Explicit":explicits,
        "Track_No.":track_nos,
        "Disc_No.":disc_nos,
        "Spotify_Song_ID":spotify_song_ids,
        "Album_ID":track_album_ids
    }
    spotify_songs_df = pd.DataFrame(spotify_songs_dict)


    req = connection.execute(
        """
        SELECT
            Spotify_Song_ID
        FROM
            spotify_tracks_raw;
        """
    )
    ids = [i[0] for i in req.fetchall()]
    spotify_songs_df = spotify_songs_df.loc[~spotify_songs_df['Spotify_Song_ID'].astype(str).isin(ids)]

    req = connection.execute(
        """
        SELECT
            MAX(spotify_tracks_raw.index)
        FROM
            spotify_tracks_raw
        """
    )
    current_index = req.fetchall()[0][0]+1
    spotify_songs_df.index = range(current_index, current_index +len(spotify_songs_df))

    spotify_songs_df['Artist_ID'] = [artists_df.loc[artists_df['Name']==i].index[0] if len(artists_df.loc[artists_df['Name']==i])>0 else -1 for i in spotify_songs_df['Artist']]
    spotify_songs_df.drop('Artist',axis=1, inplace=True)

    spotify_songs_df.to_sql('spotify_tracks_raw', con=connection, if_exists='append')


def run_transformation():
    import pandas as pd
    import datetime
    from sqlalchemy import create_engine
    import time
    from IPython.core.display import clear_output
    import regex as re
    import numpy as np

    engine = create_engine('mysql+mysqlconnector://root:E2ab6214@localhost:3306/Spotify')
    connection = engine.connect()

    sql_artist_fix = """
        SET SQL_SAFE_UPDATES=0;


        
        UPDATE spotify_albums_raw
            LEFT JOIN
                artists_raw
                ON
                    spotify_albums_raw.Album_Artist_ID = artists_raw.index
        SET spotify_albums_raw.Album_Artist_ID = (
            SELECT
                artists_raw.index
            FROM
                artists_raw
            WHERE
                artists_raw.Name = 'Fela Kuti'
        )
        WHERE
            artists_raw.Name REGEXP '^Fẹla|^Fela';



        UPDATE spotify_tracks_raw
            LEFT JOIN
                artists_raw
                ON
                    spotify_tracks_raw.Artist_ID = artists_raw.index
        SET spotify_tracks_raw.Artist_ID = (
            SELECT
                artists_raw.index
            FROM
                artists_raw
            WHERE
                artists_raw.Name = 'Fela Kuti'
        )
        WHERE
            artists_raw.Name REGEXP '^Fẹla|^Fela';

        

        UPDATE spotify_tracks_raw
            INNER JOIN
                spotify_albums_raw
                ON
                    spotify_tracks_raw.Album_ID = spotify_albums_raw.index
            INNER JOIN
                artists_raw
                ON
                    spotify_albums_raw.Album_Artist_ID = artists_raw.index
        SET spotify_tracks_raw.Artist_ID = (
            SELECT
                artists_raw.index
            FROM
                artists_raw
            WHERE
                artists_raw.Name = 'Sun Ra'
        )
        WHERE
            artists_raw.Name REGEXP '^Sun Ra';


        UPDATE spotify_tracks_raw
            INNER JOIN
                spotify_albums_raw
                ON
                    spotify_tracks_raw.Album_ID = spotify_albums_raw.index
            INNER JOIN
                artists_raw
                ON
                    spotify_albums_raw.Album_Artist_ID = artists_raw.index
        SET spotify_tracks_raw.Artist_ID = (
            SELECT
                artists_raw.index
            FROM
                artists_raw
            WHERE
                artists_raw.Name = 'Various Artists'
        )
        WHERE
            artists_raw.Name REGEXP 'Various Artists';


        UPDATE spotify_tracks_raw
            INNER JOIN
                spotify_albums_raw
                ON
                    spotify_tracks_raw.Album_ID = spotify_albums_raw.index
            INNER JOIN
                artists_raw
                ON
                    spotify_albums_raw.Album_Artist_ID = artists_raw.index
        SET spotify_tracks_raw.Artist_ID = (
            SELECT
                artists_raw.index
            FROM
                artists_raw
            WHERE
                artists_raw.Name = 'Flume'
        )
        WHERE
            artists_raw.Name REGEXP 'Flume';


        UPDATE spotify_tracks_raw
            INNER JOIN
                    spotify_albums_raw
                    ON
                        spotify_tracks_raw.Album_ID = spotify_albums_raw.index
            INNER JOIN
                artists_raw
                ON
                    spotify_albums_raw.Album_Artist_ID = artists_raw.index
        SET
            spotify_albums_raw.Album_Artist_ID = (
                SELECT
                    artists_raw.index
                FROM
                    artists_raw
                WHERE
                    artists_raw.Name = 'Nick Cave & The Bad Seeds'
            ),
            spotify_tracks_raw.Artist_ID = spotify_albums_raw.Album_Artist_ID
        WHERE
            artists_raw.Name REGEXP '^Nick Cave';



        SET SQL_SAFE_UPDATES=1
        """

    for query in sql_artist_fix.split(";"):
        try:
            connection.execute(query)
        except:
            print("SQL error with statement:\n {}".format(query))
            break

    tracks_df = pd.read_sql_query("""
        SELECT
            *
        FROM
            spotify_tracks_raw
        WHERE
            spotify_tracks_raw.index >
            (SELECT
                MAX(tracks.index)
            FROM
                tracks);
        """,
        con=connection)

    artists_df = pd.read_sql_query("""
        SELECT
            *
        FROM
            artists_raw
        WHERE
            artists_raw.index >
            (SELECT
                MAX(artists.index)
            FROM
                artists);
        """,
        con=connection)

    albums_df = pd.read_sql_query("""
        SELECT
            *
        FROM
            spotify_albums_raw
        WHERE
            spotify_albums_raw.index >
            (SELECT
                MAX(albums.index)
            FROM
                albums);
        """,
        con=connection)

    scrobbles_df = pd.read_sql_query("""
        SELECT
            *
        FROM
            scrobbles_raw
        WHERE
            scrobbles_raw.timestamp >
            (SELECT
                MAX(scrobbles.timestamp)
            FROM
                scrobbles);
        """,
        con=connection)

    def fix_album_names(column:list, split:bool=False)->list:
        fix_dict_spotify = {
            '98.12.28 男達の別れ (Live)':'98.12.28 Otokotachi no Wakare',
            'Lift Your Skinny Fists Like Antennas to Heaven':'Lift Yr. Skinny Fists Like Antennas to Heaven!',
            'The Glow, Pt. 2':'The Glow Pt. 2',
            'The Velvet Underground & Nico 45th Anniversary':'The Velvet Underground & Nico',
            'Slow Riot for New Zero Kanada':'Slow Riot for New Zerø Kanada',
            'World Psychedelic Classics 5: Who Is William Onyeabor?':'Who Is William Onyeabor?',
            'Everything is Possible World Psychedelic Classics 1':'Everything is Possible!',
            '宇宙 日本 世田谷':'Uchū Nippon Setagaya',
            "Don’t Say That":"Don't Say That",
            'Flume: Deluxe Edition (Spotify Exclusive)':'Flume (Deluxe Edition)',
            'Paebiru':'Paêbirú',
            'NO ONE EVER REALLY DIES':'NO_ONE EVER REALLY DIES',
            'Power Corruption and Lies':'Power, Corruption & Lies',
            'Operation: Doomsday (Complete)':'Operation: Doomsday',
            'awaken, my love':'"Awaken, My Love!"',
            'Awaken, My Love':'"Awaken, My Love!"',
            "Talking Heads '77 (Deluxe Version)":'Talking Heads: 77',
            "Legacy (The Very Best Of David Bowie, Deluxe)":"Legacy (The Very Best Of David Bowie)",
            "channel ORANGE (Explicit Version)":"channel ORANGE",
            "Man On The Moon: The End Of Day (Int'l Version)":"Man On The Moon: The End Of Day",
            "Live At Sin-é (Legacy Edition)":"Live At Sin-é"
        }

        fix_dict_itunes = {
            'blond':'Blonde',
            'Plantasia':"Mother Earth's Plantasia",
            'Twin Fantasy':'Twin Fantasy (Mirror To Mirror)',
            'Twin Fantasy (2018 Reissue)':'Twin Fantasy',
            '(after)':'After (Live)',
            'Islands (Part 1)':'Islands part 1',
            'Paper Mache Dream Balloon':'Paper Mâché Dream Balloon',
            'Mm.. Food':'MM...FOOD',
            'Operation Doomsday':'Operation: Doomsday',
            'Minecraft: Volume Alpha':'Minecraft - Volume Alpha',
            'The Recordings Of The Middle East':'Recordings Of The Middle East',
            'Lockjaw - EP':'Lockjaw',
            "Songs From the South: Paul Kelly's Greatest Hits":"Songs From The South: Paul Kelly's Greatest Hits 1985-2019",
            'Recurring Dream':'The Very Very Best Of Crowded House',
            'Lift Yr Skinny Fists Like Antennas To Heaven!':'Lift Yr. Skinny Fists Like Antennas to Heaven!'
        }
        if split==True:
            col_name = column.columns[0]
            split_name = column.columns[1]
            column_segment_1 = column.loc[column[split_name]<datetime.datetime(2020, 1, 14), col_name]
            column_segment_2 = column.loc[column[split_name]>=datetime.datetime(2020, 1, 14), col_name]
            for key, val in fix_dict_itunes.items():
                column_segment_1 = column_segment_1.str.replace('^'+re.escape(key)+'$', val, regex=True, flags=re.IGNORECASE)
            for key, val in fix_dict_spotify.items():
                column_segment_2 = column_segment_2.str.replace('^'+re.escape(key)+'$', val, regex=True, flags=re.IGNORECASE)
            column = column_segment_1.append(column_segment_2)
        else:
            for key, val in fix_dict_spotify.items():
                column = column.str.replace('^'+re.escape(key)+'$', val, regex=True, flags=re.IGNORECASE)
        return column

    def fix_remasters(column):
        words = ['[R|r]emaster','[D|d]eluxe', '[E|e]xpanded', '[A|a]nniversary', '[V|v]ersion', '[E|e]dition', '[E|e]xtended','[D|d]igital','[I|i]nstrumental']
        for word in words:
            column = column.str.split(r'(?= [\(\[]{0,1}[\d]{0,4}(th){0,1}\s{0,1}'+word+r')', expand=True).iloc[:,0]
            # column = column.apply(lambda x: ', '.join(re.split(r'(?= [\(\[]{0,1}[\d]{0,4}(th){0,1}\s{0,1}'+word+r')', x, flags=re.IGNORECASE))).str.split(', ', expand=True).iloc[:,0]
        return column

    def fix_remastered_songs(column):
        words = ['[R|r]emaster', '[L|l]ive', '[U|u]nfinished','[M|m]ono','[S|s]tereo','[D|d]igital', 'Instrumental','Single Version','Extended Version','1980 Version', 'LP Mix','Bonus Track']
        for word in words:
            column = column.str.split(r'(?= [\-\(\[]{1}[ ]{0,1}[\d]{0,4}(th){0,1}\s{0,1}'+word+r')', expand=True).iloc[:,0]
        return column

    def fix_scrobbles(scrobbles_df):
        fix_artists = {
            'Fela Kuti':'^Fẹla|^Fela',
            'Nick Cave & The Bad Seeds': '^Nick Cave',
            'Sun Ra': '^Sun Ra',
            'Yusuf / Cat Stevens': 'Cat Stevens|Yusuf',
            'King Gizzard & The Lizard Wizard': '^King Gizzard',
            'Belle & Sebastian': 'Belle and Sebastian',
            'N.W.A.':'N.W.A',
            'Gil Scott-Heron':'^Gil Scott-Heron',
            'Freddie Gibbs':'^Freddie Gibbs',
            "N.E.R.D":"N\*E\*R\*D",
            'Lula Côrtes':'Lula Côrtes e Zé Ramalho',
            '２８１４':'2 8 1 4'
        }
        fix_album_artists = {
            'Flume':'Flume',
            'Sing Street (Original Motion Picture Soundtrack)': 'Various Artists',
            'Japanese Ambient, Environmental & New Age Music 1980-1990': 'Various Artists'
        }
        scrobbles_df.loc[scrobbles_df['album_name']=='untitled unmastered.','song_name'] = scrobbles_df.loc[scrobbles_df['album_name']=='untitled unmastered.','song_name'].str.replace(' l',' |')
        scrobbles_df['artist_name'] = scrobbles_df['artist_name'].str.split(r' [\-\[\()]{0,1}[F|f]eat[.]{0,1}', expand=True).iloc[:,0]
        scrobbles_df['song_name'] = scrobbles_df['song_name'].str.split(r' [\-\[\()]{0,1}[F|f]eat[.]{0,1}', expand=True).iloc[:,0]

        for key, val in fix_artists.items():
            scrobbles_df.loc[scrobbles_df['artist_name'].str.contains(val, flags=re.IGNORECASE, regex=True), 'artist_name'] = key
        for key, val in fix_album_artists.items():
            scrobbles_df.loc[scrobbles_df['album_name']==key, 'artist_name'] = val
            
        return scrobbles_df

    def fix_song_names(df, col):
        fix_songs = {
            'Ecstacy':'Ecstasy',
            'Solar System':'II. Solar System',
            'IV. Mt. Eerie':'IV. Mount Eerie',
            'Things That Would Have Been Helpful To Know Before The Revolution':'Things It Would Have Been Helpful to Know Before the Revolution',
            'The Dead Flag Blues (Intro)': 'The Dead Flag Blues',
            'The King of Carrot Flowers Pt. One':'King of Carrot Flowers Pt. 1',
            'The King of Carrot Flowers Pts. Two & Three':'King of Carrot Flowers Pts. 2 & 3',
            'Two-Headed Boy Pt. Two': 'Two-Headed Boy Pt. 2',
            'Fire Engine On Fire Pt.i':'Fire Engine On Fire Part i',
            'Fire Engine On Fire Pt.ii':'Fire Engine On Fire Part ii',
            'Losing california for drusky':'Losing carolina; for drusky',
            'Happiness in on the outside':'Happiness is on the outside',
            'Beef Rap':'Beef Rapp',
            'Yoshimi Battles the Pink Robots (part 1)':'Yoshimi Battles the Pink Robots, Pt. 1',
            'Yoshimi Battles the Pink Robots (Part 2)':'Yoshimi Battles the Pink Robots, Pt. 2',
            'Approaching Pavonis Mons by Balloon (Utopia Planitia)':'Approaching Pavonis Mons by Balloon',
            '(Something) - 1': 'Deep',
            'Instrumental (2)':'Instrumental - 2',
            'Toothbrush/Trash':'Toothbrush / Trash',
            'Feeler - Michael Brauer Mix':'Feeler',
            'Fall Your Way - Eric Sarafin Mix':'Fall Your Way',
            "Wooly Mammoth's Mighty Absence":"Woolly Mammoth's Mighty Absence",
            'Yeah (Crass Version)':'Yeah',
            'Yeah (Pretentious Version)':'Yeah - Pretentious Mix',
            'Speak To Me/Breathe [Breathe In The Air]':'Breathe (In the Air)',
            'Body is a Blade':'The Body is a Blade',
            'Baby²':'Baby',
            'Slowmo':'Slomo',
            'Normalization':'Normalisation',
            'Mellow Mood for Maidenhair':'A Mellow Mood for Maidenhair',
            'Like Antennas To Heaven…':'Antennas To Heaven',
            'Mr. Follow Follow':'Mister Follow Follow',
            'Leon Take Us Outside':'Leon Takes Us Outside',
            'Day N Night (Nightmare)':'Day N Nite (Nightmare)',
            'Up Up And Away':'Up Up & Away',
            'Enter Galactic (Love Connection Part 1)':'Enter Galactic (Love Connection Part I)',
            'The Queen Is Dead (Take Me Back to Dear Old Blighty) [Medley]':'The Queen Is Dead',
            'The Black Hawk War':'The Black Hawk War, Or, How to Demolish an Entire Civilization and Still Feel Good About Yourself In the Morning, Or, We Apologize for the Inconvenience But You''re Going to Have to Leave Now, Or...',
            'Riffs and Variations on a Single Note':'A conjunction of drones simulating the way in which Sufjan Stevens has an existential crisis in the Great Godfrey Maze',
            'Changeling / Transmission 1':'Transmission 1',
            'Stem / Long Stem / Transmission 2':'Transmission 2',
            'What Does Your Soul Look Like (Part 1): Blue Sky Revisit / Transmission 3':'Transmission 3',
            'Napalm Brain / Scatter Brain':'Napalm Brain / Scatter Brain - Medley',
            "Bitch, Don't Kill My Vibe {Jay Z Remix}":"Bitch, Don't Kill My Vibe - Remix",
            'Swimming Pools (Drank) (extended version)':'Swimming Pools (Drank)',
            'What Does Your Soul Look Like (Part 4)':'What Does Your Soul Look Like - Pt. 4'
        }
        for key, val in fix_songs.items():
            df.loc[df[col].str.lower()==key.lower(), col] = val

        return df[col]

    if len(albums_df)>0:
        albums_df['Album'] = fix_album_names(albums_df['Album'])
        albums_df['Album'] = fix_remasters(albums_df['Album'])

    if len(tracks_df)>0:
        tracks_df['Name'] = fix_remastered_songs(tracks_df['Name'])
        tracks_df['Name'] = fix_song_names(tracks_df,'Name')
        tracks_df['Name'] = tracks_df['Name'].str.split(r' [\-\[\()]{0,1}[F|f]eat[.]{0,1}', expand=True).iloc[:,0]
        tracks_df['Name'] = tracks_df['Name'].str.split(r' [\-\[\()]{0,1}FEAT[.]{0,1}', expand=True).iloc[:,0]
        tracks_df["Name_join"] = tracks_df['Name'].str.replace('[^\w]','')

    if len(scrobbles_df)>0:
        scrobbles_df['album_name'] = fix_album_names(scrobbles_df[['album_name','timestamp']], True)
        scrobbles_df['album_name'] = fix_remasters(scrobbles_df['album_name'])
        scrobbles_df['song_name'] = fix_remastered_songs(scrobbles_df['song_name'])
        scrobbles_df['song_name'] = fix_song_names(scrobbles_df,'song_name')
        scrobbles_df = fix_scrobbles(scrobbles_df)
    scrobbles_df["song_name_join"] = scrobbles_df['song_name'].str.replace('[^\w]','')

    scrobbles_df.to_sql('scrobbles_temp', con=connection, index=False, if_exists='replace')
    albums_df.to_sql('albums', con=connection, index=False, if_exists='append')
    artists_df.to_sql('artists', con=connection, index=False, if_exists='append')
    tracks_df.to_sql('tracks', con=connection, index=False, if_exists='append')

    artist_ids = pd.read_sql_query(
        """
        SELECT
            artists.index
        FROM
            scrobbles_temp
        LEFT JOIN
            artists
            ON
                LOWER(scrobbles_temp.artist_name) = LOWER(artists.Name)
        """
        , con=connection
    )

    scrobbles_df['Artist_ID'] = artist_ids['index'].replace(np.nan, -1)
    scrobbles_df['Artist_ID'] = scrobbles_df['Artist_ID'].astype(int)
    scrobbles_df.drop('artist_name', axis=1, inplace=True)
    scrobbles_df = scrobbles_df.loc[scrobbles_df['Artist_ID']>=0]
    scrobbles_df.reset_index(drop=True, inplace=True)
    scrobbles_df.to_sql('scrobbles_temp', con=connection, index=False, if_exists='replace')

    album_ids = pd.read_sql_query(
        """
        SELECT
            albums.index
        FROM
            scrobbles_temp
        LEFT JOIN
            albums
            ON
                LOWER(scrobbles_temp.album_name) = LOWER(albums.Album)
                AND
                scrobbles_temp.Artist_ID = albums.Album_Artist_ID
        """
        , con=connection
    )

    scrobbles_df['Album_ID'] = album_ids['index'].replace(np.nan, -1)
    scrobbles_df['Album_ID'] = scrobbles_df['Album_ID'].astype(int)
    scrobbles_df.drop('album_name', axis=1, inplace=True)
    scrobbles_df = scrobbles_df.loc[scrobbles_df['Album_ID']>=0]
    scrobbles_df.reset_index(drop=True, inplace=True)
    scrobbles_df.to_sql('scrobbles_temp', con=connection, index=False, if_exists='replace')

    track_ids = pd.read_sql_query(
        """
        SELECT
            timestamp, tracks.index, song_name
        FROM
            scrobbles_temp
        LEFT JOIN
            tracks
            ON
                SUBSTRING(LOWER(scrobbles_temp.song_name_join),1,35) = SUBSTRING(LOWER(tracks.Name_join),1,35)
                AND
                scrobbles_temp.Album_ID = tracks.Album_ID;
        """
        , con=connection
    )

    track_ids = track_ids.drop_duplicates(['timestamp', 'song_name'],keep= 'first')
    track_ids.sort_values(by='timestamp', inplace=True)
    track_ids.reset_index(drop=True, inplace=True)
    scrobbles_df['Song_ID'] = track_ids['index'].replace(np.nan, -1)
    scrobbles_df['Song_ID'] = scrobbles_df['Song_ID'].astype(int)

    scrobbles_df.drop(['song_name_join','song_name'], axis=1, inplace=True)
    scrobbles_df.to_sql('scrobbles', con=connection, index=False, if_exists='append')