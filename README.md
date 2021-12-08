Music Listening Visualisation
============================

This project uses the Spotify and Last FM API's to access data about a user's music library and listening habits.
### Directory layout

    .
    ├── Extraction.ipynb            # Fetches data from Spotify & last.fm and inserts into SQL
    ├── Transformation.ipynb        # Processes data from SQL into clean tables
    │
    ├── current_page_lastfm.txt     # Pagination information
    ├── all_itunes_tracks_spot      # Past data from iTunes
    │
    ├── dags                       
    │   ├── dag_test.py             # DAGS
    │   └── spotify_etl.py          # Code used in DAGS
    │
    ├── dash_app                   
    │   ├── database               
    │   │   └── transforms.py       # Reads in and filters data from SQL
    │   │
    │   ├── tabs                    
    │   │   ├── sidepanel.py        # Displays filtering options
    │   │   ├── tab1.py             # Displays data table
    │   │   └── tab2.py             # Displays scatterplot
    │   │ 
    │   ├── app.py                  # base dash app
    │   └── index.py                # dash app
    │
    ├── requirements.txt
    └── README.md


### Prerequisites + Installation

* pip
  ```sh
  $ pip install -r requirements.txt
  ```
* Spotify API
    
    Create account at https://developer.spotify.com/dashboard/ and retrieve your client id and client secret.
* Last FM API

    Create account at https://www.last.fm/api/account/create and retrieve your client id and client secret.
* SQL server

    Create SQL server and record password and database name.
* .env

    The following information is needed in a .env file:
    * SQL_PASSWORD
    * SQL_DATABASE
    * SPOTIFY_CLIENT_ID
    * SPOTIFY_CLIENT_SECRET
    * LASTFM_KEY
    * LASTFM_SECRET

### Running the project
* Extraction.ipynb
    
    Execute all cells in the notebook. Data will be extracted from the two API's and inserted into SQL tables.
* Transformation.ipynb

    Execute all cells in the notebook. Data from original SQL tables will be processed and inserted into new tables.
* Airflow automation

    Add dags folder to your airflow configuration. The processes will be run daily to keep SQL tables up to date.
* Dash
  ```sh
  $ python3 dash_app/index.py
  ```
  Go to http://localhost:8050/ in your browser and app will be displayed.
