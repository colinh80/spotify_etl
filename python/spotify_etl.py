# This is a python script to extract tracks from Spotify, transform the data and load it into a PostgreSQL database.
# It is placed inside a function for the corrosponding Airflow DAG to call.
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import datetime
import validation
import psycopg2
from sqlalchemy import create_engine



def spotify_etl_func():

    # Extract data from the spotify api
    client_id = ''
    client_secret = ''
    spotify_redirect_url = "http://localhost"

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id,
                                                   client_secret=client_secret,
                                                   redirect_uri=spotify_redirect_url,
                                                   scope="user-read-recently-played"))
    data = sp.current_user_recently_played(limit=50)

    # Creating lists for track data structure
    timestamp_id =[] # the timestamp for when the track was played
    track_names = [] # the title of the track
    popularity = [] # the popularity of the track
    track_id = [] # the id of the track
    album_id =[] # the id of the album
    artist_id = [] # the id of the artist

    # Append track data to lists
    for track in data["items"]:
        timestamp_id.append(track["played_at"])
        track_names.append(track["track"]["name"])
        popularity.append(track['track']['popularity'])
        track_id.append(track["track"]["id"])
        album_id.append(track['track']["album"]["id"])
        artist_id.append(track['track']['artists'][0]['id'])

    # Collect track lists into a dictionary
    track_dict = {
        "timestamp_id" : timestamp_id,
        "track_name" : track_names,
        "popularity" : popularity,
        "track_id" : track_id,
        "album_id" : album_id,
        "artist_id" : artist_id
    }

    # Creating lists for album data structure
    album_id =[] # the id of the album
    album_title = [] # the title of the album
    album_release_date = [] # the release date of the album
    album_track_count = [] # the number of tracks in the album

    # Append album data to lists
    for album in data['items']:
        album_id.append(album['track']['album']['id'])
        album_title.append(album['track']['album']['name'])
        album_release_date.append(album['track']['album']['release_date'])
        album_track_count.append(album['track']['album']['total_tracks'])

    # Collect album lists into a dictionary
    album_dict = {
        "album_id" : album_id,
        "title" : album_title,
        "release_date" : album_release_date,
        "track_count" : album_track_count
    }

    # Creating lists for artist data structure
    artist_id =[] # the id of the artist
    artist_name = [] # the name of the artist
    artist_url = [] # url for the artist page

    # Append artist data to lists
    # For all artists data, I'm only selecting the first artist listed for the track/album
    for artist in data['items']:
        artist_id.append(artist['track']['artists'][0]['id'])
        artist_name.append(artist["track"]["album"]["artists"][0]["name"])
        artist_url.append(artist['track']['artists'][0]['external_urls']['spotify'])

    # Collect artist lists into a dictionary
    artist_dict = {
        "artist_id" : artist_id,
        "name" : artist_name,
        "url" : artist_url
    }

    # Convert dictionaries to pandas dataframes
    track_df = pd.DataFrame(track_dict, columns=["timestamp_id", "track_name", "popularity", "track_id", "album_id", "artist_id"])
    album_df = pd.DataFrame(album_dict, columns=["album_id", "title", "release_date", "track_count"])
    artist_df = pd.DataFrame(artist_dict, columns=["artist_id", "name", "url"])

    # Format album_df release_date column
    album_df['release_date'] = pd.to_datetime(album_df['release_date'])
    
    # Convert timestamp my local timezone (MST)
    track_df["timestamp_id"] = pd.DatetimeIndex(track_df["timestamp_id"]).tz_convert('MST')

    # Format timestamp column
    track_df['timestamp_id'] = track_df['timestamp_id'].astype(str)
    track_df['timestamp_id'] = track_df['timestamp_id'].str.replace(r'-07:00$', "", regex=True) # removing timezone adjustment value for date filtering
    track_df["timestamp_id"] = pd.to_datetime(track_df["timestamp_id"], format= "%Y-%m-%dT%H:%M:%S.%f")

    # Create datetime filter to only keep tracks played within the last 24 hours
    old_track_count = len(track_df)
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    date_filter = track_df["timestamp_id"] < yesterday
    track_df.drop(index=track_df[date_filter].index, inplace=True)
    new_track_count = len(track_df)
    tracks_removed = old_track_count - new_track_count
    print("Tracks removed: " + str(tracks_removed))

    # Drop rows from artist_df and album_df that corrospond with the dropped rows in track_df after the datetime filter was applied
    artist_df = artist_df[:-tracks_removed]
    album_df = album_df[:-tracks_removed]

   # Drop duplicate values in dimension tables (album_df and artist_df)
    album_df = album_df.drop_duplicates(subset=['album_id'])
    artist_df = artist_df.drop_duplicates(subset=['artist_id'])
    
    # Run validation functions
    if validation.check_if_track_data_is_valid(track_df):
        print("Track data has been validated")
    if validation.check_if_album_data_is_valid(album_df):
        print("Album data has been validated")
    if validation.check_if_artist_data_is_valid(artist_df):
        print("Artist data has been validated")  

    # Load data into PostgreSQL database

    # Database connection
    conn = psycopg2.connect(host = "",port="", dbname = "")
    cur = conn.cursor()
    engine = create_engine('postgresql+psycopg2://@/)
    conn_eng = engine.raw_connection()
    cur_eng = conn_eng.cursor()

    # Load track data into a temp table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_track AS SELECT * FROM spotify_schema.recently_played_tracks LIMIT 0
    """)
    track_df.to_sql("tmp_track", con = engine, if_exists='append', index = False)
    # Move track data from temp table to production table
    cur.execute(
    """
    INSERT INTO spotify_schema.recently_played_tracks
    SELECT tmp_track.*
    FROM   tmp_track
    LEFT   JOIN spotify_schema.recently_played_tracks USING (timestamp_id)
    WHERE  spotify_schema.recently_played_tracks.timestamp_id IS NULL;
    
    DROP TABLE tmp_track;""")
    conn.commit()
    # Load album data to a temp table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_album AS SELECT * FROM spotify_schema.spotify_albums LIMIT 0
    """)
    album_df.to_sql("tmp_album", con = engine, if_exists='append', index = False)
    conn_eng.commit()
    # Move album data from temp table to production table
    cur.execute(
    """
    INSERT INTO spotify_schema.spotify_albums
    SELECT tmp_album.*
    FROM   tmp_album
    LEFT   JOIN spotify_schema.spotify_albums USING (album_id)
    WHERE  spotify_schema.spotify_albums.album_id IS NULL;
    
    DROP TABLE tmp_album;""")
    conn.commit()

    # Load artist data to a temp table
    cur_eng.execute(
    """
    CREATE TEMP TABLE IF NOT EXISTS tmp_artist AS SELECT * FROM spotify_schema.spotify_artists LIMIT 0
    """)
    artist_df.to_sql("tmp_artist", con = engine, if_exists='append', index = False)
    conn_eng.commit()
    # Move artist data from temp table to production table
    cur.execute(
    """
    INSERT INTO spotify_schema.spotify_artists
    SELECT tmp_artist.*
    FROM   tmp_artist
    LEFT   JOIN spotify_schema.spotify_artists USING (artist_id)
    WHERE  spotify_schema.spotify_artists.artist_id IS NULL;
    
    DROP TABLE tmp_artist;""")
    conn.commit()