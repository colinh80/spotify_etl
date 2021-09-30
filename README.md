# spotify_etl

This is a Python project where I build an ETL pipeline for extracting data consisting of my recently played tracks on Spotify. The pipeline begins by extracting my account data from Spotify API using the Python library [Spotipy](https://spotipy.readthedocs.io/en/2.19.0/). I'm interested in compiling the data over the course of a year to then query against the database for further insights on my listening habits. I transform the data using Pandas to clean, filter and validate the data before loading it into a PostgreSQL database on my local machine.

# Extraction: Spotify API

I extracted data out of the Spotify API using this [endpoint](https://developer.spotify.com/documentation/web-api/reference/#endpoint-get-recently-played) under the scope 'user-read-recently-played'. The API has a limit of 50 tracks, which was acceptable for my own listening habits. Although I include a warning in the validation stage if the limit was reached to indicate some played tracks have been lost. In that case, one would need schedule the ETL at a higher frequency than once a day. The data extracted is parsed through and appended to lists, which are then collected into dictionaries and finally Pandas dataframes. The result is 3 dataframes, which inturn will become a fact table (recently_played_tracks) and two dimension tables (spotify_albums/spotify_artists).

# Transform: Python and the Pandas Library

After extraction with Python, it made sense to fit the data into Pandas dataframes for cleaning. I want to keep a record of every track I listen to. Considering I can only listen to one track at a time, I used the timestamp column as my primary key for the fact table (recently_played_tracks). I converted timestamp data to my local timezone, then formatted the column for filtering. This was a crucial step, because the pipline will always extract the 50 most recently played tracks. However, I want to filter out timestamps beyond the last 24 hours. I performed the filter on my track dataframe which will ultimatly become the fact table. I then removed the corrosponding rows from both the albums and artists dataframes. Lastly, I removed duplicate values from my dimension tables, spotify_artists and spotify_albums. I used the artist_id and album_id respectivly for the primary keys. 

The final step here was running my [validation functions](https://github.com/colinjhicks/spotify_etl/blob/main/python/validation.py) for each dataframe. I wanted to check that the primary keys were unique and there are no null values in any dataframe. For my track dataframe, I also validated whether or not the timestamps are from within the last 24 hours. I also include a warning for lost tracks if the dataframe after filtering is still 50 rows in length. Again, just due to the limits of this API endpoint.

The code for the Extraction and Transform stages can be seen in the [spotify_etl](https://github.com/colinjhicks/spotify_etl/blob/main/python/spotify_etl.py) file.

# Load: Python and PostgreSQL

I created a database on my local machine using PostgreSQL to store the three dataframes in a schema titled 'spotify_schema'. The SQL code for creating these tables can be seen [here](https://github.com/colinjhicks/spotify_etl/blob/main/sql/create_tables.sql). I used Psycopg2 for my engine and the pandas method to_sql() to load the data. Each dataframe is loaded into a temporary table, then joined with the main database under the condition that there are no matching primary keys. The code for this stage is also found in the [spotify_etl](https://github.com/colinjhicks/spotify_etl/blob/main/python/spotify_etl.py) file. Below I have a visual of my database schema.

# Schedule: Apache Airflow

I utlized Apache Airflow to create a DAG to schedule the data pipeline for a 24 hour cycle. The corrosponding code can be seen [here](https://github.com/colinjhicks/spotify_etl/blob/main/dags/spotify_dag.py).
