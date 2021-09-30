-- Create track table (Fact Table)
CREATE TABLE IF NOT EXISTS spotify_schema.recently_played_tracks(
    timestamp_id TIMESTAMP PRIMARY KEY NOT NULL,
    track_name TEXT,
    popularity SMALLINT,
    track_id CHAR(22),
    album_id CHAR(22),
    artist_id CHAR(22),
    date_time_inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Create Album Table (Dimension Table)
CREATE TABLE IF NOT EXISTS spotify_schema.spotify_albums(
    album_id CHAR(22) PRIMARY KEY NOT NULL,
    title TEXT,
    release_date DATE,
    track_count SMALLINT
    );

-- Create Artist Table (Dimension Table)
CREATE TABLE IF NOT EXISTS spotify_schema.spotify_artists(
    artist_id CHAR(22) PRIMARY KEY NOT NULL,
    name TEXT,
    url TEXT);