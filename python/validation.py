# This file includes three functions used to validate dataframes created in the spotify_etl script.
import pandas as pd
import datetime

# This is a validation function used to ensure the data retrieved is as expected.
def check_if_track_data_is_valid(df: pd.DataFrame) -> bool:
    # Check if dataframe is empty
    if df.empty:
        print("No tracks downloaded. Finishing execution.")
        return False
    # Primary key check
    if df["timestamp_id"].is_unique:
        pass
    else:
        raise Exception("Primary Key is violated, not unique.")
    # Check for nulls
    if df.isnull().values.any():
        raise Exception("There are null values in the data.")
    # Check that all timestamps are from the last 24 hours
    yesterday_ver = datetime.datetime.now() - datetime.timedelta(days=1)
    timestamp_list = df["timestamp_id"].to_list()
    for i in timestamp_list:
        timestamp = pd.to_datetime(i)
        if timestamp < yesterday_ver:
            raise Exception("There is at least one track returned that does not come from within the last 24 hours.")
    # Check if data returned is 50 rows, indicating the max limit may have been reached.
    # The data will still be passed but with a warning that the data may be incomplete due to the limits of the api.
    if len(df) == 50:
        print("The list of tracks returned is equal to 50, which is the limit for the Spotify api. Some tracks may have been lost.")
    return True

def check_if_album_data_is_valid(df: pd.DataFrame) -> bool:
    # Primary key check
    if df["album_id"].is_unique:
        pass
    else:
        raise Exception("Primary Key is violated, not unique.")
    # Check for nulls
    if df.isnull().values.any():
        raise Exception("There are null values in the data.")
    return True

def check_if_artist_data_is_valid(df: pd.DataFrame) -> bool:
    # Primary key check
    if df["artist_id"].is_unique:
        pass
    else:
        raise Exception("Primary Key is violated, not unique.")
    # Check for nulls
    if df.isnull().values.any():
        raise Exception("There are null values in the data.")
    return True