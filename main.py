import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import pymysql

USER = "diygabrielaa" 
TOKEN = ""



def check_if_valid_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No songs downloaded.")
        return False 

    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("no existe data de ayer")

    return True


if __name__ == "__main__":
    
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer {token}".format(token=TOKEN)
    }
    
    hoy = datetime.datetime.now()
    ayer = hoy - datetime.timedelta(days=2)
    ayer_unix_timestamp = int(ayer.timestamp()) * 1000
    hoy_unix_timestamp = int(hoy.timestamp()) * 1000
    #print(ayer_unix_timestamp)
    
    # Get data yesterday
    obteniendo = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=ayer_unix_timestamp), headers = headers)
    
    datos = obteniendo.json()
    #print(datos)
    
        #Arrays for info
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Guardando info
    for song in datos["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
    
   
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_dataframe = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
    
    print(song_dataframe)
    
    # Validando
    if check_if_valid_data(song_dataframe):
        print("Data validada, Procediendo")

    
    # Connection MYSQl
    conexion = pymysql.connect(
        host='localhost',
        port=3308,
        user='root',
        password='root',
        database='etl_played_tracks'
    )
    
    cursor = conexion.cursor()
    
        # Creation Table if not exists
    sql_query = """
        CREATE TABLE IF NOT EXISTS etl_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
        """
        # Execute query create database    
    cursor.execute(sql_query)
    print("Opened database satisfactoriamente")
    
    try:
        song_dataframe.to_sql("my_played_tracks", cursor, index=False, if_exists='append')
    except:
        print("la data existe en la DB")

    #conexion.close()
    #print("Close database successfully")
    