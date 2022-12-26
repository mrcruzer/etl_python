import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd 
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import pymysql

USER = "31jclj74jujykp63nynogjsfvh4i" 
TOKEN = "BQDAQxu9Qpvv6_ZeoymlEubkidYpjR7PcLAY3vJEe00QVf3_ceXi-1SOAakW7PqYemUgy1WNJI655tcNHQaVtZk7ltzLF1YBjgWtmbmHV-Zkp5xeXoD0koEKOP9E7Qz6Sa7YcI3v_7b0KnI-RTJuK2DBaj79I75RXDGdV1AoIQC_utElmX5mhELF44HQjKx-s2N0h4LT"



def checking_valid_data(df: pd.DataFrame) -> bool:
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

    '''yesterday = datetime.datetime.now() - datetime.timedelta(days=2)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    #timestamps = df["timestamp"].tolist()
    #for timestamp in timestamps:
     #   if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
      #      raise Exception("no existe data de ayer")'''

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
    
    # Get data
    obteniendo = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=ayer_unix_timestamp), headers = headers)
    
    # transform data to json
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
    if checking_valid_data(song_dataframe):
        print("Data validada, Procediendo")

    
    # Connection MYSQl
    conexion = pymysql.connect(
        host='localhost',
        port=3308,
        user='root',
        password='root',
        db='etl_played_tracks'
    )
    
    cursor = conexion.cursor()
    
    # Create engine, SQLALchemy
    engine = create_engine("mysql+pymysql://{user}:{pw}@localhost:{port}/{db}"
                       .format(user="root",
                               pw="root",
                               port = 3308,
                               db="etl_played_tracks"))
    
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
    
    # passing dataframe to SQL Database table, etl_played_tracks
    try:
        song_dataframe.to_sql("etl_played_tracks", engine, index=False, if_exists='append')
    except:
        print("la data existe en la DB")

    #conexion.close()
    #print("Close database successfully")
    