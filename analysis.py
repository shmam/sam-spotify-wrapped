import os
import mysql.connector
from services.spotifyService import getWebAccessToken, getAudioFeatures
from dotenv import load_dotenv

load_dotenv()

def establishConnection() -> any: 
    connection = mysql.connector.connect(
                host=os.environ["DB_HOST"],
                user=os.environ["DB_USERNAME"],
                password=os.environ["DB_PASSWORD"],
                database=os.environ["DB_NAME"],
                ssl_ca="/etc/ssl/cert.pem",
        )
    return connection

def getTracks(connection) -> list: 
    cursor = connection.cursor()

    query = """
        SELECT DISTINCT track_uri as all_distinct_trackuri from
        (
            SELECT distinct track_uri from Activity
            union
            SELECT distinct item_uri from MyActivity
        ) t
        WHERE track_uri NOT IN (SELECT uri FROM AudioAnalysis)
    """

    cursor.execute(query)
    queryResult = cursor.fetchall()

    tracks = []
    for eachTrack in queryResult:
        tracks.append(eachTrack[0])
    return tracks

def cleanTracks(uncleanTracks: list) -> list:
    cleaned = []
    for eachTrack in uncleanTracks:
        components = eachTrack.split(":")
        cleaned.append(components[2])

    return cleaned

def uploadAnalysis(data, connnection): 
    cursor = connnection.cursor()

    insert_query = (
        "INSERT INTO AudioAnalysis "
        "(danceability, energy, track_key, loudness, mode, speechiness, acousticness, instrumentalness, liveness, valence, tempo, type, uri, duration_ms, time_signature)"
        "VALUES (%(danceability)s, %(energy)s, %(key)s, %(loudness)s, %(mode)s, %(speechiness)s, %(acousticness)s, %(instrumentalness)s, %(liveness)s, %(valence)s, %(tempo)s, %(type)s, %(uri)s, %(duration_ms)s, %(time_signature)s);"
    )

    count = 0
    failures = 0
    for each in data["audio_features"]:

        try:
            cursor.execute(insert_query, each)
            connnection.commit()
            count+=1
        except mysql.connector.Error as err:
            print("ERROR INSERTING INTO AudioAnalysis: ", cursor.statement, err)
            failures += 1

    print(
        count,
        "Record inserted successfully into AudioAnalysis, ",
        failures,
        "Records failed",
    )
    cursor.close()

def main():
    spdc = os.environ["SPDC"]
    spauthurl = os.environ["SPAUTHURL"]
    spapi = os.environ["SPAPI"]

    token = getWebAccessToken(spdc, spauthurl)

    conn = establishConnection()

    uncleanTracks = getTracks(conn)
    cleanedTracks = cleanTracks(uncleanTracks)

    batches = []
    if len(cleanedTracks) > 100:
        batches = [cleanedTracks[i:i+100] for i in range(0, len(cleanedTracks), 100)]
    else:
        batches = [cleanedTracks]

    for eachBatch in batches: 
        audioFeatures = getAudioFeatures(token, spapi, eachBatch)
        uploadAnalysis(audioFeatures, conn)

    


if __name__ == "__main__":
    main()