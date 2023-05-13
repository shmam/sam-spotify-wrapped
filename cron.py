import os
import modal
import json
import pandas as pd
import mysql.connector
from services.spotifyService import *


vol = modal.SharedVolume().persist("spotifriends-vol")
image = modal.Image.debian_slim().pip_install(
    ["requests", "pandas", " mysql-connector-python"]
)
stub = modal.Stub("spotifriends")


def loadCachedObject(filename: str) -> dict:
    cached = {}
    with open(filename) as f:
        try:
            cached = json.load(f)
        except json.JSONDecodeError:
            return None
    return cached


def saveCachedObject(filename: str, data: dict):
    serializedJson = json.dumps(data, indent=4)

    with open(filename, "w") as outfile:
        outfile.write(serializedJson)


def determineNewChanges(newVersion, cachedVersion):
    newChangesResult = {"friends": []}

    if cachedVersion is None:
        return newVersion

    for eachFriend in newVersion["friends"]:
        timestamp = eachFriend["timestamp"]
        userUri = eachFriend["user"]["uri"]
        trackUri = eachFriend["track"]["uri"]

        filteredListFromCache = list(
            filter(
                lambda friend: friend["user"]["uri"] == userUri,
                cachedVersion["friends"],
            )
        )

        if len(filteredListFromCache) > 0:
            friendCachedData = filteredListFromCache[0]

            friendCachedTimestamp = friendCachedData["timestamp"]
            friendCachedTrackUri = friendCachedData["track"]["uri"]
            if timestamp != friendCachedTimestamp and trackUri != friendCachedTrackUri:
                newChangesResult["friends"].append(eachFriend)

    return newChangesResult


def flattenStructure(buddyList: dict) -> dict:
    newChangesResult = {"friends": []}

    for eachFriend in buddyList["friends"]:
        df = pd.json_normalize(eachFriend, sep="_")
        flattened = df.to_dict(orient="records")[0]

        newChangesResult["friends"].append(flattened)

    return newChangesResult


def flattenStructureMe(meData: dict) -> dict:
    meData["item"]["artists"] = meData["item"]["artists"][0]

    df = pd.json_normalize(meData, sep="_")
    flattened = df.to_dict(orient="records")[0]
    return flattened


def saveActivityToDb(data, dbconnection):
    cursor = dbconnection.cursor()

    add_activity = (
        "INSERT INTO Activity "
        "(timestamp, user_uri, user_name, track_uri, track_name, track_imageUrl, track_album_uri, track_album_name, track_artist_uri, track_artist_name, track_context_name, track_context_index) "
        "VALUES (%(timestamp)s, %(user_uri)s, %(user_name)s, %(track_uri)s, %(track_name)s, %(track_imageUrl)s, %(track_album_uri)s, %(track_album_name)s, %(track_artist_uri)s, %(track_artist_name)s, %(track_context_name)s, %(track_context_index)s);"
    )

    count = 0
    failures = 0
    for each in data["friends"]:
        try:
            cursor.execute(add_activity, each)
            dbconnection.commit()
            count += 1
        except mysql.connector.Error as err:
            print("ERROR INSERTING INTO ACTIVITY: ", cursor.statement, err)
            failures += 1

    print(
        count,
        "Record inserted successfully into Activity, ",
        failures,
        "Records failed",
    )
    cursor.close()


def saveMyActivityToDb(data, dbconnection):
    cursor = dbconnection.cursor()

    add_my_activity = (
        "INSERT INTO MyActivity"
        "(shuffle_state, repeat_state, timestamp, progress_ms, currently_playing_type, is_playing, device_id, device_is_active, device_is_private_session, device_is_restricted, device_name, device_type, device_volume_percent, context_type, context_uri, item_album_name, item_album_uri, item_artists_name, item_artists_uri, item_duration_ms, item_explicit, item_is_local, item_name, item_popularity, item_track_number, item_uri)"
        "VALUES (%(shuffle_state)s, %(repeat_state)s, %(timestamp)s, %(progress_ms)s, %(currently_playing_type)s, %(is_playing)s, %(device_id)s, %(device_is_active)s, %(device_is_private_session)s, %(device_is_restricted)s, %(device_name)s, %(device_type)s, %(device_volume_percent)s, %(context_type)s, %(context_uri)s, %(item_album_name)s, %(item_album_uri)s, %(item_artists_name)s, %(item_artists_uri)s, %(item_duration_ms)s, %(item_explicit)s, %(item_is_local)s, %(item_name)s, %(item_popularity)s, %(item_track_number)s, %(item_uri)s)"
    )

    try:
        cursor.execute(add_my_activity, data)
        dbconnection.commit()
        print(cursor.rowcount, "Record inserted successfully into my activity table")

    except mysql.connector.Error as err:
        print("ERROR INSERTING INTO ACTIVITY: ", cursor.statement, err)

    cursor.close()


@stub.function(
    schedule=modal.Cron("*/30 * * * * *"),
    secret=modal.Secret.from_name("spotifriends-secrets"),
    image=image,
    shared_volumes={"/cache": vol},
)
def main():
    spdc = os.environ["SPDC"]
    spclient = os.environ["SPCLIENT"]
    spauthurl = os.environ["SPAUTHURL"]
    spapi = os.environ["SPAPI"]

    cachedVersion = loadCachedObject("/cache/cached.json")

    accessToken = getWebAccessToken(spdc, spauthurl)

    if accessToken is None:
        sys.exit("error with authentication")

    myActivity = getMyCurrentPlayback(accessToken, spapi)
    newBuddyList = getBuddyList(accessToken, spclient)

    newChanges = determineNewChanges(newBuddyList, cachedVersion)

    connection = None

    if myActivity is not None:
        cachedMe = loadCachedObject("/cache/cachedMe.json")
        normalizedMeData = flattenStructureMe(myActivity)

        del normalizedMeData["item_album_artists"]
        del normalizedMeData["item_album_available_markets"]
        del normalizedMeData["item_available_markets"]
        del normalizedMeData["item_album_images"]

        if (
            normalizedMeData["item_uri"] != cachedMe["item_uri"]
            and normalizedMeData["timestamp"] != cachedMe["timestamp"]
        ):

            connection = mysql.connector.connect(
                host=os.environ["DB_HOST"],
                user=os.environ["DB_USERNAME"],
                password=os.environ["DB_PASSWORD"],
                database=os.environ["DB_NAME"],
                ssl_ca="/etc/ssl/cert.pem",
            )

            saveMyActivityToDb(normalizedMeData, connection)
            saveCachedObject("/cache/cachedMe.json", normalizedMeData)

    # if there are new changes
    if len(newChanges["friends"]) > 0:

        flattenedData = flattenStructure(newChanges)

        if connection is None:
            connection = mysql.connector.connect(
                host=os.environ["DB_HOST"],
                user=os.environ["DB_USERNAME"],
                password=os.environ["DB_PASSWORD"],
                database=os.environ["DB_NAME"],
                ssl_ca="/etc/ssl/cert.pem",
            )

        saveActivityToDb(flattenedData, connection)

        saveCachedObject("/cache/cached.json", newBuddyList)

    if connection is not None:
        connection.close()


if __name__ == "__main__":
    with stub.run():
        # main.call()
        stub.deploy("spotifriends")
