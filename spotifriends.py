import os
import requests
import modal
import json
import pandas as pd
import mysql.connector
import datetime


vol = modal.SharedVolume().persist("spotifriends-vol")
image = modal.Image.debian_slim().pip_install(
    ["requests", "pandas", " mysql-connector-python"]
)
stub = modal.Stub("spotifriends")


def getWebAccessToken(spdc: str) -> str:
    req = requests.get(
        "https://open.spotify.com/get_access_token?reason=transport&productType=web_player",
        headers={"Cookie": "sp_dc=" + spdc},
    )
    resp = req.json()
    return resp["accessToken"]


def getBuddyList(accessToken: str, spclient: str) -> dict:
    req = requests.get(
        spclient + "/presence-view/v1/buddylist",
        headers={"Authorization": "Bearer " + accessToken},
    )

    return req.json()


def loadCachedVersion(filename: str) -> dict:
    cached = {}
    with open(filename) as f:
        cached = json.load(f)
    return cached


def saveCachedVersion(filename: str, data: dict):
    serializedJson = json.dumps(data, indent=4)

    with open(filename, "w") as outfile:
        outfile.write(serializedJson)


def determineNewChanges(cachedVersion, newVersion):
    newChangesResult = {"friends": []}

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


def saveToDb(data, dbconnection):
    cursor = dbconnection.cursor()

    add_activity = (
        "INSERT INTO Activity "
        "(timestamp, user_uri, user_name, track_uri, track_name, track_imageUrl, track_album_uri, track_album_name, track_artist_uri, track_artist_name, track_context_name, track_context_index) "
        "VALUES (%(timestamp)s, %(user_uri)s, %(user_name)s, %(track_uri)s, %(track_name)s, %(track_imageUrl)s, %(track_album_uri)s, %(track_album_name)s, %(track_artist_uri)s, %(track_artist_name)s, %(track_context_name)s, %(track_context_index)s)"
    )
    cursor.executemany(add_activity, data["friends"])
    dbconnection.commit()
    print(cursor.rowcount, "Record inserted successfully into table")
    cursor.close()


@stub.function(
    schedule=modal.Cron("*/2 * * * *"),
    secret=modal.Secret.from_name("spotifriends-secrets"),
    image=image,
    shared_volumes={"/cache": vol},
)
def main():
    spdc = os.environ["SPDC"]
    spclient = os.environ["SPCLIENT"]

    cachedVersion = loadCachedVersion("/cache/cached.json")

    accessToken = getWebAccessToken(spdc)
    newBuddyList = getBuddyList(accessToken, spclient)

    newChanges = determineNewChanges(newBuddyList, cachedVersion)

    # if there are new changes
    if len(newChanges["friends"]) > 0:

        flattenedData = flattenStructure(newChanges)

        connection = mysql.connector.connect(
            host=os.environ["DB_HOST"],
            user=os.environ["DB_USERNAME"],
            password=os.environ["DB_PASSWORD"],
            database=os.environ["DB_NAME"],
            ssl_ca="/etc/ssl/cert.pem",
        )

        saveToDb(flattenedData, connection)

        connection.close()
        saveCachedVersion("/cache/cached.json", newBuddyList)


if __name__ == "__main__":
    with stub.run():
        # main.call()
        stub.deploy("spotifriends")
