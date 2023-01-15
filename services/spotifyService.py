import requests


def getWebAccessToken(spdc: str, spauthurl: str) -> str:
    req = requests.get(
        spauthurl,
        headers={"Cookie": "sp_dc=" + spdc},
        timeout=2
    )
    resp = req.json()

    if req.status_code != 200 and resp.get("accessToken") is None:
        print(resp)
        return None

    return resp["accessToken"]

def getBuddyList(accessToken: str, spclient: str) -> dict:
    req = requests.get(
        spclient + "/presence-view/v1/buddylist",
        headers={"Authorization": "Bearer " + accessToken},
        timeout=2

    )

    return req.json()

def getMyCurrentPlayback(accessToken: str, spapi: str) -> dict:
    req = requests.get(
        spapi + "/v1/me/player",
        headers={"Authorization": "Bearer " + accessToken},
        timeout=2
    )

    if req.status_code == 200:
        return req.json()
    else:
        return None

def getAudioFeatures(accessToken: str, spapi: str, songIds: list) -> dict:
    req = requests.get(
        spapi + "/v1/audio-features",
        headers={"Authorization": "Bearer " + accessToken},
        params={
            'ids': ','.join(songIds)
        },
        timeout=2
    )

    if req.status_code == 200:
        return req.json()
    else:
        return None