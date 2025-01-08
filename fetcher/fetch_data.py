import requests as r

def fetch_air_data(url, latitude, longitude):
    payload = {
    "universalAqi": "true",
    "location": {
        "latitude":latitude,
        "longitude":longitude
    },
    "extraComputations": [
        "DOMINANT_POLLUTANT_CONCENTRATION",
        "POLLUTANT_CONCENTRATION",
        "LOCAL_AQI"
    ],
    "languageCode": "pt-br"
    }
    headers = {
    "Content-Type": "application/json"
    }
    response = r.post(url, json=payload, headers=headers)

    return response.json()