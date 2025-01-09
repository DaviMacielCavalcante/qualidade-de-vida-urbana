from pipe import json_data as jd
import pandas as pd
from dotenv import dotenv_values

def generate_df():
    data = jd.return_json_body()
    data_list = []

    for p in data['pollutants']:
        data_list.append({
            "datetime": data['dateTime'],
            "latitude": dotenv_values()['LATITUDE'],
            "longitude": dotenv_values()['LONGITUDE'],
            "pollutant": p["displayName"].lower(),
            "value": p["concentration"]["value"],
            "unit": p["concentration"]["units"].lower()
        })

    df = pd.DataFrame(data_list)

    df["datetime"] = pd.to_datetime(df["datetime"])
    df["year"] = df["datetime"].dt.year
    df["month"] = df["datetime"].dt.month
    df["day"] = df["datetime"].dt.day
    df["hour"] = df["datetime"].dt.hour
    df.drop("datetime", axis=1, inplace=True)
    df["latitude"] = df["latitude"].astype(float)
    df["longitude"] = df["longitude"].astype(float)
    df["value"] = df["value"].astype(float)
    return df

