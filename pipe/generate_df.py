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
    return df

