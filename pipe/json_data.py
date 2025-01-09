from fetcher.fetch_data import fetch_air_data
from dotenv import dotenv_values

API_KEY = dotenv_values()['API_KEY']
url = f'https://airquality.googleapis.com/v1/currentConditions:lookup?key={API_KEY}'
latitude = dotenv_values()['LATITUDE']
longitude = dotenv_values()['LONGITUDE']

def return_json_body():
    data = fetch_air_data(url, latitude, longitude)
    return data