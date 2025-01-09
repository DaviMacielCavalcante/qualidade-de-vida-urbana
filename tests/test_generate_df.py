from unittest.mock import patch
import pandas as pd
from pipe.generate_df import generate_df

# Mock dos dados retornados por return_json_body
mocked_data = {
    "dateTime": "2025-01-09T01:00:00Z",

    "pollutants": [
        {
            "displayName": "PM2.5",
            "concentration": {
                "value": 42.5,
                "units": "µg/m³"
            }
        },
        {
            "displayName": "PM10",
            "concentration": {
                "value": 75.0,
                "units": "µg/m³"
            }
        }
    ]
}

# Mock dos valores no dotenv
mocked_dotenv = {
    "LATITUDE": 37.7749,
    "LONGITUDE": -122.4194
}

@patch("pipe.json_data.return_json_body")
@patch("dotenv.dotenv_values")
def test_generate_df(mock_dotenv, mock_return_json_body):
    # Configura o mock para dotenv_values
    
    mock_dotenv.return_value = mocked_dotenv

    # Configura o mock para return_json_body
    mock_return_json_body.return_value = mocked_data

    # Chama a função
    df = generate_df()

    # Dados esperados no DataFrame
    expected_data = [
        {
            "latitude": float(mocked_dotenv["LATITUDE"]),
            "longitude": float(mocked_dotenv["LONGITUDE"]),
            "pollutant": "pm2.5",
            "value": 42.5,
            "unit": "µg/m³",
            "year": 2025,
            "month": 1,
            "day": 9,
            "hour": 1
        },
        {
            "latitude": float(mocked_dotenv["LATITUDE"]),
            "longitude": float(mocked_dotenv["LONGITUDE"]),
            "pollutant": "pm10",
            "value": 75.0,
            "unit": "µg/m³",
            "year": 2025,
            "month": 1,
            "day": 9,
            "hour": 1
        }
    ]

    # Converte os dados esperados para um DataFrame
    expected_df = pd.DataFrame(expected_data)

    # Normaliza os tipos de dados
    df = df.astype(expected_df.dtypes.to_dict())
