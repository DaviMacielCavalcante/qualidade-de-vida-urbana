from unittest.mock import patch
from fetcher.fetch_data import fetch_air_data

mocked_data = {
    "url" : "fake",
    "latitude": 12,
    "longitude" : 12
}

@patch('fetcher.fetch_data.requests.post')
def test_fetch_data(mocked_post):
    expect_response = {
        "dateTime": "2022-12-12T12:12:12Z",
        "pollution": {
            "main_pollutant": "pm25",
            "concentration": {
            "value": 13.14,
            "units": "MICROGRAMS_PER_CUBIC_METER"
          }
        }
    }

    mocked_post.return_value.json.return_value = expect_response
    response = fetch_air_data(mocked_data["url"], mocked_data["latitude"], mocked_data["longitude"])

    assert expect_response == response

