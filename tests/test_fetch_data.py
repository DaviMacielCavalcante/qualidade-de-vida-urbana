import pytest
from unittest.mock import patch
from pipe.json_data import return_json_body

mocked_data = {
    "pollution": {
        "main_pollutant": "pm25",
        "concentration": {
        "value": 13.14,
        "units": "MICROGRAMS_PER_CUBIC_METER"
      }
    }
}

@patch("pipe.json_data.fetch_air_data")
def test_return_json_body(mock_fetch_air_data):
    mock_fetch_air_data.return_value = mocked_data

    
    result = return_json_body()

    # Verifica se os campos relevantes estão corretos
    assert "pollution" in result
    assert result["pollution"]["main_pollutant"] == mocked_data["pollution"]["main_pollutant"]
    assert result["pollution"]["concentration"]["value"] == mocked_data["pollution"]["concentration"]["value"]
    assert result["pollution"]["concentration"]["units"] == mocked_data["pollution"]["concentration"]["units"]

    # Verifica se fetch_air_data foi chamada corretamente
    mock_fetch_air_data.assert_called_once()
