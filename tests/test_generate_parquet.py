from unittest.mock import patch, MagicMock
from pipe.generate_parquet import generate_parquet

# Mock dos dados retornados por generate_df
mocked_generate_df = [{
    "latitude": 37.7749,
    "longitude": -122.4194,
    "pollutant": "pm2.5",
    "value": 42.5,
    "unit": "µg/m³",
    "year": 2025,
    "month": 1,
    "day": 9,
    "hour": 1
}]

# Mock dos dados retornados por jd.return_json_body
mocked_json_body = {
    "pollutants": [
        {"displayName": "PM2.5", "concentration": {"value": 42.5, "units": "µg/m³"}}
    ],
    "dateTime": "2025-01-01T00:00:00Z"
}

@patch('pipe.generate_df.jd.return_json_body')  # Mocka a função jd.return_json_body
@patch('pyspark.sql.SparkSession.createDataFrame')  # Mocka o método createDataFrame
def test_generate_parquet(mock_create_df, mock_return_json_body):
    # Configurar o mock de return_json_body
    mock_return_json_body.return_value = mocked_json_body

    # Configurar o mock de generate_df
    mock_create_df.return_value = MagicMock()  # Simula o Spark DataFrame

    # Configurar mock para o write
    mock_write = MagicMock()
    mock_create_df.return_value.write = mock_write

    # Configurar comportamento da cadeia
    mock_write.mode.return_value = mock_write
    mock_write.partitionBy.return_value = mock_write

    # Chamar a função para teste
    generate_parquet()

    # Verificar chamadas na cadeia
    mock_write.mode.assert_called_once_with("append")
    mock_write.partitionBy.assert_called_once_with("year", "month", "day")
    mock_write.parquet.assert_called_once_with("../datalake/raw")
