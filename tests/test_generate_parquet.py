from unittest.mock import patch, MagicMock
from pipe.generate_parquet import generate_parquet

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

@patch('pyspark.sql.SparkSession.createDataFrame')
def test_generate_parquet(mock_generate_df):

    mock_generate_df.return_value = MagicMock()  # Simula o Spark DataFrame

    # Configurar mock para o write
    mock_write = MagicMock()
    mock_generate_df.return_value.write = mock_write

    # Configurar comportamento da cadeia
    mock_write.mode.return_value = mock_write
    mock_write.partitionBy.return_value = mock_write

    # Chamar a função para teste
    generate_parquet()

    # Verificar chamadas na cadeia
    mock_write.mode.assert_called_once_with("append")
    mock_write.partitionBy.assert_called_once_with("year", "month", "day")
    mock_write.parquet.assert_called_once_with("../datalake/raw")

    




