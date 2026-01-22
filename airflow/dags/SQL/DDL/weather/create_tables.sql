CREATE TABLE IF NOT EXISTS bronze.weather_api_data(
	id SERIAL PRIMARY KEY,
	data JSONB
);

CREATE TABLE IF NOT EXISTS silver.weather_api_data(
    id SERIAL PRIMARY KEY,
    bairro VARCHAR(100),
    latitude NUMERIC(10,7),
    longitude NUMERIC(10,7),
    temperatura NUMERIC(5,2),
    sensacao_termica NUMERIC(5,2),
    umidade INTEGER,
    pressao INTEGER,
    velocidade_vento NUMERIC(4,2),
    direcao_vento INTEGER,
    descricao VARCHAR(100),
    nascer_sol BIGINT,
    por_sol BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
