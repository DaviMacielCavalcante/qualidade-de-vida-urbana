CREATE TABLE IF NOT EXISTS bronze.google_api_data(
	id SERIAL PRIMARY KEY,
	data JSONB
);

CREATE TABLE IF NOT EXISTS silver.google_api_data (
    id SERIAL PRIMARY KEY,
    data_medicao TIMESTAMP,
    regiao VARCHAR(2),
    
    aqi_universal INTEGER,
    codigo_universal VARCHAR(4),
    cor_vermelho_universal DECIMAL(9,8),
    cor_azul_universal DECIMAL(9,8),
    cor_verde_universal DECIMAL(9,8),
    categoria_universal VARCHAR(50),
    valor_display_universal INTEGER,
    nome_display_universal VARCHAR(20),
    poluente_dominante_universal VARCHAR(10),
    
    aqi_br INTEGER,
    codigo_br VARCHAR(20),
    cor_br DECIMAL(2,1),
    categoria_br VARCHAR(20),
    valor_display_br INTEGER,
    nome_display_br VARCHAR(20),
    poluente_dominante_br VARCHAR(10),
    
    co_code VARCHAR(10),
    co_nome VARCHAR(20),
    co_display_nome VARCHAR(10),
    co_concentracao_unidade VARCHAR(30),
    co_valor NUMERIC(10,6),
    
    no2_code VARCHAR(10),
    no2_nome VARCHAR(50),
    no2_display_nome VARCHAR(10),
    no2_concentracao_unidade VARCHAR(30),
    no2_valor NUMERIC(10,6),
    
    o3_code VARCHAR(10),
    o3_nome VARCHAR(50),
    o3_display_nome VARCHAR(10),
    o3_concentracao_unidade VARCHAR(30),
    o3_valor NUMERIC(10,6),
    
    pm10_code VARCHAR(10),
    pm10_nome VARCHAR(50),
    pm10_display_nome VARCHAR(10),
    pm10_concentracao_unidade VARCHAR(30),
    pm10_valor NUMERIC(10,6),
    
    pm25_code VARCHAR(10),
    pm25_nome VARCHAR(50),
    pm25_display_nome VARCHAR(10),
    pm25_concentracao_unidade VARCHAR(30),
    pm25_valor NUMERIC(10,6),
    
    so2_code VARCHAR(10),
    so2_nome VARCHAR(50),
    so2_display_nome VARCHAR(10),
    so2_concentracao_unidade VARCHAR(30),
    so2_valor NUMERIC(10,6),
	created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);