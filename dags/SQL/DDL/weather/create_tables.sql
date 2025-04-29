CREATE TABLE IF NOT EXISTS bronze.weather_api_data(
	id SERIAL PRIMARY KEY,
	data JSONB
);

CREATE TABLE IF NOT EXISTS silver.weather_api_data (
    id SERIAL PRIMARY KEY,
    regiao VARCHAR(20),
    uv NUMERIC(5,2),
    cloud INTEGER,
    is_day INTEGER,
    temp_c NUMERIC(4,2),
    temp_f NUMERIC(5,2),
    vis_km NUMERIC(4,2),
    gust_kph NUMERIC(4,2),
    gust_mph NUMERIC(4,2),
    humidity INTEGER,
    wind_dir VARCHAR(10),
    wind_kph NUMERIC(4,2),
    wind_mph NUMERIC(4,2),
    condition_code INTEGER,
    condition_icon VARCHAR(250),
    condition_text VARCHAR(100),
    precip_in NUMERIC(6,4),
    precip_mm NUMERIC(6,4),
    vis_miles NUMERIC(4,2),
    dewpoint_c NUMERIC(4,2),
    dewpoint_f NUMERIC(5,2),
    feelslike_c NUMERIC(4,2),
    feelslike_f NUMERIC(5,2),
    heatindex_c NUMERIC(4,2),
    heatindex_f NUMERIC(5,2),
    pressure_in NUMERIC(4,2),
    pressure_mb NUMERIC(6,2),
    wind_degree INTEGER,
    windchill_c NUMERIC(4,2),
    windchill_f NUMERIC(5,2),
    last_updated TIMESTAMP,
    last_updated_epoch BIGINT,
    location_lat NUMERIC(6,4),
    location_lon NUMERIC(6,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

