DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger 
        WHERE tgname = 'inserts_silver_google'
        AND tgrelid = 'bronze.google_api_data'::regclass
    ) THEN
        CREATE OR REPLACE FUNCTION bronze.inserts_silver_google()
        RETURNS TRIGGER AS $tg_func$
        DECLARE
            region_key TEXT;
            json_path TEXT[];
            
            -- Constantes para os caminhos base
            CONST_INDEXES TEXT := 'indexes';
            CONST_POLLUTANTS TEXT := 'pollutants';
            
            -- Constantes para índices
            CONST_UNIVERSAL_IDX TEXT := '0';
            CONST_BR_IDX TEXT := '1';
            
            -- Constantes para os poluentes
            CONST_CO_IDX TEXT := '0';
            CONST_NO2_IDX TEXT := '1';
            CONST_O3_IDX TEXT := '2';
            CONST_PM10_IDX TEXT := '3';
            CONST_PM25_IDX TEXT := '4';
            CONST_SO2_IDX TEXT := '5';
            
            -- Constantes para os campos comuns
            CONST_DATETIME TEXT := 'datetime';
            CONST_AQI TEXT := 'aqi';
            CONST_CODE TEXT := 'code';
            CONST_CATEGORY TEXT := 'category';
            CONST_AQI_DISPLAY TEXT := 'aqiDisplay';
            CONST_DISPLAY_NAME TEXT := 'displayName';
            CONST_DOMINANT_POLLUTANT TEXT := 'dominantPollutant';
            
            -- Constantes para cores
            CONST_COLOR TEXT := 'color';
            CONST_RED TEXT := 'red';
            CONST_BLUE TEXT := 'blue';
            CONST_GREEN TEXT := 'green';
            
            -- Constantes para poluentes
            CONST_FULL_NAME TEXT := 'fullName';
            CONST_CONCENTRATION TEXT := 'concentration';
            CONST_UNITS TEXT := 'units';
            CONST_VALUE TEXT := 'value';
        BEGIN
            -- Obtém a primeira chave (nome da região) do JSON
            SELECT key INTO region_key
            FROM jsonb_object_keys(NEW.data) AS key
            LIMIT 1;
            
            -- Insere os dados na tabela silver
            INSERT INTO silver.google_api_data(
                data_medicao,
                regiao,
                aqi_universal,
                codigo_universal,
                cor_vermelho_universal,
                cor_azul_universal,
                cor_verde_universal,
                categoria_universal,
                valor_display_universal,
                nome_display_universal,
                poluente_dominante_universal,
                aqi_br,
                codigo_br,
                cor_br,
                categoria_br,
                valor_display_br,
                nome_display_br,
                poluente_dominante_br,
                co_code,
                co_nome,
                co_display_nome,
                co_concentracao_unidade,
                co_valor,
                no2_code,
                no2_nome,
                no2_display_nome,
                no2_concentracao_unidade,
                no2_valor,
                o3_code,
                o3_nome,
                o3_display_nome,
                o3_concentracao_unidade,
                o3_valor,
                pm10_code,
                pm10_nome,
                pm10_display_nome,
                pm10_concentracao_unidade,
                pm10_valor,
                pm25_code,
                pm25_nome,
                pm25_display_nome,
                pm25_concentracao_unidade,
                pm25_valor,
                so2_code,
                so2_nome,
                so2_display_nome,
                so2_concentracao_unidade,
                so2_valor) 
            VALUES (
                -- Data e Região
                (NEW.data #>> ARRAY[region_key, CONST_DATETIME])::TIMESTAMP,
                region_key,
                
                -- Universal AQI (Índice 0)
                (NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_UNIVERSAL_IDX, CONST_AQI])::INTEGER,
                NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_UNIVERSAL_IDX, CONST_CODE],
                (NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_UNIVERSAL_IDX, CONST_COLOR, CONST_RED])::DECIMAL(9,8),
                (NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_UNIVERSAL_IDX, CONST_COLOR, CONST_BLUE])::DECIMAL(9,8),
                (NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_UNIVERSAL_IDX, CONST_COLOR, CONST_GREEN])::DECIMAL(9,8),
                NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_UNIVERSAL_IDX, CONST_CATEGORY],
                (NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_UNIVERSAL_IDX, CONST_AQI_DISPLAY])::INTEGER,
                NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_UNIVERSAL_IDX, CONST_DISPLAY_NAME],
                NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_UNIVERSAL_IDX, CONST_DOMINANT_POLLUTANT],
                
                -- BR AQI (Índice 1)
                (NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_BR_IDX, CONST_AQI])::INTEGER,
                NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_BR_IDX, CONST_CODE],
                (NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_BR_IDX, CONST_COLOR, CONST_GREEN])::DECIMAL(2,1),
                NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_BR_IDX, CONST_CATEGORY],
                (NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_BR_IDX, CONST_AQI_DISPLAY])::INTEGER,
                NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_BR_IDX, CONST_DISPLAY_NAME],
                NEW.data #>> ARRAY[region_key, CONST_INDEXES, CONST_BR_IDX, CONST_DOMINANT_POLLUTANT],
                
                -- CO (Poluente 0)
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_CO_IDX, CONST_CODE],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_CO_IDX, CONST_FULL_NAME],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_CO_IDX, CONST_DISPLAY_NAME],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_CO_IDX, CONST_CONCENTRATION, CONST_UNITS],
                (NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_CO_IDX, CONST_CONCENTRATION, CONST_VALUE])::NUMERIC(10,6),
                
                -- NO2 (Poluente 1)
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_NO2_IDX, CONST_CODE],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_NO2_IDX, CONST_FULL_NAME],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_NO2_IDX, CONST_DISPLAY_NAME],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_NO2_IDX, CONST_CONCENTRATION, CONST_UNITS],
                (NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_NO2_IDX, CONST_CONCENTRATION, CONST_VALUE])::NUMERIC(10,6),
                
                -- O3 (Poluente 2)
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_O3_IDX, CONST_CODE],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_O3_IDX, CONST_FULL_NAME],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_O3_IDX, CONST_DISPLAY_NAME],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_O3_IDX, CONST_CONCENTRATION, CONST_UNITS],
                (NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_O3_IDX, CONST_CONCENTRATION, CONST_VALUE])::NUMERIC(10,6),
                
                -- PM10 (Poluente 3)
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_PM10_IDX, CONST_CODE],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_PM10_IDX, CONST_FULL_NAME],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_PM10_IDX, CONST_DISPLAY_NAME],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_PM10_IDX, CONST_CONCENTRATION, CONST_UNITS],
                (NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_PM10_IDX, CONST_CONCENTRATION, CONST_VALUE])::NUMERIC(10,6),
                
                -- PM2.5 (Poluente 4)
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_PM25_IDX, CONST_CODE],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_PM25_IDX, CONST_FULL_NAME],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_PM25_IDX, CONST_DISPLAY_NAME],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_PM25_IDX, CONST_CONCENTRATION, CONST_UNITS],
                (NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_PM25_IDX, CONST_CONCENTRATION, CONST_VALUE])::NUMERIC(10,6),
                
                -- SO2 (Poluente 5)
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_SO2_IDX, CONST_CODE],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_SO2_IDX, CONST_FULL_NAME],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_SO2_IDX, CONST_DISPLAY_NAME],
                NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_SO2_IDX, CONST_CONCENTRATION, CONST_UNITS],
                (NEW.data #>> ARRAY[region_key, CONST_POLLUTANTS, CONST_SO2_IDX, CONST_CONCENTRATION, CONST_VALUE])::NUMERIC(10,6)
            );

            RETURN NEW;
        END;
        $tg_func$ LANGUAGE plpgsql;

        
        CREATE TRIGGER inserts_silver_google
        AFTER INSERT ON bronze.google_api_data
        FOR EACH ROW 
        EXECUTE FUNCTION bronze.inserts_silver_google();
        
    ELSE
        RAISE NOTICE 'Trigger inserts_silver_google já existe!';
    END IF;
END
$$;