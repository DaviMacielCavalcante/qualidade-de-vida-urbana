DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger 
        WHERE tgname = 'inserts_silver_google'
        AND tgrelid = 'bronze.google_api_data'::regclass
    ) THEN
        CREATE OR REPLACE FUNCTION bronze.inserts_silver_google()
        RETURNS TRIGGER AS $tg_func$
        BEGIN
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
                (NEW.data->>'dateTime')::TIMESTAMP,
                NEW.data->>'regionCode',
                (NEW.data#>>'{indexes,0,aqi}')::INTEGER,
                NEW.data#>>'{indexes,0,code}',
                (NEW.data#>>'{indexes,0,color,red}')::DECIMAL(9,8),
                (NEW.data#>>'{indexes,0,color,blue}')::DECIMAL(9,8),
                (NEW.data#>>'{indexes,0,color,green}')::DECIMAL(9,8),
                NEW.data#>>'{indexes,0,category}',
                (NEW.data#>>'{indexes,0,aqiDisplay}')::INTEGER,
                NEW.data#>>'{indexes,0,displayName}',
                NEW.data#>>'{indexes,0,dominantPollutant}',
                (NEW.data#>>'{indexes,1,aqi}')::INTEGER,
                NEW.data#>>'{indexes,1,code}',
                (NEW.data#>>'{indexes,1,color,green}')::DECIMAL(2,1),
                NEW.data#>>'{indexes,1,category}',
                (NEW.data#>>'{indexes,1,aqiDisplay}')::INTEGER,
                NEW.data#>>'{indexes,1,displayName}',
                NEW.data#>>'{indexes,1,dominantPollutant}',
                NEW.data#>>'{pollutants,0,code}',
                NEW.data#>>'{pollutants,0,fullName}',
                NEW.data#>>'{pollutants,0,displayName}',
                NEW.data#>>'{pollutants,0,concentration,units}',
                (NEW.data#>>'{pollutants,0,concentration,value}')::NUMERIC(10,6),
                NEW.data#>>'{pollutants,1,code}',
                NEW.data#>>'{pollutants,1,fullName}',
                NEW.data#>>'{pollutants,1,displayName}',
                NEW.data#>>'{pollutants,1,concentration,units}',
                (NEW.data#>>'{pollutants,1,concentration,value}')::NUMERIC(10,6),
                NEW.data#>>'{pollutants,2,code}',
                NEW.data#>>'{pollutants,2,fullName}',
                NEW.data#>>'{pollutants,2,displayName}',
                NEW.data#>>'{pollutants,2,concentration,units}',
                (NEW.data#>>'{pollutants,2,concentration,value}')::NUMERIC(10,6),
                NEW.data#>>'{pollutants,3,code}',
                NEW.data#>>'{pollutants,3,fullName}',
                NEW.data#>>'{pollutants,3,displayName}',
                NEW.data#>>'{pollutants,3,concentration,units}',
                (NEW.data#>>'{pollutants,3,concentration,value}')::NUMERIC(10,6),
                NEW.data#>>'{pollutants,4,code}',
                NEW.data#>>'{pollutants,4,fullName}',
                NEW.data#>>'{pollutants,4,displayName}',
                NEW.data#>>'{pollutants,4,concentration,units}',
                (NEW.data#>>'{pollutants,4,concentration,value}')::NUMERIC(10,6),
                NEW.data#>>'{pollutants,5,code}',
                NEW.data#>>'{pollutants,5,fullName}',
                NEW.data#>>'{pollutants,5,displayName}',
                NEW.data#>>'{pollutants,5,concentration,units}',
                (NEW.data#>>'{pollutants,5,concentration,value}')::NUMERIC(10,6)
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