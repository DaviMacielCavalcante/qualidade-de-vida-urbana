DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger 
        WHERE tgname = 'inserts_silver_weather'
        AND tgrelid = 'bronze.weather_api_data'::regclass
    ) THEN
        CREATE OR REPLACE FUNCTION bronze.inserts_silver_weather()
        RETURNS TRIGGER AS $tg_func$
        DECLARE
            region_key TEXT;
            
            -- Caminhos base
            CONST_DATA TEXT := 'data';
            CONST_LATITUDE TEXT := 'latitude';
            CONST_LONGITUDE TEXT := 'longitude';
            
            -- Constantes para campos de dados meteorológicos
            CONST_TEMPERATURA TEXT := 'temperatura';
            CONST_SENSACAO_TERMICA TEXT := 'sensacao_termica';
            CONST_UMIDADE TEXT := 'umidade';
            CONST_PRESSAO TEXT := 'pressao';
            CONST_VELOCIDADE_VENTO TEXT := 'velocidade_vento';
            CONST_DIRECAO_VENTO TEXT := 'direcao_vento';
            CONST_DESCRICAO TEXT := 'descricao';
            CONST_NASCER_SOL TEXT := 'nascer_sol';
            CONST_POR_SOL TEXT := 'por_sol';
        BEGIN
            -- Obtém a primeira chave (nome da região) do JSON
            SELECT key INTO region_key
            FROM jsonb_object_keys(NEW.data) AS key
            LIMIT 1;
            
            INSERT INTO silver.weather_api_data(
                bairro,
                latitude,
                longitude,
                temperatura,
                sensacao_termica,
                umidade,
                pressao,
                velocidade_vento,
                direcao_vento,
                descricao,
                nascer_sol,
                por_sol
            ) 
            VALUES (
                region_key,
                (NEW.data #>> ARRAY[region_key, CONST_LATITUDE])::NUMERIC(10,7),
                (NEW.data #>> ARRAY[region_key, CONST_LONGITUDE])::NUMERIC(10,7),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_TEMPERATURA])::NUMERIC(5,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_SENSACAO_TERMICA])::NUMERIC(5,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_UMIDADE])::NUMERIC::INTEGER,
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_PRESSAO])::NUMERIC::INTEGER,
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_VELOCIDADE_VENTO])::NUMERIC(4,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_DIRECAO_VENTO])::NUMERIC::INTEGER,
                NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_DESCRICAO],
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_NASCER_SOL])::NUMERIC::BIGINT,
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_POR_SOL])::NUMERIC::BIGINT
            );

            RETURN NEW;
        END;
        $tg_func$ LANGUAGE plpgsql;

        CREATE TRIGGER inserts_silver_weather
        AFTER INSERT ON bronze.weather_api_data
        FOR EACH ROW 
        EXECUTE FUNCTION bronze.inserts_silver_weather();
        
    ELSE
        RAISE NOTICE 'Trigger inserts_silver_weather já existe!';
    END IF;
END
$$;