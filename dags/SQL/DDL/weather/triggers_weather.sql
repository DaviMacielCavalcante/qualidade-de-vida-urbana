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
            CONST_CONDITION TEXT := 'condition';
            
            CONST_LATITUDE TEXT := 'latitude';
            CONST_LONGITUDE TEXT := 'longitude';
            
            -- Constantes para campos de dados meteorológicos
            CONST_UV TEXT := 'uv';
            CONST_CLOUD TEXT := 'cloud';
            CONST_IS_DAY TEXT := 'is_day';
            CONST_TEMP_C TEXT := 'temp_c';
            CONST_TEMP_F TEXT := 'temp_f';
            CONST_VIS_KM TEXT := 'vis_km';
            CONST_GUST_KPH TEXT := 'gust_kph';
            CONST_GUST_MPH TEXT := 'gust_mph';
            CONST_HUMIDITY TEXT := 'humidity';
            CONST_WIND_DIR TEXT := 'wind_dir';
            CONST_WIND_KPH TEXT := 'wind_kph';
            CONST_WIND_MPH TEXT := 'wind_mph';
            CONST_CODE TEXT := 'code';
            CONST_ICON TEXT := 'icon';
            CONST_TEXT TEXT := 'text';
            CONST_PRECIP_IN TEXT := 'precip_in';
            CONST_PRECIP_MM TEXT := 'precip_mm';
            CONST_VIS_MILES TEXT := 'vis_miles';
            CONST_DEWPOINT_C TEXT := 'dewpoint_c';
            CONST_DEWPOINT_F TEXT := 'dewpoint_f';
            CONST_FEELSLIKE_C TEXT := 'feelslike_c';
            CONST_FEELSLIKE_F TEXT := 'feelslike_f';
            CONST_HEATINDEX_C TEXT := 'heatindex_c';
            CONST_HEATINDEX_F TEXT := 'heatindex_f';
            CONST_PRESSURE_IN TEXT := 'pressure_in';
            CONST_PRESSURE_MB TEXT := 'pressure_mb';
            CONST_WIND_DEGREE TEXT := 'wind_degree';
            CONST_WINDCHILL_C TEXT := 'windchill_c';
            CONST_WINDCHILL_F TEXT := 'windchill_f';
            CONST_LAST_UPDATED TEXT := 'last_updated';
            CONST_LAST_UPDATED_EPOCH TEXT := 'last_updated_epoch';
        BEGIN
            -- Obtém a primeira chave (nome da região) do JSON
            SELECT key INTO region_key
            FROM jsonb_object_keys(NEW.data) AS key
            LIMIT 1;
            
            INSERT INTO silver.weather_api_data(
                regiao,
                uv,
                cloud,
                is_day,
                temp_c,
                temp_f,
                vis_km,
                gust_kph,
                gust_mph,
                humidity,
                wind_dir,
                wind_kph,
                wind_mph,
                condition_code,
                condition_icon,
                condition_text,
                precip_in,
                precip_mm,
                vis_miles,
                dewpoint_c,
                dewpoint_f,
                feelslike_c,
                feelslike_f,
                heatindex_c,
                heatindex_f,
                pressure_in,
                pressure_mb,
                wind_degree,
                windchill_c,
                windchill_f,
                last_updated,
                last_updated_epoch,
                location_lat,
                location_lon
            ) 
            VALUES (
                region_key,
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_UV])::NUMERIC(5,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_CLOUD])::NUMERIC::INTEGER,
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_IS_DAY])::NUMERIC::INTEGER,
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_TEMP_C])::NUMERIC(4,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_TEMP_F])::NUMERIC(5,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_VIS_KM])::NUMERIC(4,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_GUST_KPH])::NUMERIC(4,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_GUST_MPH])::NUMERIC(4,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_HUMIDITY])::NUMERIC::INTEGER,
                NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_WIND_DIR],
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_WIND_KPH])::NUMERIC(4,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_WIND_MPH])::NUMERIC(4,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_CONDITION, CONST_CODE])::NUMERIC::INTEGER,
                NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_CONDITION, CONST_ICON],
                NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_CONDITION, CONST_TEXT],
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_PRECIP_IN])::NUMERIC(6,4),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_PRECIP_MM])::NUMERIC(6,4),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_VIS_MILES])::NUMERIC(4,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_DEWPOINT_C])::NUMERIC(4,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_DEWPOINT_F])::NUMERIC(5,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_FEELSLIKE_C])::NUMERIC(4,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_FEELSLIKE_F])::NUMERIC(5,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_HEATINDEX_C])::NUMERIC(4,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_HEATINDEX_F])::NUMERIC(5,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_PRESSURE_IN])::NUMERIC(4,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_PRESSURE_MB])::NUMERIC(6,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_WIND_DEGREE])::NUMERIC::INTEGER,
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_WINDCHILL_C])::NUMERIC(4,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_WINDCHILL_F])::NUMERIC(5,2),
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_LAST_UPDATED])::TIMESTAMP,
                (NEW.data #>> ARRAY[region_key, CONST_DATA, CONST_LAST_UPDATED_EPOCH])::NUMERIC::BIGINT,
                (NEW.data #>> ARRAY[region_key, CONST_LATITUDE])::NUMERIC(6,4),
                (NEW.data #>> ARRAY[region_key, CONST_LONGITUDE])::NUMERIC(6,4)
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