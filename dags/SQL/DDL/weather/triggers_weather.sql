DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger 
        WHERE tgname = 'inserts_silver_weather'
        AND tgrelid = 'bronze.weather_api_data'::regclass
    ) THEN
        CREATE OR REPLACE FUNCTION bronze.inserts_silver_weather()
        RETURNS TRIGGER AS $tg_func$
        BEGIN
            INSERT INTO silver.weather_api_data(
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
                location_lon,
                location_name,
                location_tz_id,
                location_region,
                location_country,
                location_localtime,
                location_localtime_epoch
            ) 
            VALUES (
                (NEW.data#>>'{current,uv}')::NUMERIC(5,2),
                (NEW.data#>>'{current,cloud}')::NUMERIC::INTEGER,
                (NEW.data#>>'{current,is_day}')::NUMERIC::INTEGER,
                (NEW.data#>>'{current,temp_c}')::NUMERIC(4,2),
                (NEW.data#>>'{current,temp_f}')::NUMERIC(4,2),
                (NEW.data#>>'{current,vis_km}')::NUMERIC(4,2),
                (NEW.data#>>'{current,gust_kph}')::NUMERIC(4,2),
                (NEW.data#>>'{current,gust_mph}')::NUMERIC(4,2),
                (NEW.data#>>'{current,humidity}')::NUMERIC::INTEGER,
                NEW.data#>>'{current,wind_dir}',
                (NEW.data#>>'{current,wind_kph}')::NUMERIC(4,2),
                (NEW.data#>>'{current,wind_mph}')::NUMERIC(4,2),
                (NEW.data#>>'{current,condition,code}')::NUMERIC::INTEGER,
                NEW.data#>>'{current,condition,icon}',
                NEW.data#>>'{current,condition,text}',
                (NEW.data#>>'{current,precip_in}')::NUMERIC(6,4),
                (NEW.data#>>'{current,precip_mm}')::NUMERIC(6,4),
                (NEW.data#>>'{current,vis_miles}')::NUMERIC(4,2),
                (NEW.data#>>'{current,dewpoint_c}')::NUMERIC(4,2),
                (NEW.data#>>'{current,dewpoint_f}')::NUMERIC(4,2),
                (NEW.data#>>'{current,feelslike_c}')::NUMERIC(4,2),
                (NEW.data#>>'{current,feelslike_f}')::NUMERIC(4,2),
                (NEW.data#>>'{current,heatindex_c}')::NUMERIC(4,2),
                (NEW.data#>>'{current,heatindex_f}')::NUMERIC(4,2),
                (NEW.data#>>'{current,pressure_in}')::NUMERIC(4,2),
                (NEW.data#>>'{current,pressure_mb}')::NUMERIC(6,2),
                (NEW.data#>>'{current,wind_degree}')::NUMERIC::INTEGER,
                (NEW.data#>>'{current,windchill_c}')::NUMERIC(4,2),
                (NEW.data#>>'{current,windchill_f}')::NUMERIC(4,2),
                (NEW.data#>>'{current,last_updated}')::TIMESTAMP,
                (NEW.data#>>'{current,last_updated_epoch}')::NUMERIC::BIGINT,
                (NEW.data#>>'{location,lat}')::NUMERIC(6,4),
                (NEW.data#>>'{location,lon}')::NUMERIC(6,4),
                NEW.data#>>'{location,name}',
                NEW.data#>>'{location,tz_id}',
                NEW.data#>>'{location,region}',
                NEW.data#>>'{location,country}',
                (NEW.data#>>'{location,localtime}')::TIMESTAMP,
                (NEW.data#>>'{location,localtime_epoch}')::NUMERIC::BIGINT
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