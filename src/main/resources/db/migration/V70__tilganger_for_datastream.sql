DO $$BEGIN
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'bigquery_datastream')
    THEN
        GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "bigquery_datastream";
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "bigquery_datastream";
    END IF;
END$$;

DO $$ BEGIN
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'bigquery_datastream')
    THEN
        ALTER DEFAULT PRIVILEGES FOR USER spaghet2 IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO "bigquery_datastream";
        ALTER DEFAULT PRIVILEGES FOR USER spaghet2 IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO "bigquery_datastream";
    END IF;
END $$;
