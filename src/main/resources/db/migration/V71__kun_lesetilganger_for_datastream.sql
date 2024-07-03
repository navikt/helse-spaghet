DO $$BEGIN
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'bigquery_datastream')
    THEN
        REVOKE USAGE, UPDATE ON ALL SEQUENCES IN SCHEMA public FROM "bigquery_datastream";
        REVOKE INSERT, REFERENCES, TRIGGER, TRUNCATE, DELETE ON ALL TABLES IN SCHEMA public FROM "bigquery_datastream";
    END IF;
END$$;

DO $$ BEGIN
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'bigquery_datastream')
    THEN
        ALTER DEFAULT PRIVILEGES FOR USER spaghet2 IN SCHEMA public GRANT SELECT ON SEQUENCES TO "bigquery_datastream";
        ALTER DEFAULT PRIVILEGES FOR USER spaghet2 IN SCHEMA public GRANT SELECT ON TABLES TO "bigquery_datastream";
    END IF;
END $$;
