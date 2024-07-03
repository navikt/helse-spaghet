DO $$BEGIN
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'bigquery_datastream')
    THEN
        REVOKE UPDATE ON ALL TABLES IN SCHEMA public FROM "bigquery_datastream";
    END IF;
END$$;
