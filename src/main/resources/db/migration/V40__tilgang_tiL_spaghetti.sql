DO $$BEGIN
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'spaghetti')
    THEN
        GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "spaghetti";
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "spaghetti";

    END IF;
END$$;

DO $$ BEGIN
    IF EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'spaghetti')
    THEN
        ALTER DEFAULT PRIVILEGES FOR USER spaghet IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO "spaghetti";
        ALTER DEFAULT PRIVILEGES FOR USER spaghet IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO "spaghetti";
    END IF;
END $$;
