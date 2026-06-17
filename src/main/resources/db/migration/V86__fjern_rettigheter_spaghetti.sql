DO $$BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'spaghetti') THEN
        REASSIGN OWNED BY "spaghetti" TO "spaghet2";

        REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM "spaghetti";
        REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM "spaghetti";

        ALTER DEFAULT PRIVILEGES FOR USER spaghet IN SCHEMA public
            REVOKE ALL PRIVILEGES ON TABLES FROM "spaghetti";
        ALTER DEFAULT PRIVILEGES FOR USER spaghet IN SCHEMA public
            REVOKE ALL PRIVILEGES ON SEQUENCES FROM "spaghetti";
    END IF;
END $$;