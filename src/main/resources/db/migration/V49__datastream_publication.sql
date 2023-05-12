DO
$$
    BEGIN
        if not exists
            (select 1 from pg_publication where pubname = 'spaghet_publication')
        then
            CREATE PUBLICATION spaghet_publication for ALL TABLES;
        end if;
    end;
$$;
