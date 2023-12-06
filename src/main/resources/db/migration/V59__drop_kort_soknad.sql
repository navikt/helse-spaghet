ALTER TABLE soknad ADD COLUMN event VARCHAR;

UPDATE soknad SET event='sendt_søknad_nav';
UPDATE soknad SET event='sendt_søknad_arbeidsgiver' WHERE kort_soknad=true;
ALTER TABLE soknad ALTER COLUMN event SET NOT NULL;
ALTER TABLE soknad DROP COLUMN kort_soknad;
