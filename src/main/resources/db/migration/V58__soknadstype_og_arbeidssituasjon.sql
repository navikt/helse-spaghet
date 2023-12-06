DROP INDEX soknad_hendelseId_idx;

ALTER TABLE soknad ADD PRIMARY KEY (hendelse_id);

ALTER TABLE soknad
    ADD COLUMN soknadstype VARCHAR,
    ADD COLUMN arbeidssituasjon VARCHAR;

UPDATE soknad SET
    soknadstype='ARBEIDSTAKERE',
    arbeidssituasjon='ARBEIDSTAKER';

ALTER TABLE soknad
    ALTER COLUMN soknadstype SET NOT NULL,
    ALTER COLUMN arbeidssituasjon SET NOT NULL;

CREATE INDEX arbeidssituasjon_idx on soknad(arbeidssituasjon);