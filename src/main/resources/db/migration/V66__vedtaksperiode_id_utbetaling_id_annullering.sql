CREATE TYPE id_type AS ENUM('FAGSYSTEM_ID', 'VEDTAKSPERIODE_ID');

ALTER TABLE annullering RENAME COLUMN fagsystem_id TO id;
ALTER TABLE annullering ADD COLUMN id_type id_type NOT NULL default 'FAGSYSTEM_ID';
ALTER TABLE annullering ALTER COLUMN id_type DROP DEFAULT;

CREATE INDEX ON annullering(id_type);

ALTER TABLE annullering ALTER COLUMN id TYPE varchar;
