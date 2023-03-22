ALTER TABLE godkjenning
    ADD COLUMN er_saksbehandleroverstyringer BOOLEAN DEFAULT false;

CREATE TABLE godkjenning_overstyringer (
    godkjenning_ref INT REFERENCES godkjenning(id) NOT NULL,
    overstyring_hendelse_id UUID NOT NULL
);
