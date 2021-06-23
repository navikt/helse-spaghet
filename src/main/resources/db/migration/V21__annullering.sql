CREATE TABLE annullering
(
    fagsystem_id  VARCHAR(32) PRIMARY KEY,
    saksbehandler UUID        NOT NULL,
    begrunnelser  TEXT        NOT NULL,
    kommentar     TEXT        NULL,
    opprettet     TIMESTAMPTZ NOT NULL
);