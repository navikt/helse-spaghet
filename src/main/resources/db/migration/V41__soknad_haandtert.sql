CREATE TABLE soknad_haandtert
(
    id                 SERIAL PRIMARY KEY,
    soknad_hendelse_id UUID        NOT NULL,
    vedtaksperiode_id  UUID        NOT NULL,
    opprettet          TIMESTAMP   NOT NULL,
    UNIQUE (soknad_hendelse_id, vedtaksperiode_id)
);