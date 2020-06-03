CREATE TABLE godkjenning
(
    id                 SERIAL PRIMARY KEY,
    vedtaksperiode_id  UUID        NOT NULL,
    aktor_id           VARCHAR(16) NOT NULL,
    fodselsnummer      VARCHAR(16) NOT NULL,
    godkjent_av        VARCHAR(64) NOT NULL,
    godkjent_tidspunkt TIMESTAMPTZ NOT NULL,
    godkjent           BOOLEAN     NOT NULL,
    arsak              TEXT,
    kommentar          TEXT
);

CREATE TABLE begrunnelse
(
    id              SERIAL PRIMARY KEY,
    godkjenning_ref INT REFERENCES godkjenning (id) NOT NULL,
    tekst           TEXT                            NOT NULL
);

CREATE TABLE warning
(
    id              SERIAL PRIMARY KEY,
    godkjenning_ref INT REFERENCES godkjenning (id) NOT NULL,
    tekst           TEXT                            NOT NULL
)
