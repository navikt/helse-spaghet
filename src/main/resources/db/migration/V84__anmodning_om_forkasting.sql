CREATE TABLE anmodning_om_forkasting
(
    id                  SERIAL PRIMARY KEY,
    vedtaksperiode_id   UUID        NOT NULL,
    fødselsnummer       VARCHAR     NOT NULL,
    organisasjonsnummer VARCHAR     NOT NULL,
    yrkesaktivitetstype VARCHAR     NOT NULL,
    avsender            VARCHAR     NOT NULL,
    årsaker             JSONB       NOT NULL,
    kommentar           TEXT,
    opprettet           TIMESTAMPTZ NOT NULL
);
