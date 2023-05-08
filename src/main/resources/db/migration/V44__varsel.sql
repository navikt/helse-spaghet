CREATE TABLE varsel
(
    id                SERIAL PRIMARY KEY,
    vedtaksperiode_id UUID      NOT NULL,
    varselkode        VARCHAR   NOT NULL,
    nivå              VARCHAR   NOT NULL,
    melding           VARCHAR   NOT NULL,
    opprettet         TIMESTAMP NOT NULL
);

CREATE INDEX varsel_vedtaksperiodeId_idx ON varsel(vedtaksperiode_id);
CREATE INDEX varsel_varselkode_idx ON varsel(varselkode);