CREATE TABLE funksjonell_feil
(
    id                SERIAL PRIMARY KEY,
    vedtaksperiode_id UUID      NOT NULL,
    varselkode        VARCHAR   NOT NULL,
    nivå              VARCHAR   NOT NULL,
    melding           VARCHAR   NOT NULL,
    opprettet         TIMESTAMP NOT NULL

);