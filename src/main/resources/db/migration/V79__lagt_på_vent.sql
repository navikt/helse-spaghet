CREATE TABLE lagt_paa_vent
(
    id SERIAL PRIMARY KEY NOT NULL,
    oppgave_id          BIGINT      NOT NULL,
    behandling_id       uuid        NOT NULL,
    skal_tildeles       BOOLEAN     NOT NULL,
    frist               DATE        NOT NULL,
    opprettet           TIMESTAMPTZ   NOT NULL,
    saksbehandler_oid   uuid        NOT NULL,
    saksbehandler_ident VARCHAR(32) NOT NULL,
    notat_tekst         TEXT
);

CREATE TABLE lagt_paa_vent_arsak
(
    lagt_paa_vent_id BIGINT REFERENCES lagt_paa_vent (id) NOT NULL,
    arsak      TEXT                                                      NOT NULL,
    key        VARCHAR(32)                                               NOT NULL
);