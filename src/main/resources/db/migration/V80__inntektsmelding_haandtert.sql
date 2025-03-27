CREATE TABLE inntektsmelding_haandtert
(
    id                  SERIAL PRIMARY KEY NOT NULL,
    vedtaksperiode_id   uuid        NOT NULL,
    inntektsmelding_id  uuid        NOT NULL,
    dokument_id         uuid        NOT NULL,
    opprettet           timestamp   NOT NULL
);