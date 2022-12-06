CREATE TABLE revurdering_igangsatt
(
    id                      SERIAL PRIMARY KEY,
    opprettet               TIMESTAMP NOT NULL,
    fodselsnummer           VARCHAR   NOT NULL,
    aktor_id                VARCHAR   NOT NULL,
    kilde                   UUID      NOT NULL,
    skjaeringstidspunkt     DATE      NOT NULL,
    periode_for_endring_fom DATE      NOT NULL,
    periode_for_endring_tom DATE      NOT NULL,
    aarsak                  VARCHAR   NOT NULL
);

CREATE TABLE revurdering_igangsatt_vedtaksperiode
(
    id                       SERIAL PRIMARY KEY,
    revurdering_igangsatt_id INT REFERENCES revurdering_igangsatt (id),
    vedtaksperiode_id        UUID    NOT NULL,
    periode_fom              DATE    NOT NULL,
    periode_tom              DATE    NOT NULL,
    skjaeringstidspunkt      DATE    NOT NULL,
    orgnummer                VARCHAR NOT NULL
)