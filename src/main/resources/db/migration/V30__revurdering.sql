DROP TABLE IF EXISTS revurdering_igangsatt_vedtaksperiode;
DROP TABLE IF EXISTS revurdering_igangsatt;

CREATE TYPE revurderingstatus AS ENUM ('IKKE_FERDIG', 'FERDIGSTILT_AUTOMATISK', 'FERDIGSTILT_MANUELT', 'AVVIST_AUTOMATISK', 'AVVIST_MANUELT', 'FEILET', 'ERSTATTET');

CREATE TABLE revurdering
(
    id                      UUID PRIMARY KEY,
    opprettet               TIMESTAMP         NOT NULL,
    kilde                   UUID              NOT NULL,
    skjaeringstidspunkt     DATE              NOT NULL,
    periode_for_endring_fom DATE              NOT NULL,
    periode_for_endring_tom DATE              NOT NULL,
    aarsak                  VARCHAR           NOT NULL,
    status                  revurderingstatus NOT NULL DEFAULT 'IKKE_FERDIG'::revurderingstatus,
    oppdatert               TIMESTAMP
);

CREATE TABLE revurdering_vedtaksperiode
(
    vedtaksperiode_id        UUID              NOT NULL,
    revurdering_id UUID REFERENCES revurdering (id),
    periode_fom              DATE              NOT NULL,
    periode_tom              DATE              NOT NULL,
    skjaeringstidspunkt      DATE              NOT NULL,
    oppdatert                TIMESTAMP,
    status                   revurderingstatus NOT NULL DEFAULT 'IKKE_FERDIG'::revurderingstatus,
    CONSTRAINT pk_revurdering_vedtaksperiode PRIMARY KEY (vedtaksperiode_id, revurdering_id)
);