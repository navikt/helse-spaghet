CREATE TABLE godkjenningsbehov
(
    id                UUID        NOT NULL PRIMARY KEY,
    vedtaksperiode_id UUID        NOT NULL,
    periodetype       VARCHAR(64)
);

CREATE TABLE godkjenningsbehov_losning
(
    id                   UUID      NOT NULL REFERENCES godkjenningsbehov (id),
    godkjent             BOOLEAN   NOT NULL,
    automatisk_behandling BOOLEAN   NOT NULL,
    arsak                VARCHAR(255),
    godkjenttidspunkt    TIMESTAMP NOT NULL
);

CREATE TABLE godkjenningsbehov_losning_begrunnelse
(
    id          UUID         NOT NULL REFERENCES godkjenningsbehov (id),
    begrunnelse VARCHAR(255) NOT NULL
);

CREATE TABLE godkjenningsbehov_warnings
(
    vedtaksperiode_id UUID NOT NULL,
    melding           TEXT NOT NULL,
    UNIQUE (vedtaksperiode_id, melding)
)
