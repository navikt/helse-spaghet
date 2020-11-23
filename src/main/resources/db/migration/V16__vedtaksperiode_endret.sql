DELETE
FROM vedtaksperiode_aktivitet;

ALTER TABLE vedtaksperiode_aktivitet
    ADD COLUMN kilde UUID NOT NULL;

CREATE TABLE vedtaksperiode_tilstandsendring
(
    vedtaksperiode_id UUID        NOT NULL,
    tidsstempel       TIMESTAMP   NOT NULL,
    tilstand_fra      VARCHAR(64) NOT NULL,
    tilstand_til      VARCHAR(64) NOT NULL,
    kilde             UUID        NOT NULL,
    kilde_type        VARCHAR(64) NOT NULL
)
