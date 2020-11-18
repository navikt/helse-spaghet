CREATE TABLE vedtaksperiode_aktivitet
(
    vedtaksperiode_id UUID NOT NULL,
    melding           TEXT NOT NULL,
    level             VARCHAR(32) NOT NULL,
    tidsstempel       TIMESTAMP NOT NULL,
    UNIQUE (vedtaksperiode_id, melding, tidsstempel)
)
