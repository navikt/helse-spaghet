CREATE TABLE vedtaksperiode_venter
(
    vedtaksperiodeId            UUID PRIMARY KEY,
    skjaeringstidspunkt         DATE                            NOT NULL,
    fodselsnummer               VARCHAR                         NOT NULL,
    organisasjonsnummer         VARCHAR                         NOT NULL,
    tidsstempel                 TIMESTAMP DEFAULT now()         NOT NULL,
    ventetSiden                 TIMESTAMP                       NOT NULL,
    venterTil                   TIMESTAMP                       NOT NULL,
    venterForAlltid             BOOLEAN                         NOT NULL,
    venterPaVedtaksperiodeId    UUID                            NOT NULL,
    venterPaOrganisasjonsnummer VARCHAR                         NOT NULL,
    venterPaSkjaeringstidspunkt DATE                            NOT NULL,
    venterPaHva                 VARCHAR                         NOT NULL,
    venterPaHvorfor             VARCHAR,
    hendelseId                  UUID                            NOT NULL,
    hendelse                    JSONB                           NOT NULL
);

CREATE INDEX vedtaksperiode_venter_fodselsnummer_idx ON vedtaksperiode_venter(fodselsnummer);
CREATE INDEX vedtaksperiode_venter_venterPaHva_idx ON vedtaksperiode_venter(venterPaHva);
CREATE INDEX vedtaksperiode_venter_venterPaHvorfor_idx ON vedtaksperiode_venter(venterPaHvorfor);
