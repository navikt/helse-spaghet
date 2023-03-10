CREATE TABLE vedtaksperiode_ventetilstand
(
    hendelseId                  UUID PRIMARY KEY,
    hendelse                    JSONB,
    venter                      BOOLEAN,
    vedtaksperiodeId            UUID NOT NULL,
    fodselsnummer              VARCHAR NOT NULL,
    organisasjonsnummer         VARCHAR NOT NULL,
    tidsstempel                 TIMESTAMP DEFAULT now() NOT NULL,
    ventetSiden                 TIMESTAMP,
    venterTil                   TIMESTAMP,
    venterPaVedtaksperiodeId    UUID,
    venterPaOrganisasjonsnummer VARCHAR,
    venterPaHva                 VARCHAR,
    venterPaHvorfor             VARCHAR
);



