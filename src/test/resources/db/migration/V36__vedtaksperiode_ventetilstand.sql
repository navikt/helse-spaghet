CREATE TABLE vedtaksperiode_ventetilstand
(
    hendelseId                  UUID PRIMARY KEY,
    hendelse                    JSONB,
    venter                      BOOLEAN,
    vedtaksperiodeId            UUID NOT NULL,
    organisasjonsnummer         VARCHAR NOT NULL,
    tidsstempel                 TIMESTAMP DEFAULT now() NOT NULL,
    ventetSiden                 TIMESTAMP,
    venterTil                   TIMESTAMP,
    venterPåVedtaksperiodeId    UUID,
    venterPåOrganisasjonsnummer VARCHAR,
    venterPåHva                 VARCHAR,
    venterPåHvorfor             VARCHAR
);



