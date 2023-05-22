CREATE table varsel(
    varsel_id uuid NOT NULL primary key,
    behandling_id uuid,
    vedtaksperiode_id uuid NOT NULL,
    varselkode varchar NOT NULL,
    tittel varchar NOT NULL,
    kilde varchar NOT NULL,
    status varchar NOT NULL,
    sist_endret timestamp NOT NULL
);