create table skatteinntekter_lagt_til_grunn(
    lopenummer SERIAL PRIMARY KEY,
    vedtaksperiode_id UUID NOT NULL,
    behandling_id UUID NOT NULL,
    hendelse_id UUID NOT NULL,
    fnr VARCHAR NOT NULL,
    orgnummer VARCHAR NOT NULL,
    data JSONB NOT NULL
);