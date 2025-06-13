CREATE TABLE vedtaksdata
(
    id                      SERIAL PRIMARY KEY NOT NULL,
    vedtaksperiode_id       uuid               NOT NULL,
    opprettet               timestamp          NOT NULL,
    aktor_id                text               NOT NULL,
    behandling_id           uuid               NOT NULL,
    datapakke               JSON               NOT NULL
);