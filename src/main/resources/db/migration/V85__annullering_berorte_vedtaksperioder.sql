CREATE TABLE annullering_berorte_vedtaksperioder
(
    vedtaksperiode_id          UUID    PRIMARY KEY,
    utløsende_vedtaksperiode_id UUID    NOT NULL,
    organisasjonsnummer        VARCHAR NOT NULL,
    yrkesaktivitetstype        VARCHAR NOT NULL
);
