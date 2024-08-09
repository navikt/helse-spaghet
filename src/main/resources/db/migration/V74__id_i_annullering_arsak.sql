DROP TABLE annullering_arsak;
CREATE TABLE annullering_arsak
(
    id SERIAL PRIMARY KEY,
    arsak  TEXT        NOT NULL,
    key    VARCHAR(32)       NOT NULL,
    vedtaksperiode_id  VARCHAR REFERENCES annullering (id) NOT NULL
);