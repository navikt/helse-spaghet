CREATE TABLE annullering_arsak
(
    vedtaksperiode_id  VARCHAR PRIMARY KEY REFERENCES annullering (id) NOT NULL,
    arsak  TEXT        NOT NULL,
    key    VARCHAR(32)       NOT NULL
);