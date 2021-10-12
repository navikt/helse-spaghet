CREATE TABLE hendelse_ikke_håndtert_årsak
(
    id                SERIAL PRIMARY KEY,
    hendelse_id        UUID NOT NULL,
    årsak             TEXT NOT NULL,
    tidsstempel       TIMESTAMP NOT NULL,
    CONSTRAINT hendelse_id_årsak UNIQUE (hendelse_id, årsak)
);
