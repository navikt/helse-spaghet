CREATE TABLE oppgave(
    id BIGINT UNIQUE NOT NULL,
    fødselsnummer VARCHAR NOT NULL,
    opprettet timestamp NOT NULL
);

CREATE TABLE oppgave_endret(
    løpenummer BIGSERIAL PRIMARY KEY NOT NULL,
    oppgave_ref BIGINT REFERENCES oppgave(id) NOT NULL,
    egenskaper VARCHAR[] NOT NULL,
    tilstand VARCHAR NOT NULL,
    endret_tidspunkt timestamp NOT NULL,
    tildelt BOOLEAN NOT NULL
);