CREATE TABLE warning_for_godkjenning
(
    id              SERIAL PRIMARY KEY,
    godkjenning_ref INT REFERENCES godkjenning (id) NOT NULL,
    melding         TEXT                            NOT NULL,
    kilde           TEXT                            NOT NULL,
    CONSTRAINT Dedup UNIQUE (godkjenning_ref, melding)
)
