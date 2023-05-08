CREATE TABLE soknad
(
    flexId      UUID NOT NULL,
    hendelseId  UUID NOT NULL,
    kort_soknad BOOLEAN
);

CREATE INDEX kort_soknad_idx ON soknad(kort_soknad);
CREATE INDEX soknad_hendelseId_idx ON soknad(hendelseId);

