ALTER TABLE godkjenning
    ADD COLUMN refusjon_type VARCHAR;

CREATE INDEX godkjenning_refusjon_type ON godkjenning(refusjon_type);
