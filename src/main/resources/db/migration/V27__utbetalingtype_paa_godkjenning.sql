ALTER TABLE godkjenning
ADD COLUMN utbetaling_type VARCHAR;

CREATE INDEX godkjenning_utbetaling_type ON godkjenning(utbetaling_type);
