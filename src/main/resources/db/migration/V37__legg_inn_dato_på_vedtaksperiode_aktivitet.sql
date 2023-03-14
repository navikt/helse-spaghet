ALTER TABLE vedtaksperiode_aktivitet ADD COLUMN dato DATE;
UPDATE vedtaksperiode_aktivitet SET dato = tidsstempel::date;
ALTER TABLE vedtaksperiode_aktivitet ALTER COLUMN dato SET NOT NULL;

CREATE INDEX ON vedtaksperiode_aktivitet(dato);
