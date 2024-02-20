ALTER TABLE vedtaksperiode_ventetilstand
ADD COLUMN skjaeringstidspunkt DATE NOT NULL DEFAULT '9999-12-31'::date,
ADD COLUMN venterPaSkjaeringstidspunkt DATE;

UPDATE vedtaksperiode_ventetilstand
SET venterPaSkjaeringstidspunkt='9999-12-31'::date
WHERE venter=true;

ALTER TABLE vedtaksperiode_ventetilstand
ALTER COLUMN skjaeringstidspunkt DROP DEFAULT;