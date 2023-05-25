ALTER TABLE vedtaksperiode_ventetilstand
ADD COLUMN gjeldende BOOLEAN NOT NULL DEFAULT false;

UPDATE vedtaksperiode_ventetilstand
SET gjeldende = true
WHERE hendelseId in (
    SELECT hendelseId
    FROM (
         SELECT DISTINCT ON (vedtaksperiodeId) *
         FROM vedtaksperiode_ventetilstand
         ORDER BY vedtaksperiodeId, tidsstempel DESC
    ) AS sistePerVedtaksperiodeId
    WHERE venter = true
)
