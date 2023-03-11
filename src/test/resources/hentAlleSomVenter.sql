WITH sistePerVedtaksperiodeId AS (
    SELECT DISTINCT ON (vedtaksperiodeId) *
    FROM vedtaksperiode_ventetilstand
    ORDER BY vedtaksperiodeId, tidsstempel DESC
)
SELECT
    vedtaksperiodeId,
    AGE(now(), ventetSiden) as ventetI,
    concat(venterPaHva, ' fordi ' || NULLIF(venterPaHvorfor, ''), venterPaHvorfor) as årsak,
    (CASE WHEN(vedtaksperiodeId = venterpavedtaksperiodeId) THEN 'seg selv' ELSE 'annen' END) venterPaVedtaksperiode,
    (CASE WHEN(organisasjonsnummer = venterPaOrganisasjonsnummer) THEN 'samme' ELSE 'annen' END) venterPaArbeidsgiver
FROM sistePerVedtaksperiodeId
WHERE venter = true
AND ventetSiden < now() - INTERVAL '3 MONTHS'
AND NOT (venterPaHva = 'INNTEKTSMELDING' AND venterPaHvorfor IS NULL)
ORDER BY ventetSiden