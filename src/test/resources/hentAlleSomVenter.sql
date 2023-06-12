SELECT
    --count(1),                                                             -- Kommenter inn denne istedenfor * om man vil ha antall stuck per årsak
    *,
    concat(venterPaHva, ' fordi ' || NULLIF(venterPaHvorfor, '')) as årsak
FROM vedtaksperiode_ventetilstand
WHERE gjeldende = true
AND venter = true                                                           -- Kun de som venter nå
AND ventetSiden < now() - INTERVAL '3 MONTHS'                               -- Ventet minst 3 måneder
AND date_part('Year', ventertil) = 9999                                     -- Har ingen timeout, så blir ikke fanget opp av makstid ved påminnelser
AND NOT venterpahva = 'GODKJENNING'                                         -- Har ingen timeout på å vente på godkjenning, men de er heller ikke stuck
AND NOT venterpahvorfor = 'VIL_UTBETALES'                                   -- Alle AUU'er som vil utbetales, egentlig bør tas med for å få oversikt over alt som er stuck
--GROUP BY årsak                                                            -- Kommenter inn denne om man vil ha antall stuck per årsak
