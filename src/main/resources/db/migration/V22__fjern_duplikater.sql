DELETE
FROM warning_for_godkjenning
WHERE id IN (SELECT id
             FROM warning_for_godkjenning wfg
             WHERE wfg.godkjenning_ref IN (SELECT g1.id
                                           FROM godkjenning g1
                                                    INNER JOIN godkjenning g2 ON g1.vedtaksperiode_id = g2.vedtaksperiode_id
                                           WHERE g1.godkjent_tidspunkt = g2.godkjent_tidspunkt
                                             AND g2.id < g1.id
                                             AND g1.godkjent_tidspunkt > '2021-06-08'));

DELETE
FROM warning
WHERE id IN (SELECT id
             FROM warning w
             WHERE w.godkjenning_ref IN (SELECT g1.id
                                         FROM godkjenning g1
                                                  INNER JOIN godkjenning g2 ON g1.vedtaksperiode_id = g2.vedtaksperiode_id
                                         WHERE g1.godkjent_tidspunkt = g2.godkjent_tidspunkt
                                           AND g2.id < g1.id
                                           AND g1.godkjent_tidspunkt > '2021-06-08'));

DELETE
FROM begrunnelse
WHERE id IN (SELECT id
             FROM begrunnelse b
             WHERE b.godkjenning_ref in (SELECT g1.id
                                         FROM godkjenning g1
                                                  INNER JOIN godkjenning g2 ON g1.vedtaksperiode_id = g2.vedtaksperiode_id
                                         WHERE g1.godkjent_tidspunkt = g2.godkjent_tidspunkt
                                           AND g2.id < g1.id
                                           AND g1.godkjent_tidspunkt > '2021-06-08'));

DELETE
FROM godkjenning
WHERE id IN (SELECT g1.id
             FROM godkjenning g1
                      INNER JOIN godkjenning g2 ON g1.vedtaksperiode_id = g2.vedtaksperiode_id
             WHERE g1.godkjent_tidspunkt = g2.godkjent_tidspunkt
               AND g2.id < g1.id
               AND g1.godkjent_tidspunkt > '2021-06-08');
