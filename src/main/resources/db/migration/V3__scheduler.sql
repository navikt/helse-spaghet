UPDATE godkjenning
SET kommentar = NULL
WHERE kommentar IS NOT NULL
  AND trim(kommentar) = '';

CREATE TABLE schedule
(
    melding_sendt DATE
)
