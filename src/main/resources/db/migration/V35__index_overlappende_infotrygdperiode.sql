CREATE INDEX overlappende_infotrygd_periode_vedtaksperiode_tilstand_idx ON overlappende_infotrygdperiode_etter_infotrygdendring(vedtaksperiode_tilstand);

CREATE INDEX overlappende_infotrygd_periode_idx ON overlappende_infotrygd_periode(type);