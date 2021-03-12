ALTER TABLE godkjenningsbehov_losning ADD PRIMARY KEY (id);
ALTER TABLE godkjenningsbehov_losning_begrunnelse ADD PRIMARY KEY (id);
ALTER TABLE vedtaksperiode_tilstandsendring RENAME COLUMN hendelse_id TO id;
ALTER TABLE vedtaksperiode_tilstandsendring ADD PRIMARY KEY (id);
ALTER TABLE vedtaksperiode_aktivitet RENAME COLUMN hendelse_id TO id;