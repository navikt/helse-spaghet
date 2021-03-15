DELETE FROM godkjenningsbehov_losning a USING godkjenningsbehov_losning b WHERE a.CTID < b.CTID AND a.id = b.id;
DELETE FROM godkjenningsbehov_losning_begrunnelse a USING godkjenningsbehov_losning_begrunnelse b WHERE a.CTID < b.CTID AND a.id = b.id;
DELETE FROM vedtaksperiode_tilstandsendring a USING vedtaksperiode_tilstandsendring b WHERE a.CTID < b.CTID AND a.hendelse_id = b.hendelse_id;

ALTER TABLE godkjenningsbehov_losning ADD PRIMARY KEY (id);
ALTER TABLE godkjenningsbehov_losning_begrunnelse ADD PRIMARY KEY (id);
ALTER TABLE vedtaksperiode_tilstandsendring RENAME COLUMN hendelse_id TO id;
ALTER TABLE vedtaksperiode_tilstandsendring ADD PRIMARY KEY (id);
ALTER TABLE vedtaksperiode_aktivitet RENAME COLUMN hendelse_id TO id;