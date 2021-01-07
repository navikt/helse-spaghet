DELETE FROM godkjenningsbehov_losning;
ALTER TABLE godkjenningsbehov_losning ADD COLUMN godkjent_av VARCHAR(36) NOT NULL;

ALTER TABLE vedtaksperiode_aktivitet ADD COLUMN hendelse_id UUID;
ALTER TABLE vedtaksperiode_tilstandsendring ADD COLUMN hendelse_id UUID;
ALTER TABLE godkjenningsbehov ALTER COLUMN periodetype DROP NOT NULL;
