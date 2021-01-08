DELETE FROM godkjenningsbehov_losning;
ALTER TABLE godkjenningsbehov_losning ADD COLUMN godkjent_av VARCHAR(36);

ALTER TABLE vedtaksperiode_aktivitet ADD COLUMN hendelse_id UUID;
ALTER TABLE vedtaksperiode_tilstandsendring ADD COLUMN hendelse_id UUID;

ALTER TABLE godkjenningsbehov ALTER COLUMN periodetype SET NOT NULL;
ALTER TABLE godkjenningsbehov_losning ALTER COLUMN godkjent_av SET NOT NULL;

DELETE FROM warning;
DELETE FROM begrunnelse;
DELETE FROM godkjenning;
DELETE FROM godkjenningsbehov_warning;
DELETE FROM godkjenningsbehov_losning_begrunnelse;
DELETE FROM godkjenningsbehov_losning;
DELETE FROM godkjenningsbehov;
DELETE FROM vedtaksperiode_tilstandsendring;
DELETE FROM vedtaksperiode_aktivitet;
