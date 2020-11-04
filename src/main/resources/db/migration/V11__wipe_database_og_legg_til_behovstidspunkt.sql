DELETE FROM warning;
DELETE FROM begrunnelse;
DELETE FROM godkjenning;
DELETE FROM godkjenningsbehov_warning;
DELETE FROM godkjenningsbehov_losning_begrunnelse;
DELETE FROM godkjenningsbehov_losning;
DELETE FROM godkjenningsbehov;

ALTER TABLE godkjenningsbehov ADD COLUMN tidspunkt TIMESTAMP NOT NULL;