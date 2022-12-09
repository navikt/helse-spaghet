ALTER TABLE revurdering_igangsatt ADD COLUMN type_endring VARCHAR NOT NULL DEFAULT 'REVURDERING';

ALTER TABLE revurdering_igangsatt_vedtaksperiode ADD COLUMN type_endring VARCHAR NOT NULL DEFAULT 'REVURDERING';
