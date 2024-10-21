CREATE TABLE utkast_til_vedtak
(
    id UUID PRIMARY KEY,
    opprettet TIMESTAMP NOT NULL,
    vedtaksperiode_id UUID NOT NULL,
    behandling_id UUID NOT NULL,
    tags VARCHAR[] NOT NULL DEFAULT ARRAY[]::VARCHAR[],
    UNIQUE (behandling_id, tags)
)