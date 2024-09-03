alter table varsel
    rename column behandling_id to godkjenning_varsel_id;
alter table varsel
    add behandling_id uuid;

alter table godkjenning
    rename column behandling_id to godkjenning_varsel_id;
alter table godkjenning
    add behandling_id uuid;