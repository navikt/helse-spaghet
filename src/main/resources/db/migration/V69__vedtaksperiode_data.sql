create table if not exists vedtaksperiode_data
(
    vedtaksperiodeId    uuid primary key,
    fnr                 varchar   not null,
    aktorId             varchar   not null,
    yrkesaktivitet      varchar   not null,
    fom                 timestamp not null,
    tom                 timestamp not null,
    skjaeringstidspunkt date      not null,
    tilstand            varchar   not null,
    oppdatert           timestamp not null
);