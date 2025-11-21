CREATE DATABASE filecoin;

create table public.deals
(
    id             serial
        constraint deals_id_pk
            primary key,
    "dealId"       integer default 0,
    "claimId"      integer default 0 not null,
    "clientId"     integer,
    "dcSource"     integer,
    "providerId"   integer,
    "sectorId"     integer,
    "pieceCid"     varchar,
    "pieceSize"    numeric,
    "termMax"      integer,
    "termMin"      integer,
    "termStart"    integer,
    "sectorExpiry" integer
);

create index deals_dealid_index
    on public.deals ("dealId");

create index deals_claimid_index
    on public.deals ("claimId");

create table public.verifier_allowance
(
    id           serial
        constraint verifier_allowance_id_pk
            primary key,
    "verifierId" varchar,
    height       integer,
    allowance    numeric,
    "msgCid"     varchar
);

create unique index verifier_allowance_pk
    on public.verifier_allowance ("msgCid", "verifierId", height);

create table public.verified_client_allowance
(
    id           serial
        constraint verified_client_allowance_id_pk
            primary key,
    "clientId"   varchar,
    "verifierId" varchar,
    height       integer,
    allowance    numeric,
    "msgCid"     varchar
);

create unique index verified_client_allowance_pk
    on public.verified_client_allowance ("verifierId", "clientId", "msgCid", height);

create table public.allocations
(
    "allocationId" integer not null
        constraint dc_allocation_allocation_id_pk
            primary key,
    "clientId"     integer,
    "providerId"   integer,
    "pieceCid"     varchar,
    "pieceSize"    numeric,
    "termMax"      integer,
    "termMin"      integer,
    expiration     integer
);


