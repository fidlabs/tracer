-- PostgreSQL schema for filecoin database
-- Note: Connect to 'filecoin' database before running this script
-- Or run: psql -U airflow -d filecoin -f ps_filecoin_schema.sql

create table if not exists deals
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

create index if not exists deals_dealid_index
    on deals ("dealId");

create index if not exists deals_claimid_index
    on deals ("claimId");

create index if not exists dealproposals_dealid_index
    on deals ("dealId");

create table if not exists verifier_allowance
(
    id           serial
        constraint verifier_allowance_id_pk
            primary key,
    "verifierId" varchar,
    height       integer,
    allowance    numeric,
    "msgCid"     varchar,
    type         varchar,
    constraint verifier_allowance_pk
        unique ("msgCid", "verifierId", height)
);

create table if not exists verified_client_allowance
(
    id           serial
        constraint verified_client_allowance_id_pk
            primary key,
    "clientId"   varchar,
    "verifierId" varchar,
    height       integer,
    allowance    numeric,
    "msgCid"     varchar,
    type         varchar,
    constraint verified_client_allowance_pk
        unique ("verifierId", "clientId", "msgCid", height)
);

create table if not exists allocations
(
    "allocationId"            integer not null
        constraint dc_allocation_allocation_id_pk
            primary key,
    "clientId"                integer,
    "providerId"              integer,
    "pieceCid"                varchar,
    "pieceSize"               numeric,
    "termMax"                 integer,
    "termMin"                 integer,
    expiration                integer,
    "contractImmediateCaller" integer
);

create table if not exists deal_proposals
(
    id                     serial
        constraint dealproposals_id_pk
            primary key,
    "dealId"               integer default 0,
    "clientId"             varchar,
    "providerId"           varchar,
    "pieceCid"             varchar,
    "pieceSize"            numeric,
    "startEpoch"           integer,
    "endEpoch"             integer,
    "clientCollateral"     numeric,
    "providerCollateral"   numeric,
    "storagePricePerEpoch" numeric,
    label                  varchar,
    verified               boolean
);

create unique index if not exists deal_proposals_dealid_index
    on deal_proposals ("dealId");

create table if not exists sector_activations
(
    id                 serial
        constraint sector_activations_id_pk
            primary key,
    "dealId"           integer default 0,
    "providerId"       integer,
    "activationHeight" integer,
    "sectorNumber"     integer
);


create unique index if not exists sector_activations_dealid_index
    on sector_activations ("dealId");

create table if not exists meta_allocators
(
    "addressId"     integer not null
        constraint meta_allocators_pk
            primary key,
    address         varchar,
    "addressEth"    varchar,
    "robustAddress" varchar
);

create table if not exists client_contracts
(
    "addressId"     integer not null
        constraint client_contracts_pk
            primary key,
    address         varchar,
    "addressEth"    varchar,
    "robustAddress" varchar
);

create table if not exists actors
(
    "addressId"  integer,
    address      varchar,
    "addressEth" varchar
);