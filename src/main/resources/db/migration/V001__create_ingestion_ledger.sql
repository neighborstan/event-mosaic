create table ingestion_archives (
    idempotency_key text primary key,
    archive_name text not null,
    archive_url text not null,
    expected_md5 varchar(64),
    actual_md5 varchar(64),
    archive_type varchar(64) not null,
    status varchar(32) not null,
    file_size_bytes bigint,
    raw_record_count bigint not null default 0,
    parsed_record_count bigint not null default 0,
    processed_record_count bigint not null default 0,
    indexed_record_count bigint not null default 0,
    failed_record_count bigint not null default 0,
    first_seen_at timestamptz not null,
    last_attempt_at timestamptz,
    completed_at timestamptz,
    attempt_count integer not null default 0,
    last_error text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index ix_ingestion_archives_status
    on ingestion_archives (status);

create index ix_ingestion_archives_archive_type
    on ingestion_archives (archive_type);

create index ix_ingestion_archives_first_seen_at
    on ingestion_archives (first_seen_at);
