create table ingestion_archive_processing (
    archive_idempotency_key text primary key,
    source_fingerprint varchar(128) not null,
    projection_revision varchar(64) not null,
    processing_fingerprint varchar(64) not null,
    status varchar(32) not null,
    attempt_token uuid,
    lease_expires_at timestamptz,
    attempt_count integer not null default 0,
    last_attempt_at timestamptz,
    delivered_records bigint not null default 0,
    source_invalid_records bigint not null default 0,
    mapping_rejected_records bigint not null default 0,
    submitted_operations bigint not null default 0,
    succeeded_operations bigint not null default 0,
    failed_operations bigint not null default 0,
    receipt_documents bigint not null default 0,
    first_failed_line bigint,
    failed_at timestamptz,
    completed_at timestamptz,
    last_error_code varchar(64),
    last_error_retryable boolean,
    first_seen_at timestamptz not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint fk_ingestion_archive_processing_archive
        foreign key (archive_idempotency_key)
        references ingestion_archives (idempotency_key),
    constraint uq_ingestion_archive_processing_fingerprint
        unique (processing_fingerprint),
    constraint ck_ingestion_archive_processing_source_fingerprint
        check (btrim(source_fingerprint) <> ''),
    constraint ck_ingestion_archive_processing_projection_revision
        check (btrim(projection_revision) <> ''),
    constraint ck_ingestion_archive_processing_fingerprint
        check (processing_fingerprint ~ '^[0-9a-f]{64}$'),
    constraint ck_ingestion_archive_processing_error_code
        check (
            last_error_code is null
            or last_error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
        ),
    constraint ck_ingestion_archive_processing_status
        check (status in ('PENDING', 'PROCESSING', 'FAILED', 'INDEXED')),
    constraint ck_ingestion_archive_processing_attempt_count
        check (attempt_count >= 0),
    constraint ck_ingestion_archive_processing_counters
        check (
            delivered_records >= 0
            and source_invalid_records >= 0
            and mapping_rejected_records >= 0
            and submitted_operations >= 0
            and succeeded_operations >= 0
            and failed_operations >= 0
            and receipt_documents >= 0
            and mapping_rejected_records <= delivered_records
            and submitted_operations <= delivered_records - mapping_rejected_records
            and succeeded_operations <= submitted_operations
            and failed_operations <= submitted_operations - succeeded_operations
        ),
    constraint ck_ingestion_archive_processing_failed_line
        check (
            (failed_operations = 0 and first_failed_line is null)
            or
            (
                failed_operations > 0
                and first_failed_line is not null
                and first_failed_line > 0
            )
        ),
    constraint ck_ingestion_archive_processing_lease
        check (
            (attempt_token is null and lease_expires_at is null)
            or
            (
                attempt_token is not null
                and lease_expires_at is not null
                and last_attempt_at is not null
                and lease_expires_at > last_attempt_at
            )
        ),
    constraint ck_ingestion_archive_processing_failure
        check (
            (
                last_error_code is null
                and last_error_retryable is null
                and failed_at is null
            )
            or
            (
                last_error_code is not null
                and last_error_retryable is not null
                and failed_at is not null
            )
        ),
    constraint ck_ingestion_archive_processing_status_state
        check (
            (
                status = 'PENDING'
                and attempt_count = 0
                and last_attempt_at is null
                and attempt_token is null
                and lease_expires_at is null
                and failed_at is null
                and completed_at is null
                and last_error_code is null
                and delivered_records = 0
                and source_invalid_records = 0
                and mapping_rejected_records = 0
                and submitted_operations = 0
                and succeeded_operations = 0
                and failed_operations = 0
                and receipt_documents = 0
                and first_failed_line is null
            )
            or
            (
                status = 'PROCESSING'
                and attempt_count > 0
                and last_attempt_at is not null
                and attempt_token is not null
                and lease_expires_at is not null
                and failed_at is null
                and completed_at is null
                and last_error_code is null
            )
            or
            (
                status = 'FAILED'
                and attempt_count > 0
                and last_attempt_at is not null
                and attempt_token is null
                and lease_expires_at is null
                and failed_at is not null
                and completed_at is null
                and last_error_code is not null
            )
            or
            (
                status = 'INDEXED'
                and attempt_count > 0
                and last_attempt_at is not null
                and attempt_token is null
                and lease_expires_at is null
                and failed_at is null
                and completed_at is not null
                and last_error_code is null
                and failed_operations = 0
                and first_failed_line is null
                and submitted_operations = succeeded_operations
                and submitted_operations = delivered_records - mapping_rejected_records
                and receipt_documents = succeeded_operations
            )
        )
);

create index ix_ingestion_archive_processing_status
    on ingestion_archive_processing (status);

create index ix_ingestion_archive_processing_lease_expires_at
    on ingestion_archive_processing (lease_expires_at)
    where status = 'PROCESSING';
