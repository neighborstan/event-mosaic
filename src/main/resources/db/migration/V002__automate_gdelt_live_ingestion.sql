alter table ingestion_source_state
    drop constraint ck_ingestion_source_state_policy;

alter table ingestion_source_state
    add constraint ck_ingestion_source_state_policy
        check (first_run_policy in ('LATEST', 'FIXED', 'RECENT_WINDOW'));

alter table ingestion_source_poll_state
    add column retry_sequence bigint not null default 1,
    add column last_exhausted_at timestamptz,
    add column last_exhausted_error_code varchar(64);

update ingestion_source_poll_state
set last_exhausted_at = failed_at,
    last_exhausted_error_code = last_error_code
where status = 'FAILED'
  and last_error_retryable
  and automatic_retries_used = automatic_retry_limit;

alter table ingestion_source_poll_state
    add constraint ck_ingestion_source_poll_retry_sequence
        check (retry_sequence > 0),
    add constraint ck_ingestion_source_poll_exhausted_error_code
        check (
            last_exhausted_error_code is null
            or last_exhausted_error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
        ),
    add constraint ck_ingestion_source_poll_exhausted_evidence
        check (
            (last_exhausted_at is null and last_exhausted_error_code is null)
            or
            (last_exhausted_at is not null and last_exhausted_error_code is not null)
        );

alter table ingestion_archives
    add column retry_sequence bigint not null default 1,
    add column last_exhausted_at timestamptz,
    add column last_exhausted_error_code varchar(64);

update ingestion_archives
set last_exhausted_at = failed_at,
    last_exhausted_error_code = last_error_code
where status = 'FAILED'
  and last_error_retryable
  and automatic_retries_used = automatic_retry_limit;

alter table ingestion_archives
    add constraint ck_ingestion_archives_retry_sequence
        check (retry_sequence > 0),
    add constraint ck_ingestion_archives_exhausted_error_code
        check (
            last_exhausted_error_code is null
            or last_exhausted_error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
        ),
    add constraint ck_ingestion_archives_exhausted_evidence
        check (
            (last_exhausted_at is null and last_exhausted_error_code is null)
            or
            (last_exhausted_at is not null and last_exhausted_error_code is not null)
        );

alter table ingestion_archive_processing
    add column retry_sequence bigint not null default 1,
    add column last_exhausted_at timestamptz,
    add column last_exhausted_error_code varchar(64);

update ingestion_archive_processing
set last_exhausted_at = failed_at,
    last_exhausted_error_code = last_error_code
where status = 'FAILED'
  and last_error_retryable
  and automatic_retries_used = automatic_retry_limit;

alter table ingestion_archive_processing
    add constraint ck_ingestion_archive_processing_retry_sequence
        check (retry_sequence > 0),
    add constraint ck_ingestion_archive_processing_exhausted_error_code
        check (
            last_exhausted_error_code is null
            or last_exhausted_error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
        ),
    add constraint ck_ingestion_archive_processing_exhausted_evidence
        check (
            (last_exhausted_at is null and last_exhausted_error_code is null)
            or
            (last_exhausted_at is not null and last_exhausted_error_code is not null)
        );

create table ingestion_cycle_state (
    source_name varchar(64) primary key,
    status varchar(16) not null,
    owner_token uuid,
    fencing_epoch bigint not null default 0,
    lease_expires_at timestamptz,
    last_started_at timestamptz,
    last_progress_at timestamptz,
    last_terminal_at timestamptz,
    last_outcome varchar(32),
    state_version bigint not null default 0,
    created_at timestamptz not null default clock_timestamp(),
    updated_at timestamptz not null default clock_timestamp(),
    constraint ck_ingestion_cycle_state_name
        check (btrim(source_name) <> ''),
    constraint ck_ingestion_cycle_state_status
        check (status in ('IDLE', 'ACTIVE')),
    constraint ck_ingestion_cycle_state_versions
        check (fencing_epoch >= 0 and state_version >= 0),
    constraint ck_ingestion_cycle_state_ownership
        check (
            (
                status = 'IDLE'
                and owner_token is null
                and lease_expires_at is null
            )
            or
            (
                status = 'ACTIVE'
                and owner_token is not null
                and fencing_epoch > 0
                and lease_expires_at is not null
                and last_started_at is not null
                and lease_expires_at > last_started_at
            )
        ),
    constraint ck_ingestion_cycle_state_progress
        check (
            last_progress_at is null
            or
            (last_started_at is not null and last_progress_at >= last_started_at)
        ),
    constraint ck_ingestion_cycle_state_outcome
        check (
            last_outcome is null
            or last_outcome in (
                'COMPLETED',
                'UNCHANGED',
                'RETRY_DEFERRED',
                'SKIPPED_ACTIVE_CYCLE',
                'OWNERSHIP_LOST',
                'STORAGE_PRESSURE',
                'DEADLINE',
                'INTERRUPTED',
                'EXPECTED_FAILURE',
                'INTERNAL_FAILURE'
            )
        ),
    constraint ck_ingestion_cycle_state_terminal
        check (
            (last_terminal_at is null and last_outcome is null)
            or
            (last_terminal_at is not null and last_outcome is not null)
        )
);

create table ingestion_recent_recovery_plan (
    source_name varchar(64) primary key,
    generation bigint not null,
    window_from timestamptz not null,
    window_to timestamptz not null,
    source_frontier timestamptz not null,
    catalog_status varchar(32) not null,
    master_generation varchar(128),
    master_etag varchar(256),
    master_verified_from timestamptz,
    master_verified_at timestamptz,
    state_version bigint not null default 0,
    created_at timestamptz not null default clock_timestamp(),
    updated_at timestamptz not null default clock_timestamp(),
    constraint fk_ingestion_recent_recovery_plan_source
        foreign key (source_name) references ingestion_source_state (source_name),
    constraint ck_ingestion_recent_recovery_plan_generation
        check (generation > 0 and state_version >= 0),
    constraint ck_ingestion_recent_recovery_plan_status
        check (catalog_status in ('PENDING', 'CATALOG_COMPLETE')),
    constraint ck_ingestion_recent_recovery_plan_window
        check (
            window_to = window_from + interval '24 hours'
            and mod(extract(epoch from window_from), 900) = 0
            and mod(extract(epoch from window_to), 900) = 0
            and mod(extract(epoch from source_frontier), 900) = 0
        ),
    constraint ck_ingestion_recent_recovery_plan_master_generation
        check (
            master_generation is null
            or master_generation ~ '^[0-9]+$'
        ),
    constraint ck_ingestion_recent_recovery_plan_master_evidence
        check (
            (
                master_generation is null
                and master_etag is null
                and master_verified_from is null
                and master_verified_at is null
            )
            or
            (
                master_generation is not null
                and master_etag is not null
                and btrim(master_etag) <> ''
                and master_verified_from is not null
                and master_verified_at is not null
            )
        ),
    constraint ck_ingestion_recent_recovery_plan_complete
        check (
            catalog_status <> 'CATALOG_COMPLETE'
            or
            (
                master_generation is not null
                and master_verified_from <= window_from
            )
        )
);

create table ingestion_receipt_audit_state (
    source_name varchar(64) primary key,
    status varchar(16) not null,
    due_at timestamptz not null,
    attempt_token uuid,
    lease_expires_at timestamptz,
    archive_idempotency_key text,
    processing_fingerprint varchar(64),
    processing_attempt_count integer,
    processing_state_version bigint,
    logical_partition_key varchar(32),
    partition_state_version bigint,
    generation_id bigint,
    generation_uuid uuid,
    index_kind varchar(16),
    index_uuid varchar(128),
    total_attempt_count integer not null default 0,
    automatic_retries_used integer not null default 0,
    consecutive_retryable_failures integer not null default 0,
    automatic_retry_limit integer not null,
    retry_sequence bigint not null default 1,
    retry_not_before timestamptz,
    last_attempt_at timestamptz,
    failed_at timestamptz,
    last_error_code varchar(64),
    last_error_retryable boolean,
    last_exhausted_at timestamptz,
    last_exhausted_error_code varchar(64),
    last_audited_at timestamptz,
    last_outcome varchar(32),
    state_version bigint not null default 0,
    created_at timestamptz not null default clock_timestamp(),
    updated_at timestamptz not null default clock_timestamp(),
    constraint fk_ingestion_receipt_audit_source
        foreign key (source_name) references ingestion_source_state (source_name),
    constraint fk_ingestion_receipt_audit_processing
        foreign key (archive_idempotency_key)
        references ingestion_archive_processing (archive_idempotency_key),
    constraint fk_ingestion_receipt_audit_generation
        foreign key (generation_id, logical_partition_key, generation_uuid)
        references index_generations (id, partition_key, generation_uuid),
    constraint ck_ingestion_receipt_audit_status
        check (status in ('IDLE', 'AUDITING', 'FAILED')),
    constraint ck_ingestion_receipt_audit_attempts
        check (
            total_attempt_count >= 0
            and automatic_retries_used between 0 and automatic_retry_limit
            and automatic_retries_used <= greatest(total_attempt_count - 1, 0)
            and automatic_retry_limit between 0 and 100
            and consecutive_retryable_failures >= 0
            and consecutive_retryable_failures <= total_attempt_count
            and retry_sequence > 0
            and state_version >= 0
            and (
                (total_attempt_count = 0 and last_attempt_at is null)
                or
                (total_attempt_count > 0 and last_attempt_at is not null)
            )
        ),
    constraint ck_ingestion_receipt_audit_error_code
        check (
            last_error_code is null
            or last_error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
        ),
    constraint ck_ingestion_receipt_audit_failure
        check (
            (
                last_error_code is null
                and last_error_retryable is null
                and failed_at is null
                and retry_not_before is null
            )
            or
            (
                last_error_code is not null
                and last_error_retryable is not null
                and failed_at is not null
                and (
                    (last_error_retryable and retry_not_before is not null)
                    or
                    (not last_error_retryable and retry_not_before is null)
                )
            )
        ),
    constraint ck_ingestion_receipt_audit_exhausted_error_code
        check (
            last_exhausted_error_code is null
            or last_exhausted_error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'
        ),
    constraint ck_ingestion_receipt_audit_exhausted_evidence
        check (
            (last_exhausted_at is null and last_exhausted_error_code is null)
            or
            (last_exhausted_at is not null and last_exhausted_error_code is not null)
        ),
    constraint ck_ingestion_receipt_audit_binding
        check (
            (
                archive_idempotency_key is null
                and processing_fingerprint is null
                and processing_attempt_count is null
                and processing_state_version is null
                and logical_partition_key is null
                and partition_state_version is null
                and generation_id is null
                and generation_uuid is null
                and index_kind is null
                and index_uuid is null
            )
            or
            (
                archive_idempotency_key is not null
                and processing_fingerprint is not null
                and processing_fingerprint ~ '^[0-9a-f]{64}$'
                and processing_attempt_count is not null
                and processing_attempt_count > 0
                and processing_state_version is not null
                and processing_state_version >= 0
                and logical_partition_key is not null
                and partition_state_version is not null
                and partition_state_version >= 0
                and generation_id is not null
                and generation_uuid is not null
                and index_kind in ('EVENT', 'MENTION')
                and index_uuid is not null
                and btrim(index_uuid) <> ''
                and index_uuid !~ '[*?,[:space:]]'
            )
        ),
    constraint ck_ingestion_receipt_audit_ownership
        check (
            (
                status = 'IDLE'
                and attempt_token is null
                and lease_expires_at is null
                and archive_idempotency_key is null
                and last_error_code is null
            )
            or
            (
                status = 'AUDITING'
                and attempt_token is not null
                and lease_expires_at is not null
                and last_attempt_at is not null
                and lease_expires_at > last_attempt_at
                and archive_idempotency_key is not null
                and last_error_code is null
            )
            or
            (
                status = 'FAILED'
                and attempt_token is null
                and lease_expires_at is null
                and archive_idempotency_key is null
                and total_attempt_count > 0
                and last_error_code is not null
            )
        ),
    constraint ck_ingestion_receipt_audit_outcome
        check (
            last_outcome is null
            or last_outcome in (
                'MATCHED',
                'SHORTAGE',
                'SURPLUS',
                'IDENTITY_MISMATCH',
                'MISSING_GENERATION',
                'STALE_BINDING',
                'INFRASTRUCTURE_FAILURE'
            )
        )
);

create index ix_ingestion_cycle_state_lease
    on ingestion_cycle_state (lease_expires_at)
    where status = 'ACTIVE';

create index ix_ingestion_archives_recent_work
    on ingestion_archives (
        archive_type,
        status,
        source_update_time desc,
        idempotency_key
    )
    include (retry_not_before, lease_expires_at);

create index ix_ingestion_archive_processing_recent_work
    on ingestion_archive_processing (status, archive_idempotency_key)
    include (retry_not_before, lease_expires_at);

create index ix_ingestion_receipt_audit_due
    on ingestion_receipt_audit_state (status, due_at, retry_not_before, source_name)
    where status in ('IDLE', 'FAILED');

create index ix_ingestion_receipt_audit_lease
    on ingestion_receipt_audit_state (lease_expires_at)
    where status = 'AUDITING';
