CREATE TABLE runs (
    id uuid PRIMARY KEY,
    owner_id text NOT NULL,
    idempotency_key text NOT NULL,
    input_hash char(64) NOT NULL,
    submission jsonb NOT NULL,
    state text NOT NULL CHECK (state IN ('SUBMITTED','PLANNING','RUNNING','CANCELLING','SUCCEEDED','FAILED','CANCELLED')),
    plan jsonb,
    failure text,
    final_artifact_id uuid,
    event_sequence bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    deadline_at timestamptz NOT NULL,
    UNIQUE (owner_id, idempotency_key)
);
CREATE TABLE tasks (
    run_id uuid NOT NULL REFERENCES runs(id),
    id text NOT NULL,
    spec jsonb NOT NULL,
    state text NOT NULL CHECK (state IN ('BLOCKED','READY','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED','CANCELLED')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    fencing_token bigint NOT NULL DEFAULT 0,
    first_started_at timestamptz,
    next_run_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    result jsonb,
    PRIMARY KEY (run_id, id)
);
CREATE TABLE dependencies (
    run_id uuid NOT NULL,
    task_id text NOT NULL,
    dependency_id text NOT NULL,
    PRIMARY KEY (run_id, task_id, dependency_id),
    FOREIGN KEY (run_id, task_id) REFERENCES tasks(run_id, id),
    FOREIGN KEY (run_id, dependency_id) REFERENCES tasks(run_id, id),
    CHECK (task_id <> dependency_id)
);
CREATE TABLE attempts (
    id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    task_id text NOT NULL,
    number integer NOT NULL CHECK (number > 0),
    fencing_token bigint NOT NULL,
    worker_id text NOT NULL,
    state text NOT NULL CHECK (state IN ('ACTIVE','SUCCEEDED','FAILED','EXPIRED','CANCELLED')),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_expires_at timestamptz NOT NULL,
    finished_at timestamptz,
    error text,
    FOREIGN KEY (run_id, task_id) REFERENCES tasks(run_id, id),
    UNIQUE (run_id, task_id, number)
);
CREATE UNIQUE INDEX one_active_attempt ON attempts(run_id, task_id) WHERE state = 'ACTIVE';
CREATE INDEX expired_attempts ON attempts(lease_expires_at) WHERE state = 'ACTIVE';
CREATE INDEX schedulable_runs ON runs(created_at) WHERE state IN ('SUBMITTED','RUNNING','CANCELLING');
CREATE TABLE artifacts (
    id uuid PRIMARY KEY,
    run_id uuid NOT NULL,
    task_id text NOT NULL,
    attempt_id uuid NOT NULL REFERENCES attempts(id),
    sha256 char(64) NOT NULL,
    storage_key text NOT NULL,
    media_type text NOT NULL,
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (run_id, task_id) REFERENCES tasks(run_id, id),
    UNIQUE (run_id, id)
);
ALTER TABLE runs ADD CONSTRAINT final_artifact_fk FOREIGN KEY (id, final_artifact_id) REFERENCES artifacts(run_id, id);
CREATE TABLE events (
    run_id uuid NOT NULL REFERENCES runs(id),
    sequence bigint NOT NULL,
    kind text NOT NULL,
    actor text NOT NULL,
    task_id text,
    attempt_id uuid,
    data jsonb NOT NULL DEFAULT '{}',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, sequence)
);
