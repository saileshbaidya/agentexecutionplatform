CREATE TABLE budget_accounts (
    run_id uuid PRIMARY KEY REFERENCES runs(id),
    limit_micro_usd bigint NOT NULL CHECK (limit_micro_usd >= 0),
    settled_micro_usd bigint NOT NULL DEFAULT 0 CHECK (settled_micro_usd >= 0),
    reserved_micro_usd bigint NOT NULL DEFAULT 0 CHECK (reserved_micro_usd >= 0),
    breached boolean NOT NULL DEFAULT false,
    CHECK (breached OR settled_micro_usd + reserved_micro_usd <= limit_micro_usd)
);
INSERT INTO budget_accounts(run_id,limit_micro_usd)
    SELECT id,(submission->>'budget_micro_usd')::bigint FROM runs;

CREATE TABLE operations (
    id uuid PRIMARY KEY,
    sequence bigserial UNIQUE NOT NULL,
    run_id uuid NOT NULL,
    task_id text NOT NULL,
    attempt_id uuid NOT NULL REFERENCES attempts(id),
    call_key text NOT NULL,
    request_hash char(64) NOT NULL,
    request jsonb NOT NULL,
    quote jsonb NOT NULL,
    state text NOT NULL CHECK (state IN ('RESERVED','DISPATCHED','UNKNOWN','SUCCEEDED','FAILED','CANCELLED')),
    dispatch_token uuid,
    result jsonb,
    error text,
    last_checked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (run_id,task_id) REFERENCES tasks(run_id,id),
    UNIQUE (run_id,task_id,call_key),
    UNIQUE (run_id,id)
);
CREATE TABLE budget_reservations (
    operation_id uuid PRIMARY KEY REFERENCES operations(id),
    run_id uuid NOT NULL REFERENCES budget_accounts(run_id),
    amount_micro_usd bigint NOT NULL CHECK (amount_micro_usd >= 0),
    state text NOT NULL CHECK (state IN ('HELD','SETTLED','RELEASED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    FOREIGN KEY (run_id,operation_id) REFERENCES operations(run_id,id)
);
CREATE TABLE usage_ledger (
    sequence bigserial PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES budget_accounts(run_id),
    operation_id uuid NOT NULL,
    entry_key text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('CHARGE','ADJUSTMENT')),
    amount_micro_usd bigint NOT NULL,
    reason text NOT NULL,
    evidence jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (run_id,operation_id) REFERENCES operations(run_id,id),
    UNIQUE(run_id,entry_key),
    CHECK (kind='ADJUSTMENT' OR amount_micro_usd >= 0)
);
CREATE FUNCTION immutable_usage_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'usage_ledger is append-only';
END;
$$;
CREATE TRIGGER immutable_usage_ledger BEFORE UPDATE OR DELETE ON usage_ledger
    FOR EACH ROW EXECUTE FUNCTION immutable_usage_ledger();
CREATE TRIGGER immutable_usage_ledger_truncate BEFORE TRUNCATE ON usage_ledger
    FOR EACH STATEMENT EXECUTE FUNCTION immutable_usage_ledger();

-- Durable fake-provider evidence survives worker/process/container restarts.
-- This is a test adapter's receipt store, not a claim about real provider APIs.
CREATE TABLE fake_provider_receipts (
    operation_id uuid PRIMARY KEY REFERENCES operations(id),
    request_hash char(64) NOT NULL,
    receipt jsonb NOT NULL,
    available_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE attempts DROP CONSTRAINT attempts_state_check;
ALTER TABLE attempts ADD CONSTRAINT attempts_state_check CHECK
    (state IN ('ACTIVE','RECONCILIATION','SUCCEEDED','FAILED','EXPIRED','CANCELLED'));
CREATE INDEX unsettled_operations ON operations(run_id,state)
    WHERE state IN ('RESERVED','DISPATCHED','UNKNOWN');
CREATE INDEX operation_history ON operations(run_id,sequence);
CREATE INDEX ledger_history ON usage_ledger(run_id,sequence);
