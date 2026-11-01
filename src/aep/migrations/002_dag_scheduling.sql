ALTER TABLE runs
    ADD COLUMN max_concurrency integer NOT NULL DEFAULT 4 CHECK (max_concurrency BETWEEN 1 AND 32),
    ADD COLUMN last_claimed_at timestamptz,
    ADD COLUMN cancel_requested_at timestamptz,
    ADD COLUMN cancel_requested_by text,
    ADD COLUMN cancel_reason text;

-- Snapshot only immutable inputs. Runtime lease credentials stay separate.
ALTER TABLE attempts ADD COLUMN input_snapshot jsonb NOT NULL DEFAULT '{}';

CREATE INDEX task_scheduling ON tasks(run_id, state);
CREATE INDEX fair_run_scheduling ON runs(last_claimed_at ASC NULLS FIRST, created_at, id)
    WHERE state='RUNNING' AND failure IS NULL;
