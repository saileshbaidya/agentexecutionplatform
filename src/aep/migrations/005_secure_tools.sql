ALTER TABLE tasks DROP CONSTRAINT tasks_state_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_state_check CHECK
 (state IN ('BLOCKED','READY','RUNNING','RETRY_WAIT','TOOL_WAIT','SUCCEEDED','FAILED','CANCELLED'));
ALTER TABLE attempts DROP CONSTRAINT attempts_state_check;
ALTER TABLE attempts ADD CONSTRAINT attempts_state_check CHECK
 (state IN ('ACTIVE','RECONCILIATION','TOOL_WAIT','SUCCEEDED','FAILED','EXPIRED','CANCELLED'));

CREATE TABLE tool_actions (
 id uuid PRIMARY KEY,
 sequence bigserial UNIQUE NOT NULL,
 run_id uuid NOT NULL,
 task_id text NOT NULL,
 call_key text NOT NULL,
 action_digest char(64) NOT NULL,
 action jsonb NOT NULL,
 state text NOT NULL CHECK (state IN ('PENDING','READY','RUNNING','SUCCEEDED','FAILED','DENIED','EXPIRED','CANCELLED','UNKNOWN')),
 expires_at timestamptz NOT NULL,
 dispatched_at timestamptz,
 dispatch_token uuid,
 cleanup_required boolean NOT NULL DEFAULT false,
 result jsonb,
 error text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 finished_at timestamptz,
 FOREIGN KEY(run_id,task_id) REFERENCES tasks(run_id,id),
 UNIQUE(run_id,task_id,call_key),
 UNIQUE(run_id,id)
);
CREATE TABLE tool_approvals (
 action_id uuid PRIMARY KEY REFERENCES tool_actions(id),
 action_digest char(64) NOT NULL,
 decision text NOT NULL CHECK(decision IN ('approve','deny')),
 approver text NOT NULL,
 expires_at timestamptz NOT NULL,
 consumed_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX tool_dispatch ON tool_actions(state,created_at);
