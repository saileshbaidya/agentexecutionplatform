ALTER TABLE runs ADD COLUMN execution_pool text NOT NULL DEFAULT 'default';
ALTER TABLE runs ADD COLUMN payload_purged_at timestamptz;
CREATE INDEX execution_pool_claims ON runs(execution_pool,state,last_claimed_at);

CREATE TABLE context_snapshots (
 id uuid PRIMARY KEY,
 run_id uuid NOT NULL,
 task_id text NOT NULL,
 recipe_version text NOT NULL,
 input_digest char(64) NOT NULL,
 context_digest char(64) NOT NULL,
 manifest_digest char(64) NOT NULL,
 context jsonb,
 manifest jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 FOREIGN KEY(run_id,task_id) REFERENCES tasks(run_id,id),
 UNIQUE(run_id,task_id)
);
CREATE TABLE retention_jobs (
 run_id uuid PRIMARY KEY REFERENCES runs(id),
 storage_keys jsonb NOT NULL,
 state text NOT NULL CHECK(state IN ('PENDING','DONE')),
 error text,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 finished_at timestamptz
);
CREATE TABLE evaluation_batches (
 id uuid PRIMARY KEY,
 owner_id text NOT NULL,
 evaluation_key text NOT NULL,
 dataset_digest char(64) NOT NULL,
 dataset jsonb NOT NULL,
 configuration jsonb NOT NULL,
 label text NOT NULL,
 evaluator_version text NOT NULL,
 state text NOT NULL CHECK(state IN ('RUNNING','SUCCEEDED')),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 finished_at timestamptz,
 UNIQUE(owner_id,evaluation_key)
);
CREATE TABLE evaluation_cases (
 batch_id uuid NOT NULL REFERENCES evaluation_batches(id),
 case_id text NOT NULL,
 run_id uuid REFERENCES runs(id),
 injection_done boolean NOT NULL DEFAULT false,
 metrics jsonb,
 PRIMARY KEY(batch_id,case_id)
);
