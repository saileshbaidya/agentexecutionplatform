ALTER TABLE runs ADD COLUMN agent_profile jsonb;
ALTER TABLE operations ADD COLUMN provider_response_id text;
CREATE UNIQUE INDEX unique_provider_response ON operations((request->>'provider'),provider_response_id)
    WHERE provider_response_id IS NOT NULL;

CREATE TABLE source_documents (
    run_id uuid NOT NULL REFERENCES runs(id),
    document_id text NOT NULL,
    artifact_id uuid NOT NULL UNIQUE,
    title text NOT NULL,
    content text NOT NULL,
    sha256 char(64) NOT NULL,
    PRIMARY KEY(run_id,document_id)
);
CREATE TABLE model_receipts (
    operation_id uuid PRIMARY KEY REFERENCES operations(id),
    request_hash char(64) NOT NULL,
    receipt jsonb NOT NULL,
    raw_response jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
