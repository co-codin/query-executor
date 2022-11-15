CREATE TABLE IF NOT EXISTS queries (
    id BIGSERIAL PRIMARY KEY NOT NULL,
    guid VARCHAR(36) UNIQUE NOT NULL,
    db VARCHAR(36) NOT NULL,
    query TEXT NOT NULL,
    status VARCHAR(64) NOT NULL,
    error_description TEXT,

    results_file TEXT,
    results_table TEXT,

    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS results (
    id BIGSERIAL PRIMARY KEY NOT NULL,
    query_id BIGSERIAL,
    dest_type VARCHAR(36) NOT NULL,
    path TEXT
);

CREATE INDEX IF NOT EXISTS idx_queries_guid ON queries (guid);
