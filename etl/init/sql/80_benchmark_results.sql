-- Create table for benchmark results
CREATE TABLE IF NOT EXISTS benchmark_results (
    id SERIAL PRIMARY KEY,
    test_run_id INTEGER NOT NULL,
    iteration INTEGER NOT NULL,
    execution_time_ms INTEGER NOT NULL
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    query_name VARCHAR(255) NOT NULL,
    explain JSONB NOT NULL,
    type TEXT NOT NULL,
);

CREATE SEQUENCE IF NOT EXISTS test_run_id_seq AS INTEGER
  MINVALUE 0
  START WITH 0
  OWNED BY benchmark_results.test_run_id; 