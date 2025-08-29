CREATE TABLE IF NOT EXISTS partial_results (
  worker_id TEXT,
  chunk_id INTEGER,
  rows_processed INTEGER,
  total_sales REAL,
  price_min REAL,
  price_max REAL,
  price_sum REAL,
  inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (worker_id, chunk_id)
);
