CREATE TABLE IF NOT EXISTS transactions (
  id BIGSERIAL PRIMARY KEY,
  occurred_at TIMESTAMPTZ NOT NULL,
  category VARCHAR(80) NOT NULL,
  amount NUMERIC(18,2) NOT NULL,
  source VARCHAR(120)
);
CREATE INDEX IF NOT EXISTS idx_transactions_occurred_at_id ON transactions (occurred_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_category_occurred_at ON transactions (category, occurred_at);
