-- Create tables for FinTech pipeline

CREATE TABLE IF NOT EXISTS raw_transactions (
  transaction_id      TEXT PRIMARY KEY,
  user_id             TEXT NOT NULL,
  event_time          TIMESTAMPTZ NOT NULL,
  merchant_category   TEXT NOT NULL,
  amount              NUMERIC(12,2) NOT NULL,
  country             TEXT NOT NULL,
  city                TEXT NOT NULL,
  ingested_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS validated_transactions (
  transaction_id      TEXT PRIMARY KEY,
  user_id             TEXT NOT NULL,
  event_time          TIMESTAMPTZ NOT NULL,
  merchant_category   TEXT NOT NULL,
  amount              NUMERIC(12,2) NOT NULL,
  country             TEXT NOT NULL,
  city                TEXT NOT NULL,
  validated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
  alert_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  transaction_id      TEXT NOT NULL,
  user_id             TEXT NOT NULL,
  event_time          TIMESTAMPTZ NOT NULL,
  fraud_type          TEXT NOT NULL, -- HIGH_AMOUNT or IMPOSSIBLE_TRAVEL
  reason              TEXT NOT NULL,
  amount              NUMERIC(12,2) NOT NULL,
  country             TEXT NOT NULL,
  city                TEXT NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_raw_user_time ON raw_transactions (user_id, event_time);
CREATE INDEX IF NOT EXISTS idx_valid_user_time ON validated_transactions (user_id, event_time);
CREATE INDEX IF NOT EXISTS idx_fraud_user_time ON fraud_alerts (user_id, event_time);
CREATE INDEX IF NOT EXISTS idx_fraud_category ON fraud_alerts (fraud_type);