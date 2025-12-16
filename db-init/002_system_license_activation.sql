-- Add SM2 license activation support columns
ALTER TABLE IF EXISTS system_licenses
  ADD COLUMN IF NOT EXISTS license_key TEXT,
  ADD COLUMN IF NOT EXISTS license_payload JSONB,
  ADD COLUMN IF NOT EXISTS activated_at TIMESTAMP;

-- Ensure seed row still exists
INSERT INTO system_licenses (id, status, created_at, updated_at)
VALUES (1, 'DRAFT', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
ON CONFLICT (id) DO NOTHING;
