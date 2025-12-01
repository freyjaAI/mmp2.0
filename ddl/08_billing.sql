CREATE TABLE api_keys (
    api_key UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stripe_customer_id TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);
