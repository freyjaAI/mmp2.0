-- MMP 2.0 - PostgreSQL Core Schema
-- Entity Resolution and Risk Analytics Database

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm; -- for quick fuzzy search later

-- 1. RAW STAGING TABLES (load once, never update)
CREATE TABLE person_raw (
    person_raw_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    src_name TEXT NOT NULL,
    src_row_id TEXT NOT NULL,
    last_name_std TEXT NOT NULL,
    first_name_std TEXT NOT NULL,
    dob DATE,
    partial_dob DATE, -- yyyy-mm-01 if only month known
    ssn4 CHAR(4),
    address_id UUID, -- fk to address_raw
    phone10 CHAR(10),
    email_local TEXT,
    hash_blob CHAR(64) NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (src_name, src_row_id)
);

CREATE TABLE business_raw (
    business_raw_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    src_name TEXT NOT NULL,
    src_row_id TEXT NOT NULL,
    legal_name_std TEXT NOT NULL,
    fein CHAR(9),
    sos_id TEXT,
    state CHAR(2),
    address_id UUID,
    formation_date DATE,
    officer_names TEXT[], -- normalized
    hash_blob CHAR(64) NOT NULL,
    ingested_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (src_name, src_row_id)
);

CREATE TABLE address_raw (
    address_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    usps_std TEXT NOT NULL,
    zip5 CHAR(5),
    zip9 CHAR(9),
    dpbc CHAR(12),
    po_box_flag BOOLEAN NOT NULL DEFAULT false,
    prison_flag BOOLEAN NOT NULL DEFAULT false,
    commercial_flag BOOLEAN NOT NULL DEFAULT false,
    lat NUMERIC(9, 6),
    lon NUMERIC(9, 6),
    UNIQUE (usps_std)
);

-- 2. CANON TABLES
CREATE TABLE person_canon (
    person_canon_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    best_name TEXT,
    best_dob DATE,
    best_address TEXT,
    confidence_score NUMERIC(4, 3), -- 0-1
    flags JSONB, -- {"ofac":false,"criminal":true}
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE business_canon (
    business_canon_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    best_legal_name TEXT,
    best_fein CHAR(9),
    best_address TEXT,
    confidence_score NUMERIC(4, 3),
    flags JSONB,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- 3. ENTITY MAPPING (many raw -> one canon)
CREATE TABLE person_raw_canon (
    person_raw_id UUID UNIQUE REFERENCES person_raw ON DELETE CASCADE,
    person_canon_id UUID REFERENCES person_canon ON DELETE CASCADE,
    match_score NUMERIC(4, 3),
    PRIMARY KEY (person_raw_id, person_canon_id)
);

CREATE TABLE business_raw_canon (
    business_raw_id UUID UNIQUE REFERENCES business_raw ON DELETE CASCADE,
    business_canon_id UUID REFERENCES business_canon ON DELETE CASCADE,
    match_score NUMERIC(4, 3),
    PRIMARY KEY (business_raw_id, business_canon_id)
);

-- 4. RISK SIGNALS
CREATE TABLE person_risk_signal (
    id BIGSERIAL PRIMARY KEY,
    person_canon_id UUID REFERENCES person_canon ON DELETE CASCADE,
    signal_type TEXT, -- enum later
    event_date DATE,
    severity SMALLINT CHECK (severity BETWEEN 1 AND 10),
    src_name TEXT,
    src_row_id TEXT,
    raw_json JSONB,
    ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX prs_canon_signal ON person_risk_signal (person_canon_id, signal_type);

CREATE TABLE business_risk_signal (
    id BIGSERIAL PRIMARY KEY,
    business_canon_id UUID REFERENCES business_canon ON DELETE CASCADE,
    signal_type TEXT,
    event_date DATE,
    severity SMALLINT CHECK (severity BETWEEN 1 AND 10),
    src_name TEXT,
    src_row_id TEXT,
    raw_json JSONB,
    ingested_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX brs_canon_signal ON business_risk_signal (business_canon_id, signal_type);

-- 5. BLOCKING TABLES (for entity resolution)
CREATE TABLE person_block (
    person_raw_id UUID REFERENCES person_raw ON DELETE CASCADE,
    block_type TEXT NOT NULL,
    block_key TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (person_raw_id, block_type, block_key)
);

CREATE INDEX person_block_key ON person_block (block_key);

CREATE TABLE business_block (
    business_raw_id UUID REFERENCES business_raw ON DELETE CASCADE,
    block_type TEXT NOT NULL,
    block_key TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (business_raw_id, block_type, block_key)
);

CREATE INDEX business_block_key ON business_block (block_key);

-- 6. AUDIT LOG
CREATE TABLE audit_log (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    entity_type TEXT NOT NULL, -- person or business
    entity_id UUID NOT NULL,
    action TEXT NOT NULL, -- view, export, etc
    timestamp TIMESTAMPTZ DEFAULT now(),
    ip_address INET,
    metadata JSONB
);

CREATE INDEX audit_log_entity ON audit_log (entity_type, entity_id);
CREATE INDEX audit_log_user ON audit_log (user_id, timestamp);

-- 7. CROSS-ENTITY RELATIONSHIPS
CREATE TABLE person_business_rel (
    id BIGSERIAL PRIMARY KEY,
    person_canon_id UUID REFERENCES person_canon ON DELETE CASCADE,
    business_canon_id UUID REFERENCES business_canon ON DELETE CASCADE,
    relationship_type TEXT NOT NULL, -- OFFICER_OF, REGISTERED_AGENT_FOR, DEBTOR_ON_UCC, etc
    start_date DATE,
    end_date DATE,
    source_name TEXT,
    source_id TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE (person_canon_id, business_canon_id, relationship_type, source_name, source_id)
);

CREATE INDEX pbr_person ON person_business_rel (person_canon_id);
CREATE INDEX pbr_business ON person_business_rel (business_canon_id);

-- Comments
COMMENT ON TABLE person_raw IS 'Raw person records from various sources, never mutated';
COMMENT ON TABLE business_raw IS 'Raw business records from various sources, never mutated';
COMMENT ON TABLE address_raw IS 'Normalized address lookup table';
COMMENT ON TABLE person_canon IS 'Canonical person entities after entity resolution';
COMMENT ON TABLE business_canon IS 'Canonical business entities after entity resolution';
COMMENT ON TABLE person_risk_signal IS 'Append-only risk signals for persons';
COMMENT ON TABLE business_risk_signal IS 'Append-only risk signals for businesses';
COMMENT ON TABLE audit_log IS 'FCRA/GLBA compliant access audit trail';
COMMENT ON TABLE person_business_rel IS 'Relationships between persons and businesses (officers, agents, etc)';
