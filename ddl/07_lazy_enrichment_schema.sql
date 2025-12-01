-- Lazy enrichment supporting tables for complete CLEAR coverage

-- Person aliases (AKAs, maiden names, nicknames)
CREATE TABLE IF NOT EXISTS person_alias (
    person_alias_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    person_canon_id UUID REFERENCES person_canon ON DELETE CASCADE,
    alias_name TEXT NOT NULL,
    alias_type TEXT CHECK (alias_type IN ('aka', 'maiden', 'former', 'nickname')),
    first_reported DATE DEFAULT CURRENT_DATE,
    UNIQUE (person_canon_id, alias_name)
);
CREATE INDEX idx_person_alias_canon ON person_alias(person_canon_id);

-- Person contact enrichment (A-Leads, Data Axle)
CREATE TABLE IF NOT EXISTS person_contact (
    person_contact_id BIGSERIAL PRIMARY KEY,
    person_canon_id UUID REFERENCES person_canon ON DELETE CASCADE,
    src_name TEXT CHECK (src_name IN ('a_leads', 'data_axle', 'usps')),
    src_row_id TEXT,
    phone10 CHAR(10),
    email_local TEXT,
    address_id UUID REFERENCES address_raw,
    first_reported DATE DEFAULT CURRENT_DATE,
    UNIQUE (person_canon_id, src_name, src_row_id)
);
CREATE INDEX idx_person_contact_canon ON person_contact(person_canon_id);

-- Business firmographics (Data Axle)
CREATE TABLE IF NOT EXISTS business_risk_signal (
    business_risk_signal_id BIGSERIAL PRIMARY KEY,
    business_canon_id UUID REFERENCES business_canon ON DELETE CASCADE,
    signal_type TEXT CHECK (signal_type IN ('firmographics', 'lien', '314a', 'officer_change')),
    event_date DATE,
    severity INT CHECK (severity BETWEEN 1 AND 10),
    src_name TEXT,
    src_row_id TEXT,
    raw_json JSONB,
    ingested_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_business_risk_signal_canon ON business_risk_signal(business_canon_id, signal_type);

-- Person-to-Person relationships (associates)
CREATE TABLE IF NOT EXISTS person_person_rel (
    rel_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    person_canon_id_1 UUID REFERENCES person_canon ON DELETE CASCADE,
    person_canon_id_2 UUID REFERENCES person_canon ON DELETE CASCADE,
    rel_type TEXT CHECK (rel_type IN ('relative', 'associate', 'shared_address', 'shared_business', 'co_defendant')),
    strength INT CHECK (strength BETWEEN 1 AND 10),
    src_name TEXT,
    src_row_id TEXT,
    discovered_date DATE DEFAULT CURRENT_DATE,
    UNIQUE (person_canon_id_1, person_canon_id_2, rel_type)
);
CREATE INDEX idx_ppr_1 ON person_person_rel(person_canon_id_1);
CREATE INDEX idx_ppr_2 ON person_person_rel(person_canon_id_2);

-- API cost tracking (for free quota management)
CREATE TABLE IF NOT EXISTS api_cost_log (
    log_id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    lookups INT NOT NULL,
    cost_cents INT NOT NULL DEFAULT 0,
    log_date DATE DEFAULT CURRENT_DATE,
    logged_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX idx_cost_log_month ON api_cost_log(source, log_date);
