-- ========== PERSON ALIASES ==========
CREATE TABLE person_alias (
  person_alias_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  person_canon_id UUID REFERENCES person_canon ON DELETE CASCADE,
  alias_name TEXT NOT NULL,
  alias_type TEXT CHECK (alias_type IN ('aka','maiden','former','nickname')),
  first_reported DATE DEFAULT CURRENT_DATE,
  UNIQUE (person_canon_id, alias_name)
);
-- seed from person_raw AKAs during ingestion

-- ========== PERSON ADDRESS HISTORY ==========
CREATE TABLE person_address_link (
  person_address_link_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  person_canon_id UUID REFERENCES person_canon ON DELETE CASCADE,
  address_id      UUID REFERENCES address_raw ON DELETE CASCADE,
  reported_date   DATE,
  source          TEXT,
  is_current      BOOLEAN DEFAULT false
);
CREATE INDEX pal_person ON person_address_link(person_canon_id);
CREATE INDEX pal_addr   ON person_address_link(address_id);

-- ========== PERSON FLAGS (materialized YES/NO) ==========
CREATE MATERIALIZED VIEW person_flags AS
SELECT pc.person_canon_id,
  -- OFAC / sanctions
  EXISTS(SELECT 1 FROM person_risk_signal ps
         WHERE ps.person_canon_id = pc.person_canon_id
           AND ps.signal_type IN ('ofac','sanctions')) AS flag_ofac,
  -- Criminal
  EXISTS(SELECT 1 FROM person_risk_signal ps
         WHERE ps.person_canon_id = pc.person_canon_id
           AND ps.signal_type = 'criminal') AS flag_criminal,
  -- Bankruptcy
  EXISTS(SELECT 1 FROM person_risk_signal ps
         WHERE ps.person_canon_id = pc.person_canon_id
           AND ps.signal_type = 'bankruptcy') AS flag_bankruptcy,
  -- Sex offender (future source)
  false AS flag_sex_offender,
  -- Multiple SSN
  (SELECT COUNT(DISTINCT ssn4) FROM person_raw pr
   WHERE pr.person_canon_id = pc.person_canon_id AND pr.ssn4 IS NOT NULL) > 1 AS flag_multiple_ssn,
  -- Deceased
  EXISTS(SELECT 1 FROM person_risk_signal ps
         WHERE ps.person_canon_id = pc.person_canon_id
           AND ps.signal_type = 'deceased') AS flag_deceased,
  -- PO box address
  EXISTS(SELECT 1 FROM person_address_link pal
         JOIN address_raw a ON a.address_id = pal.address_id
         WHERE pal.person_canon_id = pc.person_canon_id
           AND a.po_box_flag = true) AS flag_po_box,
  -- Prison address
  EXISTS(SELECT 1 FROM person_address_link pal
         JOIN address_raw a ON a.address_id = pal.address_id
         WHERE pal.person_canon_id = pc.person_canon_id
           AND a.prison_flag = true) AS flag_prison_address,
  -- Younger than SSN issue (placeholder)
  false AS flag_younger_than_ssn
FROM person_canon pc;

CREATE UNIQUE INDEX pf_person ON person_flags(person_canon_id);

-- ========== PERSON↔PERSON RELATIONSHIPS ==========
CREATE TABLE person_person_rel (
  rel_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  person_canon_id_1 UUID REFERENCES person_canon ON DELETE CASCADE,
  person_canon_id_2 UUID REFERENCES person_canon ON DELETE CASCADE,
  rel_type TEXT CHECK (rel_type IN ('relative','associate','shared_address','shared_business')),
  strength INT CHECK (strength BETWEEN 1 AND 10),
  src_name TEXT,
  src_row_id TEXT,
  UNIQUE (person_canon_id_1, person_canon_id_2, rel_type)
);
CREATE INDEX ppr_1 ON person_person_rel(person_canon_id_1);
CREATE INDEX ppr_2 ON person_person_rel(person_canon_id_2);

-- ========== BUSINESS PHONE ==========
CREATE TABLE business_phone (
  business_phone_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  business_canon_id UUID REFERENCES business_canon ON DELETE CASCADE,
  phone10 CHAR(10),
  source TEXT,
  first_seen DATE DEFAULT CURRENT_DATE,
  UNIQUE (business_canon_id, phone10)
);

-- ========== BUSINESS ADDRESS HISTORY ==========
CREATE TABLE business_address_link (
  business_address_link_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  business_canon_id UUID REFERENCES business_canon ON DELETE CASCADE,
  address_id        UUID REFERENCES address_raw ON DELETE CASCADE,
  reported_date     DATE,
  source            TEXT,
  is_headquarters   BOOLEAN DEFAULT false
);
