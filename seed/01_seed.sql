-- Test persons (Paul Spencer and Michael Dandrea)
INSERT INTO person_canon (person_canon_id, best_name, best_dob, confidence_score, flags)
VALUES
('0193c5d6-1234-1234-1234-123456789012'::uuid, 'SPENCER, PAUL EUGENE', '1972-02-01', 0.95, '{}'),
('02aaaaaa-1234-1234-1234-123456789012'::uuid, 'DANDREA, MICHAEL', '1966-03-15', 0.90, '{}');

-- Raw person records
INSERT INTO person_raw (person_raw_id, src_name, src_row_id, last_name_std, first_name_std, dob, hash_blob)
VALUES
(gen_random_uuid(), 'test_seed', 'SP001', 'SPENCER', 'PAUL', '1972-02-01', 'hash1'),
(gen_random_uuid(), 'test_seed', 'DA001', 'DANDREA', 'MICHAEL', '1966-03-15', 'hash2');

-- Canon mapping
INSERT INTO person_raw_canon (person_raw_id, person_canon_id, match_score)
SELECT person_raw_id, '0193c5d6-1234-1234-1234-123456789012'::uuid, 0.95
FROM person_raw WHERE src_row_id = 'SP001'
UNION ALL
SELECT person_raw_id, '02aaaaaa-1234-1234-1234-123456789012'::uuid, 0.90
FROM person_raw WHERE src_row_id = 'DA001';

-- Refresh materialized view
REFRESH MATERIALIZED VIEW person_flags;

-- Risk signals (criminal + OFAC)
INSERT INTO person_risk_signal (person_canon_id, signal_type, event_date, severity, src_name, src_row_id, raw_json)
VALUES
('0193c5d6-1234-1234-1234-123456789012'::uuid, 'criminal', '1997-04-23', 7, 'nc_district_court', '1997CR116288', '{"charge":"Larceny","disposition":"CASE DISPOSED","court":"DISTRICT COURT NC"}'),
('0193c5d6-1234-1234-1234-123456789012'::uuid, 'ofac', '2024-01-15', 10, 'treasury_ofac', '12345', '{}');

-- Test business
INSERT INTO business_canon (business_canon_id, best_legal_name, best_fein, best_address, confidence_score, flags)
VALUES ('03bbbbbb-1234-1234-1234-123456789012'::uuid, 'ZOOKS ENTERPRISES CORP', '000000000', '821 CANDLEWOOD LAKE RD S, NEW MILFORD, CT', 0.95, '{}');
