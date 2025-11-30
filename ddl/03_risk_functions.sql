-- MMP 2.0 - Risk Scoring Functions
-- Week 3: Risk Intelligence System

-- =============================================================================
-- 1. RISK SIGNAL AGGREGATION FUNCTION
-- Bubbles up all risk signals for a person_canon_id
-- =============================================================================

CREATE OR REPLACE FUNCTION get_person_risk_signals(p_person_canon_id UUID)
RETURNS TABLE (
    signal_type TEXT,
    event_count BIGINT,
    latest_event_date DATE,
    earliest_event_date DATE,
    max_severity SMALLINT,
    avg_severity NUMERIC(4,2),
    sources TEXT[]
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        prs.signal_type,
        COUNT(*) as event_count,
        MAX(prs.event_date) as latest_event_date,
        MIN(prs.event_date) as earliest_event_date,
        MAX(prs.severity) as max_severity,
        ROUND(AVG(prs.severity)::numeric, 2) as avg_severity,
        ARRAY_AGG(DISTINCT prs.src_name) as sources
    FROM person_risk_signal prs
    WHERE prs.person_canon_id = p_person_canon_id
    GROUP BY prs.signal_type
    ORDER BY MAX(prs.severity) DESC, COUNT(*) DESC;
END;
$$ LANGUAGE plpgsql STABLE;

-- =============================================================================
-- 2. COMPOSITE RISK SCORER
-- Calculates weighted risk score (1-999)
-- Weights: criminal x3, OFAC x10, bankruptcy x2
-- =============================================================================

CREATE OR REPLACE FUNCTION calculate_person_risk_score(p_person_canon_id UUID)
RETURNS TABLE (
    overall_score INTEGER,
    risk_level TEXT,
    category_scores JSONB,
    risk_breakdown JSONB
) AS $$
DECLARE
    v_criminal_score INTEGER := 0;
    v_ofac_score INTEGER := 0;
    v_bankruptcy_score INTEGER := 0;
    v_liens_score INTEGER := 0;
    v_other_score INTEGER := 0;
    v_total_score INTEGER := 0;
    v_risk_level TEXT;
    v_category_scores JSONB;
    v_breakdown JSONB;
BEGIN
    -- Criminal risk (weight: 3x)
    SELECT COALESCE(SUM(severity * 3), 0)::INTEGER
    INTO v_criminal_score
    FROM person_risk_signal
    WHERE person_canon_id = p_person_canon_id
      AND signal_type IN ('ARREST', 'CONVICTION', 'WARRANT', 'SEX_OFFENDER');
    
    -- OFAC/Sanctions risk (weight: 10x) - highest priority
    SELECT COALESCE(SUM(severity * 10), 0)::INTEGER
    INTO v_ofac_score
    FROM person_risk_signal
    WHERE person_canon_id = p_person_canon_id
      AND signal_type IN ('OFAC', 'SANCTIONS', 'PEP', 'WATCHLIST');
    
    -- Bankruptcy risk (weight: 2x)
    SELECT COALESCE(SUM(severity * 2), 0)::INTEGER
    INTO v_bankruptcy_score
    FROM person_risk_signal
    WHERE person_canon_id = p_person_canon_id
      AND signal_type IN ('BANKRUPTCY', 'CHAPTER_7', 'CHAPTER_11', 'CHAPTER_13');
    
    -- Liens/Judgments (weight: 2x)
    SELECT COALESCE(SUM(severity * 2), 0)::INTEGER
    INTO v_liens_score
    FROM person_risk_signal
    WHERE person_canon_id = p_person_canon_id
      AND signal_type IN ('TAX_LIEN', 'JUDGMENT', 'UCC_FILING');
    
    -- Other signals (weight: 1x)
    SELECT COALESCE(SUM(severity), 0)::INTEGER
    INTO v_other_score
    FROM person_risk_signal
    WHERE person_canon_id = p_person_canon_id
      AND signal_type NOT IN (
          'ARREST', 'CONVICTION', 'WARRANT', 'SEX_OFFENDER',
          'OFAC', 'SANCTIONS', 'PEP', 'WATCHLIST',
          'BANKRUPTCY', 'CHAPTER_7', 'CHAPTER_11', 'CHAPTER_13',
          'TAX_LIEN', 'JUDGMENT', 'UCC_FILING'
      );
    
    -- Calculate total score (capped at 999)
    v_total_score := LEAST(
        v_criminal_score + v_ofac_score + v_bankruptcy_score + v_liens_score + v_other_score,
        999
    );
    
    -- Determine risk level
    v_risk_level := CASE
        WHEN v_total_score >= 800 THEN 'CRITICAL'
        WHEN v_total_score >= 600 THEN 'HIGH'
        WHEN v_total_score >= 400 THEN 'ELEVATED'
        WHEN v_total_score >= 200 THEN 'MODERATE'
        WHEN v_total_score >= 100 THEN 'LOW'
        WHEN v_total_score > 0 THEN 'MINIMAL'
        ELSE 'CLEAR'
    END;
    
    -- Build category scores JSON
    v_category_scores := jsonb_build_object(
        'criminal', v_criminal_score,
        'ofac_sanctions', v_ofac_score,
        'bankruptcy', v_bankruptcy_score,
        'liens_judgments', v_liens_score,
        'other', v_other_score
    );
    
    -- Build detailed breakdown
    v_breakdown := jsonb_build_object(
        'criminal', jsonb_build_object(
            'score', v_criminal_score,
            'weight', 3,
            'types', ARRAY['ARREST', 'CONVICTION', 'WARRANT', 'SEX_OFFENDER']
        ),
        'ofac_sanctions', jsonb_build_object(
            'score', v_ofac_score,
            'weight', 10,
            'types', ARRAY['OFAC', 'SANCTIONS', 'PEP', 'WATCHLIST']
        ),
        'bankruptcy', jsonb_build_object(
            'score', v_bankruptcy_score,
            'weight', 2,
            'types', ARRAY['BANKRUPTCY', 'CHAPTER_7', 'CHAPTER_11', 'CHAPTER_13']
        ),
        'liens_judgments', jsonb_build_object(
            'score', v_liens_score,
            'weight', 2,
            'types', ARRAY['TAX_LIEN', 'JUDGMENT', 'UCC_FILING']
        )
    );
    
    RETURN QUERY
    SELECT 
        v_total_score,
        v_risk_level,
        v_category_scores,
        v_breakdown;
END;
$$ LANGUAGE plpgsql STABLE;

-- =============================================================================
-- 3. RISK TIMELINE BUILDER
-- Returns chronological timeline of risk events
-- =============================================================================

CREATE OR REPLACE FUNCTION get_person_risk_timeline(
    p_person_canon_id UUID,
    p_limit INTEGER DEFAULT 50
)
RETURNS TABLE (
    event_date DATE,
    signal_type TEXT,
    severity SMALLINT,
    source_name TEXT,
    details JSONB
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        prs.event_date,
        prs.signal_type,
        prs.severity,
        prs.src_name as source_name,
        prs.raw_json as details
    FROM person_risk_signal prs
    WHERE prs.person_canon_id = p_person_canon_id
      AND prs.event_date IS NOT NULL
    ORDER BY prs.event_date DESC, prs.severity DESC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql STABLE;

-- =============================================================================
-- 4. FULL RISK REPORT BUILDER
-- Combines scoring, signals, and timeline into single report
-- =============================================================================

CREATE OR REPLACE FUNCTION get_person_risk_report(p_person_canon_id UUID)
RETURNS JSONB AS $$
DECLARE
    v_report JSONB;
    v_person RECORD;
    v_score RECORD;
    v_signals JSONB;
    v_timeline JSONB;
    v_relationships JSONB;
BEGIN
    -- Get person canonical info
    SELECT * INTO v_person
    FROM person_canon
    WHERE person_canon_id = p_person_canon_id;
    
    IF NOT FOUND THEN
        RETURN jsonb_build_object(
            'error', 'Person not found',
            'person_canon_id', p_person_canon_id
        );
    END IF;
    
    -- Get risk score
    SELECT * INTO v_score
    FROM calculate_person_risk_score(p_person_canon_id);
    
    -- Get signal aggregates
    SELECT jsonb_agg(
        jsonb_build_object(
            'signal_type', signal_type,
            'event_count', event_count,
            'latest_event', latest_event_date,
            'earliest_event', earliest_event_date,
            'max_severity', max_severity,
            'avg_severity', avg_severity,
            'sources', sources
        )
    ) INTO v_signals
    FROM get_person_risk_signals(p_person_canon_id);
    
    -- Get risk timeline (last 20 events)
    SELECT jsonb_agg(
        jsonb_build_object(
            'date', event_date,
            'type', signal_type,
            'severity', severity,
            'source', source_name,
            'details', details
        )
    ) INTO v_timeline
    FROM get_person_risk_timeline(p_person_canon_id, 20);
    
    -- Get business relationships
    SELECT jsonb_agg(
        jsonb_build_object(
            'business_canon_id', bc.business_canon_id,
            'business_name', bc.best_legal_name,
            'relationship_type', pbr.relationship_type,
            'start_date', pbr.start_date,
            'end_date', pbr.end_date
        )
    ) INTO v_relationships
    FROM person_business_rel pbr
    JOIN business_canon bc ON bc.business_canon_id = pbr.business_canon_id
    WHERE pbr.person_canon_id = p_person_canon_id;
    
    -- Build complete report
    v_report := jsonb_build_object(
        'person_canon_id', p_person_canon_id,
        'generated_at', NOW(),
        'person', jsonb_build_object(
            'name', v_person.best_name,
            'dob', v_person.best_dob,
            'address', v_person.best_address,
            'confidence', v_person.confidence_score,
            'flags', v_person.flags
        ),
        'risk_score', jsonb_build_object(
            'overall', v_score.overall_score,
            'level', v_score.risk_level,
            'categories', v_score.category_scores,
            'breakdown', v_score.risk_breakdown
        ),
        'signals', COALESCE(v_signals, '[]'::jsonb),
        'timeline', COALESCE(v_timeline, '[]'::jsonb),
        'business_relationships', COALESCE(v_relationships, '[]'::jsonb),
        'source_citations', jsonb_build_object(
            'total_sources', (
                SELECT COUNT(DISTINCT src_name)
                FROM person_risk_signal
                WHERE person_canon_id = p_person_canon_id
            )
        )
    );
    
    RETURN v_report;
END;
$$ LANGUAGE plpgsql STABLE;

-- =============================================================================
-- 5. HELPER FUNCTION: Update person flags based on risk signals
-- =============================================================================

CREATE OR REPLACE FUNCTION update_person_flags(p_person_canon_id UUID)
RETURNS VOID AS $$
DECLARE
    v_flags JSONB := '{}'::jsonb;
BEGIN
    -- Check for OFAC
    IF EXISTS (
        SELECT 1 FROM person_risk_signal
        WHERE person_canon_id = p_person_canon_id
          AND signal_type IN ('OFAC', 'SANCTIONS')
    ) THEN
        v_flags := v_flags || jsonb_build_object('ofac', true);
    END IF;
    
    -- Check for criminal
    IF EXISTS (
        SELECT 1 FROM person_risk_signal
        WHERE person_canon_id = p_person_canon_id
          AND signal_type IN ('ARREST', 'CONVICTION', 'WARRANT')
    ) THEN
        v_flags := v_flags || jsonb_build_object('criminal', true);
    END IF;
    
    -- Check for bankruptcy
    IF EXISTS (
        SELECT 1 FROM person_risk_signal
        WHERE person_canon_id = p_person_canon_id
          AND signal_type LIKE '%BANKRUPTCY%' OR signal_type LIKE 'CHAPTER_%'
    ) THEN
        v_flags := v_flags || jsonb_build_object('bankruptcy', true);
    END IF;
    
    -- Update person_canon record
    UPDATE person_canon
    SET flags = v_flags,
        updated_at = NOW()
    WHERE person_canon_id = p_person_canon_id;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- INDEXES for performance
-- =============================================================================

-- Already created in 01_core.sql but adding comments
COMMENT ON INDEX prs_canon_signal IS 'Critical for risk score calculations - filters by canon_id and signal_type';

-- Additional index for date-based queries
CREATE INDEX IF NOT EXISTS prs_date_severity ON person_risk_signal (person_canon_id, event_date DESC, severity DESC);

-- =============================================================================
-- COMMENTS
-- =============================================================================

COMMENT ON FUNCTION get_person_risk_signals IS 'Aggregates all risk signals for a person by type with statistics';
COMMENT ON FUNCTION calculate_person_risk_score IS 'Calculates weighted composite risk score (1-999) with category breakdown';
COMMENT ON FUNCTION get_person_risk_timeline IS 'Returns chronological timeline of risk events, most recent first';
COMMENT ON FUNCTION get_person_risk_report IS 'Main API function - returns complete risk intelligence report as JSONB';
COMMENT ON FUNCTION update_person_flags IS 'Trigger function to update flags in person_canon based on risk signals';
