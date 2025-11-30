// MMP 2.0 - Neo4j Graph Database Constraints
// Entity Resolution Graph Schema
// Run these in Neo4j Browser after creating the database

// Node Constraints - Ensure Uniqueness
CREATE CONSTRAINT person_raw_uuid IF NOT EXISTS
FOR (p:PERSON_RAW)
REQUIRE p.person_raw_id IS UNIQUE;

CREATE CONSTRAINT business_raw_uuid IF NOT EXISTS
FOR (b:BUSINESS_RAW)
REQUIRE b.business_raw_id IS UNIQUE;

CREATE CONSTRAINT address_uuid IF NOT EXISTS
FOR (a:ADDRESS)
REQUIRE a.address_id IS UNIQUE;

CREATE CONSTRAINT person_canon_uuid IF NOT EXISTS
FOR (p:PERSON_CANON)
REQUIRE p.person_canon_id IS UNIQUE;

CREATE CONSTRAINT business_canon_uuid IF NOT EXISTS
FOR (b:BUSINESS_CANON)
REQUIRE b.business_canon_id IS UNIQUE;

// Indexes for Common Query Patterns
CREATE INDEX person_raw_blocking IF NOT EXISTS
FOR (p:PERSON_RAW)
ON (p.last_name_std, p.dob);

CREATE INDEX business_raw_blocking IF NOT EXISTS
FOR (b:BUSINESS_RAW)
ON (b.legal_name_std, b.fein);

CREATE INDEX address_lookup IF NOT EXISTS
FOR (a:ADDRESS)
ON (a.usps_std);

// Relationship types (for documentation)
// Used in the graph:
// (:PERSON_RAW)-[:BLOCKS_WITH]->(:PERSON_RAW)     - Blocking candidates
// (:BUSINESS_RAW)-[:BLOCKS_WITH]->(:BUSINESS_RAW) - Blocking candidates
// (:PERSON_RAW)-[:SAME_ENTITY]->(:PERSON_RAW)     - Matched entities
// (:BUSINESS_RAW)-[:SAME_ENTITY]->(:BUSINESS_RAW) - Matched entities
// (:PERSON_RAW)-[:LIVES_AT]->(:ADDRESS)           - Person to address
// (:BUSINESS_RAW)-[:LOCATED_AT]->(:ADDRESS)       - Business to address
// (:PERSON_CANON)-[:OFFICER_OF]->(:BUSINESS_CANON) - Person-business relationships
// (:PERSON_CANON)-[:REGISTERED_AGENT_FOR]->(:BUSINESS_CANON)
// (:PERSON_CANON)-[:DEBTOR_ON_UCC]->(:BUSINESS_CANON)

// Example: Adding a sample node (for testing)
// CREATE (p:PERSON_RAW {
//   person_raw_id: apoc.create.uuid(),
//   src_name: "test_source",
//   src_row_id: "001",
//   last_name_std: "SMITH",
//   first_name_std: "JOHN",
//   dob: date("1980-01-15")
// });

// Query patterns for entity resolution:
// 1. Find blocking candidates:
// MATCH (p1:PERSON_RAW)-[:BLOCKS_WITH]-(p2:PERSON_RAW)
// WHERE p1.person_raw_id < p2.person_raw_id
// RETURN p1, p2

// 2. Find connected components (canonical entities):
// CALL gds.wcc.stream('person-graph')
// YIELD nodeId, componentId
// RETURN gds.util.asNode(nodeId).person_raw_id AS person_raw_id,
//        componentId AS person_canon_id

// 3. Traverse person-business relationships:
// MATCH (p:PERSON_CANON)-[r:OFFICER_OF]->(b:BUSINESS_CANON)
// RETURN p.best_name, type(r), b.best_legal_name
