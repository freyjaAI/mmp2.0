# MMP 2.0 - Affordable Risk Analytics Platform

> **Production-grade risk intelligence system competing with enterprise risk analytics providers**

## 🎯 Mission

Build a **multi-source, entity-centric risk intelligence platform** that delivers the same depth of data as Thomson Reuters CLEAR and Dun & Bradstreet with significantly more competitive pricing.

## 🏗️ Architecture Overview

### Core Components

1. **Entity Resolution Engine** - Resolves identities across disparate datasets using probabilistic record linkage
2. **Risk Signal Engine** - Aggregates and scores risk signals from multiple sources
3. **Graph Database** - Neo4j for relationship mapping and identity clustering
4. **Relational Database** - PostgreSQL for structured entity data and audit logs
5. **API Layer** - FastAPI micro-service with <50ms response time
6. **Embeddable Widget** - React component for customer dashboards

### Data Layers

```
A. RAW DATA LAKE (S3/GCS)
   └─ One folder per source, parquet + JSON, never mutated

B. RECORD-LEVEL GRAPH (Neo4j)
   └─ Every row becomes a node with source+row_id
   └─ Hashed PII + select plaintext

C. ENTITY-LEVEL GRAPH (PostgreSQL)
   └─ One row = canonical entity (person or business)
   └─ Edges to every raw node claimed by that entity
```

## 📊 Risk Domains

- **Criminal & Legal Risk** - Arrests, warrants, court records
- **Financial Risk** - Bankruptcy, liens, UCC filings
- **Regulatory Risk** - OFAC, sanctions, PEPs
- **Business Risk** - Corporate filings, executive affiliations, FEIN linkage
- **Identity Risk** - SSN validation, address consistency, alias clustering

## 🗂️ Repository Structure

```
mmp2.0/
├── ddl/                    # Database schemas
│   ├── 01_core.sql        # PostgreSQL tables
│   └── 02_neo4j.cypher    # Neo4j constraints
├── jobs/                   # Data processing pipelines
│   ├── blocking_person.py # PySpark blocking job
│   └── scoring.py         # Pair scoring logic
├── api/                    # FastAPI service
│   └── main.py            # REST endpoints
├── widget/                 # React embeddable component
│   └── src/
│       └── EntityCard.jsx
├── docs/                   # Architecture documentation
└── scripts/               # Setup and utility scripts
```


## 🛠️ Tech Stack

| Layer | Tools |
|-------|-------|
| **Data Ingestion** | Airflow, Fivetran, custom scrapers |
| **Entity Resolution** | Senzing, custom ML, Apache Spark |
| **Storage** | PostgreSQL (structured), Neo4j (graph), Elasticsearch (search) |
| **Risk Engine** | Python (Pandas, scikit-learn), dbt for transforms |
| **API / UI** | FastAPI + React, or GraphQL + Next.js |
| **Compliance** | RBAC, immutable audit logs |

## 📚 Data Sources

### Free / Low-Cost Public Records

- **OFAC & Sanctions** - Treasury RSS (free)
- **Secretary of State** - All 50 state business filings
- **USPS APIs** - Address normalization (free)
- **Census Geocoder** - Free
- **Federal Bankruptcy** - PACER
- **UCC Filings** - Many state APIs available
- **Jail Rosters** - 300+ county sites
- **Court Calendars** - Public records
- **SAM.gov** - Federal contractor exclusion list (free)
- **Google Places / OSM** - Prison/commercial flags

## 🔐 Compliance Requirements

- **FCRA-compliant disclaimers**
- **GLBA-permitted use checks**
- **Audit logs** (who accessed what, when, why)
- **Opt-out / dispute process**
- **Role-based access control (RBAC)**

## 🧪 Local Development Setup

### Prerequisites

```bash
# Install dependencies
pip install pyspark fastapi[all] psycopg2-binary pandas

# Start PostgreSQL
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:15

# Start Neo4j
docker run -d -p 7474:7474 -p 7687:7687 neo4j:5
```

### Initialize Database

```bash
# Load PostgreSQL schema
psql -h localhost -U postgres < ddl/01_core.sql

# Load Neo4j constraints (in Neo4j browser)
cat ddl/02_neo4j.cypher
```

### Run Blocking Job

```bash
spark-submit --packages org.postgresql:postgresql:42.6.0 jobs/blocking_person.py
```

### Start API

```bash
export DSN="host=localhost dbname=riskdb user=postgres password=postgres"
uvicorn api.main:app --reload
```

API available at: `http://localhost:8000`

### Test API

```bash
curl http://localhost:8000/entity/{person_canon_id}
```

## 🎨 Embedding the Widget

```html
<div id="mmp-entity-card" data-canon-id="{entity_id}"></div>
<script type="module" src="https://your-cdn.com/entity-card.js"></script>
```

## 📈 Performance Targets

- **API Response Time**: <50ms
- **Blocking Performance**: 5M records/min
- **Entity Resolution**: High-volume person and business records
- **Uptime**: 99.9%

## 🤝 Contributing

This is an active development project following a weekly sprint model. Each week introduces new capabilities:

- **Week 1**: Core schema and API
- **Week 2**: Scoring engine and first data source
- **Week 3-4**: Additional data sources and ML models
- **Week 5+**: Production hardening and compliance


## 🚀 Week 4b: Lazy Enrichment System (On-Demand Data Fetching)

### What Changed
We've implemented a **lazy enrichment** system that only fetches data from external APIs when customers actually request it. This eliminates upfront batch processing costs and maximizes the value of our free API quotas.

### How It Works
1. **Customer hits `/clear/person/{id}`**
2. **System checks cache** (Redis) - if found, return instantly (5ms)
3. **If cache miss**, check DB for missing fields:
   - No phone/email? → Trigger A-Leads lookup (free quota: 60K/month)
   - No bankruptcy? → Trigger CourtListener lookup (free, unlimited)
   - Business? → Trigger Data Axle firmographics (free quota: 6K/month)
4. **Background enrichment** starts (non-blocking)
5. **Return base response** immediately (~180ms first hit)
6. **Next request** gets enriched cached data (~5ms)

### Free Data Sources
- **OFAC Sanctions**: Treasury.gov XML (free, updated daily)
- **Harris County TX Criminal**: Socrata API (free, 1K/day limit)
- **CourtListener Bankruptcy**: Free public API (unlimited)
- **A-Leads Contact Data**: 60,000 free look


## 📄 License

MIT License - See LICENSE file for details

## 🔗 Resources

- [Entity Resolution Best Practices](https://github.com/J535D165/recordlinkage)
- [FCRA Compliance Guide](https://www.ftc.gov/enforcement/statutes/fair-credit-reporting-act)
- [OFAC Sanctions Data](https://home.treasury.gov/policy-issues/financial-sanctions/)

---

**Built with curiosity and shipping code every week** 🚀
