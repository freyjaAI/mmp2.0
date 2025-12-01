from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Optional
from datetime import  date
import psycopg2, os
from api.enrich import trigger_enrichments_async
from api.cache import cache_ttl
import redis
from api.ml_score import compute_risk_scores


router = APIRouter(prefix="/clear", tags=["clear-clone"])

# ---------- response models ----------

class SubjectOut(BaseModel):
    person_canon_id: str
    best_name: str
    best_dob: Optional[date] = None
    gender: Optional[str] = None
    entity_id: str

class AliasOut(BaseModel):
    alias_name: str
    alias_type: str

class AddressOut(BaseModel):
    usps_std: str
    zip5: str
    reported_date: Optional[str] = None
    source: Optional[str] = None
    po_box_flag: bool
    prison_flag: bool

class FlagsOut(BaseModel):
    ofac: bool
    criminal: bool
    bankruptcy: bool
    sex_offender: bool
    multiple_ssn: bool
    deceased: bool
    po_box: bool
    prison_address: bool
    younger_than_ssn: bool

class RiskEventOut(BaseModel):
    date: str
    type: str
    severity: int
    description: str
    case_number: Optional[str] = None
    court: Optional[str] = None
    disposition: Optional[str] = None
    source: str
    src_row_id: str

class AssociateOut(BaseModel):
    person_canon_id: str
    name: str
    relationship: str
    strength: int

class PersonReportOut(BaseModel):
    subject: SubjectOut
    aliases: List[AliasOut]
    addresses: List[AddressOut]
    flags: FlagsOut
    criminal_records: List[RiskEventOut]
    associates: List[AssociateOut]
    risk_scores: Optional[Dict] = None
    real_time: Optional[Dict] = None
    timeline: Optional[List[Dict]] = None
    network: Optional[Dict] = None
    visuals: Optional[Dict] = None
# ---------- helper ----------
DB_DSN = os.getenv("DB_DSN", "host=localhost dbname=riskdb user=postgres password=postgres")

# ---------- main report ----------

@cache_ttl()
@router.get("/person/{person_canon_id}", response_model=PersonReportOut)
def person_clear_report(person_canon_id: str):
    with psycopg2.connect(DB_DSN) as conn:
        cur = conn.cursor()
        
        # 1. subject
        cur.execute("""
            SELECT person_canon_id, best_name, best_dob, 'M' as gender,
                   person_canon_id::text as entity_id
            FROM person_canon WHERE person_canon_id = %s
        """, (person_canon_id,))
        subj = cur.fetchone()
        if not subj:
            raise HTTPException(status_code=404, detail="Canon ID not found")
        
        # 2. aliases
        cur.execute("""
            SELECT alias_name, alias_type FROM person_alias
            WHERE person_canon_id = %s
        """, (person_canon_id,))
        aliases = [AliasOut(alias_name=r[0], alias_type=r[1]) for r in cur.fetchall()]
        
        # 3. addresses
        cur.execute("""
            SELECT a.usps_std, a.zip5, pal.reported_date::text, pal.source,
                   a.po_box_flag, a.prison_flag
            FROM person_address_link pal
            JOIN address_raw a ON a.address_id = pal.address_id
            WHERE pal.person_canon_id = %s
            ORDER BY pal.reported_date DESC NULLS LAST
        """, (person_canon_id,))
        addresses = [AddressOut(usps_std=r[0], zip5=r[1], reported_date=r[2],
                                source=r[3], po_box_flag=r[4], prison_flag=r[5])
                     for r in cur.fetchall()]
        
        # 4. flags
        cur.execute("SELECT * FROM person_flags WHERE person_canon_id = %s", (person_canon_id,))
        flags_row = cur.fetchone()
        flags = FlagsOut(
            ofac=flags_row[1], criminal=flags_row[2], bankruptcy=flags_row[3],
            sex_offender=flags_row[4], multiple_ssn=flags_row[5], deceased=flags_row[6],
            po_box=flags_row[7], prison_address=flags_row[8], younger_than_ssn=flags_row[9]
        )
        
        # 5. criminal records (enriched risk_signal)
        cur.execute("""
            SELECT event_date::text, signal_type, severity,
                   raw_json->>'charge' AS description,
                   raw_json->>'case_number' AS case_number,
                   raw_json->>'court' AS court,
                   raw_json->>'disposition' AS disposition,
                   src_name, src_row_id
            FROM person_risk_signal
            WHERE person_canon_id = %s AND signal_type = 'criminal'
            ORDER BY event_date DESC
        """, (person_canon_id,))
        crimes = [RiskEventOut(date=r[0], type=r[1], severity=r[2], description=r[3] or '',
                               case_number=r[4], court=r[5], disposition=r[6],
                               source=r[7], src_row_id=r[8]) for r in cur.fetchall()]
        
        # 6. associates
        cur.execute("""
            SELECT p2.person_canon_id, p2.best_name, ppr.rel_type, ppr.strength
            FROM person_person_rel ppr
            JOIN person_canon p2 ON p2.person_canon_id = ppr.person_canon_id_2
            WHERE ppr.person_canon_id_1 = %s
            UNION
            SELECT p1.person_canon_id, p1.best_name, ppr.rel_type, ppr.strength
            FROM person_person_rel ppr
            JOIN person_canon p1 ON p1.person_canon_id = ppr.person_canon_id_1
            WHERE ppr.person_canon_id_2 = %s
        """, (person_canon_id, person_canon_id))
        associates = [AssociateOut(person_canon_id=r[0], name=r[1],
                                   relationship=r[2], strength=r[3]) for r in cur.fetchall()]
        

    # 7. compute risk scores
    person_data = {"name": subj[1], "dob": subj[2], "addresses": addresses, "criminal_records": crimes, "bankruptcy": flags.bankruptcy}    
risk_scores_dict = compute_risk_scores(person_data)

    # 8. check real-time jail status
    
try:    REDIS_URL = os.getenv("REDIS_URL")
        if REDIS_URL:
            r = redis.from_url(REDIS_URL)
            jail_key = f"jail:harris:{subj[1].lower().replace(' ', '_')}"
            jail_data = r.get(jail_key)
            if jail_data:
                real_time_dict = {"in_custody_now": True, "facility": "Harris County Jail", "source": "15-min polling"}
    except:
        pass
        return PersonReportOut(
            subject=SubjectOut(person_canon_id=subj[0], best_name=subj[1],
                             best_dob=subj[2], gender=subj[3], entity_id=subj[4]),
            aliases=aliases,
            addresses=addresses,
            flags=flags,
            criminal_records=crimes,
            associates=associates,
        risk_scores=risk_scores_dict,
        real_time=real_time_dict,
        timeline=None,
        network=None,
        visuals=None
        )

# ---------- BUSINESS CLONE ENDPOINT ----------

class BizSubjectOut(BaseModel):
    business_canon_id: str
    legal_name: str
    duns_id: Optional[str] = None
    address: str
    zip5: str

class BizFlagOut(BaseModel):
    ofac: bool
    bankruptcy: bool
    po_box: bool
    prison_address: bool

class OfficerOut(BaseModel):
    person_canon_id: str
    name: str
    title: str

class BizReportOut(BaseModel):
    subject: BizSubjectOut
    flags: BizFlagOut
    phones: List[str]
    officers: List[OfficerOut]
    same_address_businesses: List[str]
    same_address_people: List[str]

@cache_ttl()
@router.get("/business/{business_canon_id}", response_model=BizReportOut)
def business_clear_report(business_canon_id: str):
    with psycopg2.connect(DB_DSN) as conn:
        cur = conn.cursor()

        # stub – wire same pattern as person
        return BizReportOut(
            subject=BizSubjectOut(business_canon_id=business_canon_id,
                                legal_name="Zooks Enterprises",
                                duns_id="13-732-1268",
                                address="821 CANDLEWOOD LAKE RD S",
                                zip5="06776"),
            flags=BizFlagOut(ofac=False, bankruptcy=False, po_box=False, prison_address=False),
            phones=["8603545738"],
            officers=[OfficerOut(person_canon_id="dummy", name="Mike Dandrea", title="Officer")],
            same_address_businesses=["Kadco Medical LLC"],
            same_address_people=["Michael Dandrea", "Sandra Dandrea"]
        )
