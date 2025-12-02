#!/usr/bin/env python3
"""
CLEAR-clone response builder – nation-wide risk intelligence.
Returns JSON matching Thomson Reuters CLEAR layout.
"""
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Dict, Optional
import psycopg2, os, datetime
from api.billing import get_api_key
from api.enrich import trigger_enrichments_async  # lazy enrichment
from api.cache import get_redis  # Redis cache
from api.enrich_relatives import enrich_relatives_deep
# Pydantic models
class SubjectOut(BaseModel):
            person_canon_id: str
            best_name: str
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

class LicenseOut(BaseModel):
    license_type: str
    status: str
    expiry_date: Optional[str] = None
    state: str
    violations: int
    source: str

class RelativeOut(BaseModel):
    relationship: str
    name: str
    age: Optional[int] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    source: str

class EducationOut(BaseModel):
    school: str
    degree: str
    major: Optional[str] = None
    grad_year: Optional[str] = None
    state: str
    source: str

class EmploymentOut(BaseModel):
    job_title: str
    employer: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    industry: Optional[str] = None
    source: str

class AircraftOut(BaseModel):
    n_number: str
    model: str
    year: int
    reg_date: Optional[str] = None
    state: str
    source: str

class BoatOut(BaseModel):
    hull_id: str
    vessel_name: str
    year: int
    reg_date: Optional[str] = None
    state: str
    source: str

class SocialDeepOut(BaseModel):
    twitter_handle: Optional[str] = None
    linkedin_url: Optional[str] = None
    instagram_handle: Optional[str] = None
    facebook_url: Optional[str] = None
    tiktok_handle: Optional[str] = None

class RelativeOut(BaseModel):
    relationship: str
    name: str
    age: Optional[int] = None
    address: Optional[str] = None
    phone: Optional[str] = None
    email: Optional[str] = None
    source: str

class EducationOut(BaseModel):
    school: str
    degree: str
    major: Optional[str] = None
    grad_year: Optional[str] = None
    state: str
    source: str

class EmploymentOut(BaseModel):
    job_title: str
    employer: str
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    industry: Optional[str] = None
    source: str

class AircraftOut(BaseModel):
    n_number: str
    model: str
    year: int
    reg_date: Optional[str] = None
    state: str
    source: str

class BoatOut(BaseModel):
    hull_id: str
    vessel_name: str
    year: int
    reg_date: Optional[str] = None
    state: str
    source: str

class SocialDeepOut(BaseModel):
    twitter_handle: Optional[str] = None
    linkedin_url: Optional[str] = None
    instagram_handle: Optional[str] = None
    facebook_url: Optional[str] = None
    tiktok_handle: Optional[str] = None

class RiskEventOut(BaseModel):
    date: str
    type: str
    severity: int
    description: str
    case_number: Optional[str] = None
    court: Optional[str] = None
    disposition: Optional[str] = None
    source: str
    case_number: Optional[str] = None
    court: Optional[str] = None
    disposition: Optional[str] = None
    source: str

class AssociateOut(BaseModel):
    person_can_id: str
    name: str
    relationship: str
    strength: int

class PersonReportOut(BaseModel):
    subject: SubjectOut
    aliases: List[AliasOut]
    addresses: List[AddressOut]
    flags: dict
    criminal_records: List[RiskEventOut]
    associates: List[AssociateOut]
    relatives_deep: List[RelativeOut]
    education: List[EducationOut]
