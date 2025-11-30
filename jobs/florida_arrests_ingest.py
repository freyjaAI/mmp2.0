"""
Florida Arrests Daily Ingestion Job
Week 3 - Criminal risk signal integration

Fetches booking records from Florida arrest databases and:
- Parses fixed-width format booking records
- De-duplicates by booking_number + name + DOB
- Maps to person_raw table
- Creates person_risk_signal entries with signal_type='ARREST', severity=7
- Designed for daily 04:00 UTC cron runs
"""

import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
import requests
import os
import hashlib
from typing import List, Dict, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database connection
DB_DSN = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/riskdb')

# Florida Arrest Data Sources (placeholder - replace with actual endpoints)
FLORIDA_FTP_URL = os.getenv('FLORIDA_FTP_URL', 'ftp://florida.arrests.gov/daily_bookings.txt')


def get_db_connection():
    """Get PostgreSQL database connection"""
    return psycopg2.connect(DB_DSN)


def parse_florida_booking_record(line: str) -> Optional[Dict]:
    """
    Parse fixed-width format Florida booking record
    
    Example format (columns are fixed-width):
    BOOKING_NUM  LAST_NAME    FIRST_NAME   DOB        OFFENSE_DATE  CHARGES
    FL2023-12345 SMITH        JOHN         19850615   20231115      THEFT
    
    Args:
        line: Fixed-width booking record string
        
    Returns:
        Dictionary with parsed booking data or None if invalid
    """
    try:
        # Fixed-width field positions (adjust based on actual format)
        booking_num = line[0:13].strip()
        last_name = line[13:33].strip()
        first_name = line[33:53].strip()
        dob_str = line[53:61].strip()  # YYYYMMDD
        offense_date_str = line[61:69].strip()  # YYYYMMDD
        charges = line[69:].strip()
        
        if not booking_num or not last_name or not first_name:
            return None
            
        # Parse dates
        dob = datetime.strptime(dob_str, '%Y%m%d').date() if dob_str else None
        offense_date = datetime.strptime(offense_date_str, '%Y%m%d').date() if offense_date_str else None
        
        return {
            'booking_number': booking_num,
            'last_name': last_name.upper(),
            'first_name': first_name.upper(),
            'dob': dob,
            'offense_date': offense_date,
            'charges': charges,
            'source': 'florida_arrests'
        }
    except Exception as e:
        logger.warning(f"Failed to parse booking record: {e}")
        return None


def calculate_hash(booking_number: str, last_name: str, first_name: str, dob: Optional[datetime]) -> str:
    """
    Calculate deduplication hash for booking record
    Hash = SHA256(booking_number + last_name + first_name + DOB)
    """
    dob_str = dob.isoformat() if dob else ''
    hash_input = f"{booking_number}|{last_name}|{first_name}|{dob_str}"
    return hashlib.sha256(hash_input.encode()).hexdigest()


def fetch_florida_arrests() -> List[Dict]:
    """
    Fetch daily arrests from Florida FTP/API
    Returns list of parsed booking records
    """
    logger.info(f"Fetching arrests from {FLORIDA_FTP_URL}")
    
    # Placeholder implementation - replace with actual FTP/HTTP fetch
    # In production, this would:
    # 1. Connect to Florida FTP server
    # 2. Download daily booking file
    # 3. Parse each line
    
    bookings = []
    
    try:
        # Example: fetch from HTTP endpoint instead of FTP for simplicity
        # response = requests.get(FLORIDA_FTP_URL, timeout=30)
        # response.raise_for_status()
        # lines = response.text.split('\n')
        
        # For demo: use sample data
        sample_data = [
            "FL2024-98765 DOE          JANE         19920314   20241130      BATTERY",
            "FL2024-98766 SMITH        JOHN         19850615   20241130      THEFT",
        ]
        
        for line in sample_data:
            if line.strip():
                booking = parse_florida_booking_record(line)
                if booking:
                    bookings.append(booking)
                    
        logger.info(f"Fetched {len(bookings)} booking records")
        return bookings
        
    except Exception as e:
        logger.error(f"Failed to fetch Florida arrests: {e}")
        raise


def insert_person_raw(conn, booking: Dict) -> Optional[str]:
    """
    Insert booking into person_raw table
    
    Returns:
        person_raw_id (UUID) if inserted, None if duplicate
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # Calculate deduplication hash
    hash_blob = calculate_hash(
        booking['booking_number'],
        booking['last_name'],
        booking['first_name'],
        booking['dob']
    )
    
    try:
        # Insert into person_raw
        cur.execute("""
            INSERT INTO person_raw (
                src_name,
                src_row_id,
                last_name_std,
                first_name_std,
                dob,
                hash_blob,
                ingested_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (src_name, src_row_id) DO NOTHING
            RETURNING person_raw_id
        """, (
            booking['source'],
            booking['booking_number'],
            booking['last_name'],
            booking['first_name'],
            booking['dob'],
            hash_blob
        ))
        
        result = cur.fetchone()
        conn.commit()
        
        if result:
            logger.info(f"Inserted person_raw for booking {booking['booking_number']}")
            return str(result['person_raw_id'])
        else:
            logger.debug(f"Duplicate booking skipped: {booking['booking_number']}")
            return None
            
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to insert person_raw: {e}")
        raise


def create_risk_signal(conn, person_canon_id: str, booking: Dict):
    """
    Create person_risk_signal entry for arrest
    
    Args:
        person_canon_id: UUID of canonical person
        booking: Booking record dictionary
    """
    cur = conn.cursor()
    
    try:
        cur.execute("""
            INSERT INTO person_risk_signal (
                person_canon_id,
                signal_type,
                event_date,
                severity,
                src_name,
                src_row_id,
                raw_json,
                ingested_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        """, (
            person_canon_id,
            'ARREST',  # Signal type
            booking['offense_date'],
            7,  # Severity for arrests
            booking['source'],
            booking['booking_number'],
            psycopg2.extras.Json({
                'charges': booking['charges'],
                'booking_date': datetime.now().isoformat(),
                'source': booking['source']
            })
        ))
        
        conn.commit()
        logger.info(f"Created risk signal for booking {booking['booking_number']}")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to create risk signal: {e}")
        raise


def process_bookings(bookings: List[Dict]) -> Dict[str, int]:
    """
    Process bookings: insert into person_raw and create risk signals
    
    Returns:
        Statistics dictionary
    """
    conn = get_db_connection()
    stats = {
        'total': len(bookings),
        'inserted': 0,
        'duplicates': 0,
        'errors': 0
    }
    
    try:
        for booking in bookings:
            try:
                # Insert into person_raw
                person_raw_id = insert_person_raw(conn, booking)
                
                if person_raw_id:
                    stats['inserted'] += 1
                    
                    # For now, we'll need entity resolution to get person_canon_id
                    # In a full implementation, this would:
                    # 1. Run blocking on new person_raw record
                    # 2. Match to existing canon or create new canon
                    # 3. Create risk signal with canon_id
                    
                    # Placeholder: assume person_raw_id == person_canon_id for demo
                    # In production, this should call entity resolution
                    # create_risk_signal(conn, person_raw_id, booking)
                    
                else:
                    stats['duplicates'] += 1
                    
            except Exception as e:
                stats['errors'] += 1
                logger.error(f"Error processing booking {booking.get('booking_number')}: {e}")
                
        return stats
        
    finally:
        conn.close()


def main():
    """Main execution function for Florida arrests ingestion"""
    logger.info("=" * 80)
    logger.info("Starting Florida Arrests Daily Ingestion Job")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 80)
    
    try:
        # Fetch arrests
        bookings = fetch_florida_arrests()
        
        if not bookings:
            logger.warning("No bookings fetched")
            return
        
        # Process bookings
        stats = process_bookings(bookings)
        
        # Log statistics
        logger.info("=" * 80)
        logger.info("Ingestion Complete")
        logger.info(f"Total records processed: {stats['total']}")
        logger.info(f"New records inserted: {stats['inserted']}")
        logger.info(f"Duplicates skipped: {stats['duplicates']}")
        logger.info(f"Errors: {stats['errors']}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.error(f"Fatal error in ingestion job: {e}")
        raise


if __name__ == "__main__":
    main()
