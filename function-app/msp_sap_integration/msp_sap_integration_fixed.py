import pandas as pd
import numpy as np
import os
from datetime import datetime
import logging
import concurrent.futures
import functools
from typing import Dict, List, Any, Optional, Tuple
import time
import pyodbc
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("simplified_sap_processor")

# Database connection parameters
DB_SERVER = "sql-sap-prod-v2.database.windows.net"
DB_NAME = "sap-integration-db-v2"
DB_USER = "sqladmin"
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Constants
BATCH_SIZE = 1000
MAX_WORKERS = 8
CACHE_EXPIRY = 3600

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def safe_float_conversion(value):
    """Safely convert a value to float, handling various number formats"""
    if pd.isna(value):
        return 0.0
        
    str_value = str(value).strip()
    
    if not str_value:
        return 0.0
    
    # Remove currency symbols and keep only digits, comma, dot, minus
    cleaned = ""
    for char in str_value:
        if char.isdigit() or char in [',', '.', '-']:
            cleaned += char
    
    if not cleaned:
        return 0.0
    
    try:
        return float(cleaned)
    except ValueError:
        try:
            # Handle European format
            if ',' in cleaned and '.' in cleaned:
                last_dot = cleaned.rindex('.')
                last_comma = cleaned.rindex(',')
                
                if last_dot > last_comma:
                    # US format (1,234.56)
                    cleaned = cleaned.replace(',', '')
                else:
                    # European format (1.234,56)
                    cleaned = cleaned.replace('.', '')
                    cleaned = cleaned.replace(',', '.')
            else:
                # Only comma exists, treat as decimal separator
                cleaned = cleaned.replace(',', '.')
                
            return float(cleaned)
        except (ValueError, IndexError):
            logger.warning(f"Could not convert '{str_value}' to float, using 0 instead")
            return 0.0

def safe_get(row, column, default=None):
    """Safely get a value from a pandas row, converting NaN to a default value"""
    if column not in row or pd.isna(row[column]):
        return default
    return row[column]

def safe_int_conversion(value):
    """Safely convert a value to int, handling various formats"""
    if pd.isna(value) or value is None:
        return None
        
    str_value = str(value).strip()
    
    if not str_value or str_value.lower() in ['', 'nan', 'none', 'null']:
        return None
    
    # Remove decimal part if present
    if '.' in str_value:
        str_value = str_value.split('.')[0]
    
    try:
        result = int(float(str_value))
        return result if result != 0 else None  # Convert 0 to None for optional fields
    except (ValueError, TypeError):
        logger.warning(f"Could not convert '{value}' to int, using None instead")
        return None

def safe_string_conversion(value):
    """Safely convert a value to string, handling None/NaN"""
    if pd.isna(value) or value is None:
        return None
    
    str_value = str(value).strip()
    
    if not str_value or str_value.lower() in ['nan', 'none', 'null', '']:
        return None
    
    return str_value

# =============================================================================
# DATABASE MANAGER
# =============================================================================

class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self):
        self.connection_string = None
        self.engine = None
        self._setup_connection()
    
    def _setup_connection(self):
        """Setup database connection string and SQLAlchemy engine"""
        try:
            if not DB_PASSWORD:
                raise ValueError("DB_PASSWORD environment variable is not set or is empty")
            
            # Build connection string for pyodbc
            self.connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={DB_SERVER};"
                f"DATABASE={DB_NAME};"
                f"UID={DB_USER};"
                f"PWD={DB_PASSWORD};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
            )
            
            # Build SQLAlchemy connection string
            quoted_password = quote_plus(str(DB_PASSWORD))
            sqlalchemy_url = (
                f"mssql+pyodbc://{DB_USER}:{quoted_password}@{DB_SERVER}/{DB_NAME}"
                f"?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
            )
            
            self.engine = create_engine(sqlalchemy_url, fast_executemany=True)
            logger.info("✅ Database connection configured successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to setup database connection: {str(e)}")
            raise
    
    def test_connection(self):
        """Test the database connection"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test")).fetchone()
                logger.info("✅ Database connection test successful")
                return True
        except Exception as e:
            logger.error(f"❌ Database connection test failed: {str(e)}")
            return False
    
    def get_latest_batch_id(self, table_name: str, batch_pattern: str) -> str:
        """Get the most recent batch_id for a table"""
        try:
            query = text(f"""
                SELECT TOP 1 batch_id 
                FROM {table_name} 
                WHERE batch_id LIKE :pattern 
                ORDER BY upload_date DESC, batch_id DESC
            """)
            
            with self.engine.connect() as conn:
                result = conn.execute(query, {"pattern": batch_pattern}).fetchone()
                if result:
                    logger.info(f"Latest batch for {table_name}: {result[0]}")
                    return result[0]
                else:
                    logger.warning(f"No batches found for {table_name} with pattern {batch_pattern}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error getting latest batch for {table_name}: {str(e)}")
            raise
    
    def read_table_as_dataframe(self, table_name: str, batch_id: str = None, column_mapping: dict = None) -> pd.DataFrame:
        """Read table data as pandas DataFrame"""
        try:
            # Build the query
            if batch_id:
                query = f"SELECT * FROM {table_name} WHERE batch_id = '{batch_id}'"
            else:
                query = f"SELECT * FROM {table_name}"
            
            logger.info(f"📊 Reading {table_name}...")
            
            df = pd.read_sql_query(query, self.engine)
            
            # Apply column mapping if provided
            if column_mapping:
                df = df.rename(columns=column_mapping)
            
            # Reset index and ensure clean DataFrame
            df = df.reset_index(drop=True)
            
            logger.info(f"✅ Read {len(df)} records from {table_name}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error reading {table_name}: {str(e)}")
            raise

# Initialize database manager
db_manager = DatabaseManager()

# =============================================================================
# COLUMN MAPPINGS
# =============================================================================

SAP_COLUMN_MAPPING = {
    # SAP Export Feldnamen → Python Code Namen
    'buchungskreis': 'Buchungskreis',
    'hauptbuchkonto': 'Hauptbuchkonto', 
    'geschaeftsjahr': 'Geschäftsjahr',
    'buchungsperiode': 'Buchungsperiode',
    'belegart': 'Belegart',
    'belegnummer': 'Belegnummer',
    'buchungsdatum': 'Buchungsdatum',
    'belegdatum': 'Belegdatum',
    'text_field': 'Text',
    'soll_haben_kennz': 'Soll/Haben Kennzeichen',
    'buchungsschluessel': 'Buchungsschlüssel',
    'betrag_in_hauswaehrung': 'Betrag in Hauswährung',
    'kostenstelle': 'Kostenstelle',
    'auftrag': 'Auftrag',
    'psp_element': 'PSP-Element',
    'einkaufsbeleg': 'Einkaufsbeleg',
    'steuerkennzeichen': 'Steuerkennzeichen',
    'geschaeftsbereich': 'Geschäftsbereich',
    'ausgleichsbeleg': 'Ausgleichsbeleg',
    'konto_gegenbuchung': 'Konto Gegenbuchung',
    'material': 'Material'
}

FLOOR_MAPPING_COLUMNS = {
    'department': 'Department',
    'region': 'Region', 
    'district': 'District',
    'kostenstelle': 'Kostenstelle'
}

HQ_MAPPING_COLUMNS = {
    'bezeichnung': 'Bezeichnung',
    'abteilung': 'Abteilung',
    'kostenstelle': 'Kostenstelle '  # Note the trailing space
}

# =============================================================================
# LOCATION INFO CLASS
# =============================================================================

class LocationInfo:
    """Simple class to store location information"""
    
    def __init__(self, department, region, district):
        self.department = None if pd.isna(department) else department
        self.region = None if pd.isna(region) else region
        self.district = None if pd.isna(district) else district

# =============================================================================
# CACHE IMPLEMENTATION
# =============================================================================

class Cache:
    """Simple in-memory cache with expiry"""
    
    def __init__(self, expiry_seconds=3600):
        self._cache = {}
        self._timestamps = {}
        self._expiry_seconds = expiry_seconds
    
    def get(self, key):
        """Get value from cache if it exists and is not expired"""
        if key not in self._cache:
            return None
            
        timestamp = self._timestamps.get(key, 0)
        if time.time() - timestamp > self._expiry_seconds:
            # Expired
            del self._cache[key]
            del self._timestamps[key]
            return None
            
        return self._cache[key]
    
    def set(self, key, value):
        """Set value in cache with current timestamp"""
        self._cache[key] = value
        self._timestamps[key] = time.time()
    
    def clear(self):
        """Clear all cached values"""
        self._cache.clear()
        self._timestamps.clear()

# Initialize cache
kostenstelle_cache = Cache(CACHE_EXPIRY)

# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================

def read_from_database(table_type: str) -> pd.DataFrame:
    """Read data from database tables based on table type"""
    logger.info(f"Reading {table_type} data from database...")
    
    if table_type == "sap":
        latest_batch = db_manager.get_latest_batch_id("sap_transactions", "TEST_BATCH_%")
        if not latest_batch:
            raise ValueError("No SAP data found in database")
        
        df = db_manager.read_table_as_dataframe(
            "sap_transactions", 
            latest_batch, 
            SAP_COLUMN_MAPPING
        )
        
    elif table_type == "mapping_floor":
        latest_batch = db_manager.get_latest_batch_id("kostenstelle_mapping_floor", "TEST_BATCH_%")
        if not latest_batch:
            raise ValueError("No Floor mapping data found in database")
        
        df = db_manager.read_table_as_dataframe(
            "kostenstelle_mapping_floor",
            latest_batch,
            FLOOR_MAPPING_COLUMNS
        )
        
    elif table_type == "mapping_hq":
        latest_batch = db_manager.get_latest_batch_id("kostenstelle_mapping_hq", "TEST_BATCH_%")
        if not latest_batch:
            raise ValueError("No HQ mapping data found in database")
        
        df = db_manager.read_table_as_dataframe(
            "kostenstelle_mapping_hq",
            latest_batch,
            HQ_MAPPING_COLUMNS
        )
        
    else:
        raise ValueError(f"Unknown table type: {table_type}")
    
    return df

# =============================================================================
# MAPPING FUNCTIONS
# =============================================================================

def create_mapping_index(mapping_floor: pd.DataFrame, mapping_hq: pd.DataFrame) -> Dict[str, LocationInfo]:
    """Create an index for Kostenstelle mapping"""
    mapping_index = {}
    
    # Index HQ mappings (starting with 1)
    for _, row in mapping_hq.iterrows():
        kostenstelle_col = 'Kostenstelle ' if 'Kostenstelle ' in row.index else 'Kostenstelle'
        kostenstelle = str(safe_get(row, kostenstelle_col, '')).strip()
        
        if not kostenstelle:
            continue
            
        mapping_index[kostenstelle] = LocationInfo(
            department=safe_get(row, 'Abteilung', ''),
            region=safe_get(row, 'Bezeichnung', ''),
            district='HQ'
        )
    
    # Index Floor mappings
    for _, row in mapping_floor.iterrows():
        extracted_digits = str(safe_get(row, 'Kostenstelle', '')).strip()
        
        if not extracted_digits:
            continue
            
        # Store with FLOOR_ prefix to indicate it's for Floor
        mapping_index[f"FLOOR_{extracted_digits}"] = LocationInfo(
            department=safe_get(row, 'Department', ''),
            region=safe_get(row, 'Region', ''),
            district=safe_get(row, 'District', 'Floor')
        )
    
    logger.info(f"Created mapping index with {len(mapping_index)} entries")
    return mapping_index

def map_kostenstelle_cached(kostenstelle: str, mapping_index: Dict[str, LocationInfo]) -> Optional[Tuple[LocationInfo, str]]:
    """Map Kostenstelle to location information with caching"""
    # Check cache first
    cached_result = kostenstelle_cache.get(kostenstelle)
    if cached_result is not None:
        return cached_result
    
    # Ensure kostenstelle is a string without decimal part
    if not kostenstelle:
        return None
    
    kostenstelle = str(kostenstelle).strip()
    if '.' in kostenstelle:
        kostenstelle = kostenstelle.split('.')[0]
    
    # Ensure we have at least 5 digits
    if len(kostenstelle) < 5:
        return None
    
    location_type = None
    result = None
    
    if kostenstelle.startswith('1'):
        # HQ Kostenstelle - use full number
        result = mapping_index.get(kostenstelle)
        location_type = 'HQ'
    
    elif kostenstelle.startswith('3'):
        # Floor Kostenstelle - extract digits 2-6
        extracted_digits = kostenstelle[1:6]
        
        # Try direct lookup
        result = mapping_index.get(extracted_digits)
        
        # Try with FLOOR_ prefix
        if result is None:
            result = mapping_index.get(f"FLOOR_{extracted_digits}")
            
        # Try without leading zeros
        if result is None:
            stripped_digits = extracted_digits.lstrip('0')
            result = mapping_index.get(stripped_digits)
            if result is None:
                result = mapping_index.get(f"FLOOR_{stripped_digits}")
        
        if result is not None:
            location_type = 'Floor'
    
    # Prepare the result
    final_result = (result, location_type) if result is not None else None
    
    # Cache the result
    kostenstelle_cache.set(kostenstelle, final_result)
    return final_result

# =============================================================================
# PROCESSING FUNCTIONS
# =============================================================================

def process_sap_transactions_extended_fixed(sap_data: pd.DataFrame, mapping_index: Dict[str, LocationInfo]) -> Tuple[List[Dict], List[Dict]]:
    """Process SAP transactions with extended fields"""
    direct_costs = []
    outliers = []
    
    logger.info(f"Processing {len(sap_data)} SAP transactions with extended fields...")
    
    for _, transaction in sap_data.iterrows():
        # Extract Kostenstelle
        kostenstelle = str(safe_get(transaction, 'Kostenstelle', ''))
        
        # Try to map Kostenstelle to location
        location_result = map_kostenstelle_cached(kostenstelle, mapping_index)
        
        # Extended Transaction data with all new fields
        transaction_data = {
            # Basic fields
            'transaction_id': safe_string_conversion(safe_get(transaction, 'Belegnummer', '')),
            'amount': safe_float_conversion(safe_get(transaction, 'Betrag in Hauswährung', 0)),
            'kostenstelle': safe_string_conversion(kostenstelle),
            'text_description': safe_string_conversion(safe_get(transaction, 'Text', '')),
            'booking_date': safe_string_conversion(safe_get(transaction, 'Buchungsdatum', '')),
            
            # Extended SAP fields
            'buchungskreis': safe_string_conversion(safe_get(transaction, 'Buchungskreis', '')),
            'hauptbuchkonto': safe_string_conversion(safe_get(transaction, 'Hauptbuchkonto', '')),
            'geschaeftsjahr': safe_int_conversion(safe_get(transaction, 'Geschäftsjahr', '')),
            'belegart': safe_string_conversion(safe_get(transaction, 'Belegart', '')),
            'belegdatum': safe_string_conversion(safe_get(transaction, 'Belegdatum', '')),
            'auftrag': safe_string_conversion(safe_get(transaction, 'Auftrag', '')),
            'psp_element': safe_string_conversion(safe_get(transaction, 'PSP-Element', '')),
            'einkaufsbeleg': safe_string_conversion(safe_get(transaction, 'Einkaufsbeleg', '')),
            'geschaeftsbereich': safe_string_conversion(safe_get(transaction, 'Geschäftsbereich', '')),
            'konto_gegenbuchung': safe_string_conversion(safe_get(transaction, 'Konto Gegenbuchung', '')),
            'material': safe_string_conversion(safe_get(transaction, 'Material', '')),
            'soll_haben_kennz': safe_string_conversion(safe_get(transaction, 'Soll/Haben Kennzeichen', '')),
            'buchungsschluessel': safe_string_conversion(safe_get(transaction, 'Buchungsschlüssel', '')),
            'steuerkennzeichen': safe_string_conversion(safe_get(transaction, 'Steuerkennzeichen', '')),
            'ausgleichsbeleg': safe_string_conversion(safe_get(transaction, 'Ausgleichsbeleg', '')),
            'buchungsperiode': safe_int_conversion(safe_get(transaction, 'Buchungsperiode', ''))
        }
        
        if location_result is None:
            # Could not map Kostenstelle - OUTLIER
            outlier_data = {
                **transaction_data,
                'department': None,
                'region': None,
                'district': None,
                'location_type': 'Unknown',
                'category': 'OUTLIER',
                'status': 'Unknown Location'
            }
            outliers.append(outlier_data)
            
        else:
            # Successfully mapped - DIRECT_COST
            location_info, location_type = location_result
            
            direct_cost_data = {
                **transaction_data,
                'department': safe_string_conversion(location_info.department),
                'region': safe_string_conversion(location_info.region),
                'district': safe_string_conversion(location_info.district),
                'location_type': location_type,
                'category': 'DIRECT_COST',
                'status': 'Direct Booked'
            }
            direct_costs.append(direct_cost_data)
    
    logger.info(f"✅ Processed: {len(direct_costs)} direct costs, {len(outliers)} outliers")
    return direct_costs, outliers

# =============================================================================
# SAVE FUNCTION
# =============================================================================

def save_transactions_final(direct_costs, outliers, batch_id, processing_date):
    """Save processed transactions to database"""
    try:
        all_transactions = direct_costs + outliers
        logger.info(f"💾 Saving {len(all_transactions)} transactions...")
        
        with db_manager.engine.connect() as conn:
            success_count = 0
            
            for i, tx in enumerate(all_transactions):
                try:
                    # Prepare values for database
                    values = {
                        'transaction_id': str(tx.get('transaction_id', '')),
                        'amount': float(tx.get('amount', 0)),
                        'kostenstelle': str(tx.get('kostenstelle', '')),
                        'text_description': str(tx.get('text_description', '')) if tx.get('text_description') else None,
                        'booking_date': tx.get('booking_date'),
                        'department': str(tx.get('department', '')) if tx.get('department') else None,
                        'region': str(tx.get('region', '')) if tx.get('region') else None,
                        'district': str(tx.get('district', '')) if tx.get('district') else None,
                        'location_type': str(tx.get('location_type', 'Unknown')),
                        'category': str(tx.get('category', 'OUTLIER')),
                        'status': str(tx.get('status', 'Unknown')),
                        'batch_id': str(batch_id),
                        'processing_date': processing_date,
                        
                        # Extended SAP fields
                        'konto_gegenbuchung': str(tx.get('konto_gegenbuchung', '')) if tx.get('konto_gegenbuchung') else None,
                        'material': str(tx.get('material', '')) if tx.get('material') else None,
                        'soll_haben_kennz': str(tx.get('soll_haben_kennz', '')) if tx.get('soll_haben_kennz') else None,
                        'hauptbuchkonto': str(tx.get('hauptbuchkonto', '')) if tx.get('hauptbuchkonto') else None,
                        'buchungsschluessel': str(tx.get('buchungsschluessel', '')) if tx.get('buchungsschluessel') else None,
                        'belegdatum': tx.get('belegdatum'),
                        'geschaeftsbereich': str(tx.get('geschaeftsbereich', '')) if tx.get('geschaeftsbereich') else None,
                        'einkaufsbeleg': str(tx.get('einkaufsbeleg', '')) if tx.get('einkaufsbeleg') else None,
                        'psp_element': str(tx.get('psp_element', '')) if tx.get('psp_element') else None,
                        'auftrag': str(tx.get('auftrag', '')) if tx.get('auftrag') else None,
                        'steuerkennzeichen': str(tx.get('steuerkennzeichen', '')) if tx.get('steuerkennzeichen') else None,
                        'buchungskreis': str(tx.get('buchungskreis', '')) if tx.get('buchungskreis') else None,
                        'ausgleichsbeleg': str(tx.get('ausgleichsbeleg', '')) if tx.get('ausgleichsbeleg') else None,
                        'geschaeftsjahr': int(tx.get('geschaeftsjahr')) if tx.get('geschaeftsjahr') not in [None, '', 'None'] else None,
                        'buchungsperiode': int(tx.get('buchungsperiode')) if tx.get('buchungsperiode') not in [None, '', 'None'] else None,
                        'belegart': str(tx.get('belegart', '')) if tx.get('belegart') else None
                    }
                    
                    # Clean empty strings to None
                    for key, value in values.items():
                        if value == '' or value == 'None' or value == 'nan':
                            values[key] = None
                    
                    # INSERT Query
                    insert_query = text("""
                        INSERT INTO sap_transactions_processed (
                            transaction_id, amount, kostenstelle, text_description, booking_date,
                            department, region, district, location_type, category, status,
                            batch_id, processing_date,
                            konto_gegenbuchung, material, soll_haben_kennz, hauptbuchkonto, buchungsschluessel,
                            belegdatum, geschaeftsbereich, einkaufsbeleg, psp_element, auftrag,
                            steuerkennzeichen, buchungskreis, ausgleichsbeleg, geschaeftsjahr, buchungsperiode, belegart
                        ) VALUES (
                            :transaction_id, :amount, :kostenstelle, :text_description, :booking_date,
                            :department, :region, :district, :location_type, :category, :status,
                            :batch_id, :processing_date,
                            :konto_gegenbuchung, :material, :soll_haben_kennz, :hauptbuchkonto, :buchungsschluessel,
                            :belegdatum, :geschaeftsbereich, :einkaufsbeleg, :psp_element, :auftrag,
                            :steuerkennzeichen, :buchungskreis, :ausgleichsbeleg, :geschaeftsjahr, :buchungsperiode, :belegart
                        )
                    """)
                    
                    conn.execute(insert_query, values)
                    success_count += 1
                        
                except Exception as row_error:
                    logger.warning(f"⚠️ Skipped row {i+1}: {str(row_error)}")
                    continue
            
            # Final commit
            conn.commit()
            
        logger.info(f"✅ Successfully inserted {success_count}/{len(all_transactions)} transactions")
        
        # Summary
        direct_count = sum(1 for tx in all_transactions if tx.get('category') == 'DIRECT_COST')
        outlier_count = sum(1 for tx in all_transactions if tx.get('category') == 'OUTLIER')
        total_amount = sum(tx.get('amount', 0) for tx in all_transactions)
        
        logger.info(f"📊 Summary:")
        logger.info(f"   - DIRECT_COST: {direct_count} transactions")
        logger.info(f"   - OUTLIERS: {outlier_count} transactions")
        logger.info(f"   - Total Amount: €{total_amount:,.2f}")
        
        return success_count
        
    except Exception as e:
        logger.error(f"❌ Fatal error in save: {str(e)}")
        raise

# =============================================================================
# MAIN PROCESSING FUNCTION
# =============================================================================

def main_final():
    """Main processing function with all steps"""
    start_time = time.time()
    logger.info('🚀 SAP processing started at: %s', datetime.now())
    
    try:
        # Step 1: Load data
        logger.info("📊 Loading SAP data from database...")
        sap_data = read_from_database("sap")
        mapping_floor = read_from_database("mapping_floor")
        mapping_hq = read_from_database("mapping_hq")
        
        logger.info(f"✅ Loaded {len(sap_data)} SAP transactions")
        logger.info(f"✅ Loaded {len(mapping_floor)} Floor mappings")
        logger.info(f"✅ Loaded {len(mapping_hq)} HQ mappings")
        
        # Step 2: Create mapping
        logger.info("🔍 Creating kostenstelle mapping...")
        mapping_index = create_mapping_index(mapping_floor, mapping_hq)
        kostenstelle_cache.clear()
        
        # Step 3: Process transactions
        logger.info("⚡ Processing SAP transactions with extended fields...")
        direct_costs, outliers = process_sap_transactions_extended_fixed(sap_data, mapping_index)
        
        logger.info(f"✅ Processing complete: {len(direct_costs)} direct costs, {len(outliers)} outliers")
        
        # Step 4: Save to database
        batch_id = f"FINAL_BATCH_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        processing_date = datetime.now()
        
        logger.info("💾 Saving to database...")
        success_count = save_transactions_final(direct_costs, outliers, batch_id, processing_date)
        
        # Final summary
        elapsed_time = time.time() - start_time
        
        logger.info('🎉 Processing completed successfully!')
        logger.info(f'   ⏱️ Processing time: {elapsed_time:.2f} seconds')
        logger.info(f'   📊 Transactions saved: {success_count}')
        logger.info(f'   🏷️ Batch ID: {batch_id}')
        
        return {
            "status": "success",
            "message": "Processing completed successfully",
            "transactions_saved": success_count,
            "batch_id": batch_id,
            "processing_time": elapsed_time
        }
        
    except Exception as e:
        logger.error('❌ Fatal error in processing: %s', str(e), exc_info=True)
        raise

# =============================================================================
# MAIN FUNCTION FOR AZURE FUNCTIONS
# =============================================================================

def main() -> dict:
    """Main function for Azure Functions"""
    start_time = time.time()
    logger.info('🚀 BARMER SAP processing started at: %s', datetime.now())
    
    try:
        # Database connection test
        if not db_manager.test_connection():
            raise ConnectionError("Cannot connect to Barmer database")
        
        logger.info("✅ Barmer database connection successful!")
        
        # Run the complete processing
        result = main_final()
        
        elapsed_time = time.time() - start_time
        logger.info('✅ BARMER processing completed in %.2f seconds', elapsed_time)
        
        return {
            "status": "success",
            "message": "BARMER SAP processing completed successfully",
            "processing_time": elapsed_time,
            "details": result
        }
    
    except Exception as e:
        logger.error('❌ Error in BARMER processing: %s', str(e), exc_info=True)
        return {
            "status": "error",
            "message": str(e),
            "processing_time": time.time() - start_time
        }

# =============================================================================
# MAIN EXECUTION (for local testing)
# =============================================================================

if __name__ == "__main__":
    try:
        # Check if database password is set
        if not DB_PASSWORD:
            print("❌ ERROR: DB_PASSWORD environment variable not set!")
            print("\nPlease set the database password using one of these methods:")
            print("1. Command Prompt: set DB_PASSWORD=your_password")
            print("2. PowerShell: $env:DB_PASSWORD=\"your_password\"")
            print("3. Add to Windows Environment Variables permanently")
            exit(1)
        
        logger.info(f"🔐 Using database password: {'*' * len(str(DB_PASSWORD))}")
        
        # Test database connection first
        logger.info("🔌 Testing database connection...")
        if not db_manager.test_connection():
            logger.error("❌ Database connection test failed!")
            exit(1)
        
        # Run the main processing function
        result = main()
        
        logger.info("\n🎉 Processing completed!")
        logger.info("💡 Your transactions now include all SAP fields:")
        logger.info("   📋 Buchungskreis, Hauptbuchkonto, Geschäftsjahr")
        logger.info("   📄 Belegart, Belegdatum, Material")
        logger.info("   🏗️ Auftrag, PSP-Element, Einkaufsbeleg")
        logger.info("   🏢 Geschäftsbereich, Konto Gegenbuchung")
        logger.info("   ⚖️ Soll/Haben, Buchungsschlüssel, etc.")
        logger.info(f"   📊 Result: {result}")
        
    except Exception as e:
        logger.error(f"💥 Fatal error in execution: {str(e)}", exc_info=True)
        exit(1)