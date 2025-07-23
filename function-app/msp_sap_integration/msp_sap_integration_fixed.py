import pandas as pd
import numpy as np
import re
import json
import os
from datetime import datetime
import logging
import concurrent.futures
import functools
from typing import Dict, List, Any, Optional, Tuple, Set
import time
import pyodbc
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("msp_sap_integration")

# 🔄 UPDATED: Enterprise Database connection parameters
DB_SERVER = "sql-sap-integration-prod.database.windows.net"
DB_NAME = "sap-integration-db"
# 🔄 REMOVED: DB_USER and DB_PASSWORD (using Entra Authentication)

# Constants
BATCH_SIZE = 1000
MAX_WORKERS = 8
CACHE_EXPIRY = 3600

# [Your existing helper functions - keeping them unchanged]
def safe_float_conversion(value):
    """
    Safely convert a value to float, handling various number formats:
    - European format (comma as decimal separator)
    - Currency symbols (€, $, etc.)
    - Thousands separators (dot in European format, comma in US format)
    """
    if pd.isna(value):
        return 0.0
        
    # Convert to string first
    str_value = str(value).strip()
    
    # Return 0 for empty strings
    if not str_value:
        return 0.0
    
    # Remove currency symbols and other non-numeric characters
    # Keep only digits, comma, dot, minus sign
    cleaned = ""
    for char in str_value:
        if char.isdigit() or char in [',', '.', '-']:
            cleaned += char
    
    # If empty after cleaning, return 0
    if not cleaned:
        return 0.0
    
    try:
        # Try direct conversion first (works for US format)
        return float(cleaned)
    except ValueError:
        # European format handling
        try:
            # For European format: replace decimal comma with dot
            # If both dot and comma exist, assume the last one is the decimal separator
            if ',' in cleaned and '.' in cleaned:
                # Get the positions of the last dot and comma
                last_dot = cleaned.rindex('.')
                last_comma = cleaned.rindex(',')
                
                if last_dot > last_comma:
                    # US format with thousands separator (e.g., 1,234.56)
                    # Remove all commas
                    cleaned = cleaned.replace(',', '')
                else:
                    # European format with thousands separator (e.g., 1.234,56)
                    # Replace all dots with empty string and the last comma with dot
                    cleaned = cleaned.replace('.', '')
                    cleaned = cleaned.replace(',', '.')
            else:
                # Only comma exists, treat as decimal separator
                cleaned = cleaned.replace(',', '.')
                
            return float(cleaned)
        except (ValueError, IndexError):
            # If still fails, log and return 0
            logger.warning(f"Could not convert '{str_value}' to float, using 0 instead")
            return 0.0
        
def safe_get(row, column, default=None):
    """
    Safely get a value from a pandas row, converting NaN to a default value
    """
    if column not in row or pd.isna(row[column]):
        return default
    return row[column]

class DatabaseManager:
    """
    🔄 UPDATED: Manages database connections with Entra Authentication (Managed Identity)
    """
    
    def __init__(self):
        self.connection_string = None
        self.engine = None
        self._setup_connection()
    
    def _setup_connection(self):
        """🔄 UPDATED: Setup database connection with Entra Authentication"""
        try:
            # 🔄 NEW: Use Azure Managed Identity for authentication
            logger.info("🔐 Setting up Entra Authentication...")
            
            # Try Managed Identity first (works in Azure Function App)
            try:
                credential = ManagedIdentityCredential()
                token = credential.get_token("https://database.windows.net/.default")
                access_token = token.token
                logger.info("✅ Successfully obtained Managed Identity token")
            except Exception as e:
                logger.warning(f"⚠️ Managed Identity failed, trying DefaultAzureCredential: {str(e)}")
                # Fallback to DefaultAzureCredential (for local development)
                credential = DefaultAzureCredential()
                token = credential.get_token("https://database.windows.net/.default")
                access_token = token.token
                logger.info("✅ Successfully obtained DefaultAzureCredential token")
            
            # 🔄 UPDATED: Build connection string with access token
            self.connection_string = (
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={DB_SERVER};"
                f"DATABASE={DB_NAME};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
            )
            
            # 🔄 UPDATED: SQLAlchemy connection with token-based auth
            # Note: For SQLAlchemy with Entra auth, we need to handle token refresh
            sqlalchemy_url = (
                f"mssql+pyodbc:///?odbc_connect={quote_plus(self.connection_string)}"
            )
            
            # Custom connection function that adds the access token
            def get_connection():
                conn = pyodbc.connect(
                    self.connection_string,
                    attrs_before={
                        1256: access_token  # SQL_COPT_SS_ACCESS_TOKEN
                    }
                )
                return conn
            
            self.engine = create_engine(
                sqlalchemy_url,
                creator=get_connection,
                fast_executemany=True
            )
            
            logger.info("✅ Enterprise database connection configured successfully with Entra Authentication")
            
        except Exception as e:
            logger.error(f"❌ Failed to setup enterprise database connection: {str(e)}")
            logger.error("🔧 Troubleshooting tips:")
            logger.error("   1. Ensure Function App has Managed Identity enabled")
            logger.error("   2. Ensure Managed Identity has db_datareader, db_datawriter roles on SQL Database")
            logger.error("   3. Check if SQL Server allows Azure services access")
            raise
    
    def test_connection(self):
        """🔄 UPDATED: Test the database connection with Entra Authentication"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test, SUSER_NAME() as current_user")).fetchone()
                logger.info(f"✅ Enterprise database connection test successful")
                logger.info(f"🔐 Connected as: {result[1]}")
                return True
        except Exception as e:
            logger.error(f"❌ Enterprise database connection test failed: {str(e)}")
            logger.error("🔧 Check:")
            logger.error("   1. Managed Identity permissions on SQL Database")
            logger.error("   2. Network connectivity (firewall rules)")
            logger.error("   3. SQL Server Entra Authentication configuration")
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
        """
        🔧 FIXED: Read table data as pandas DataFrame with FORCED STRING DTYPES for date columns
        This prevents pandas from auto-converting ISO date strings to datetime objects
        """
        try:
            # Build the query
            if batch_id:
                query = f"SELECT * FROM {table_name} WHERE batch_id = '{batch_id}'"
            else:
                query = f"SELECT * FROM {table_name}"
            
            # 🔧 FIX #1: Define dtype mapping to force ALL date columns to stay as strings
            # This prevents pandas from converting "2025-05-27" → datetime.date(2025, 5, 27)
            
            # Define known date columns that should NEVER be converted
            date_columns_to_preserve = [
                'datum',           # MSP date field
                'anfangsdatum',    # MSP start date
                'enddatum',        # MSP end date
                'buchungsdatum',   # SAP booking date
                'upload_date',     # System upload date
                'created_at',      # System created date
                'processing_date'  # System processing date
            ]
            
            # 🔧 FIX #2: Create dtype dictionary to force string type for ALL potential date columns
            dtype_dict = {}
            
            # First, read just the column names to see what we're dealing with
            with self.engine.connect() as conn:
                # Get column information
                column_query = text(f"""
                    SELECT COLUMN_NAME, DATA_TYPE 
                    FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_NAME = :table_name
                """)
                
                # Extract table name without schema if needed
                clean_table_name = table_name.split('.')[-1] if '.' in table_name else table_name
                
                columns_info = conn.execute(column_query, {"table_name": clean_table_name}).fetchall()
                
                # Force string dtype for known date columns AND any datetime-type columns
                for col_name, col_type in columns_info:
                    col_name_lower = col_name.lower()
                    col_type_lower = col_type.lower()
                    
                    # Force string for known date columns
                    if col_name_lower in [dc.lower() for dc in date_columns_to_preserve]:
                        dtype_dict[col_name] = 'string'
                        logger.info(f"🔧 Forcing {col_name} to string type (known date column)")
                    
                    # Force string for any SQL datetime/date type columns
                    elif any(dt in col_type_lower for dt in ['date', 'time', 'timestamp']):
                        dtype_dict[col_name] = 'string'
                        logger.info(f"🔧 Forcing {col_name} to string type (SQL date type: {col_type})")
            
            # 🔧 FIX #3: Read data with forced string dtypes for date columns
            logger.info(f"📊 Reading {table_name} with {len(dtype_dict)} forced string columns...")
            
            df = pd.read_sql_query(
                query, 
                self.engine,
                dtype=dtype_dict  # 🔧 This forces date columns to stay as strings!
            )
            
            # 🔧 FIX #4: Double-check and convert any remaining datetime objects to strings
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Check if this column contains datetime objects
                    sample_non_null = df[col].dropna()
                    if len(sample_non_null) > 0:
                        first_val = sample_non_null.iloc[0]
                        
                        # If it's a datetime-like object, convert the entire column to string
                        if hasattr(first_val, 'strftime') or str(type(first_val)) in ['<class \'datetime.date\'>', '<class \'datetime.datetime\'>', '<class \'pandas._libs.tslibs.timestamps.Timestamp\'>']:
                            logger.info(f"🔧 Converting remaining datetime objects in {col} to strings")
                            df[col] = df[col].apply(lambda x: x.strftime('%Y-%m-%d') if (pd.notna(x) and hasattr(x, 'strftime')) else str(x) if pd.notna(x) else x)
            
            # CRITICAL: Create a completely independent copy to break any database references
            df = df.copy(deep=True)
            
            # Apply column mapping if provided (to maintain compatibility with existing code)
            if column_mapping:
                df = df.rename(columns=column_mapping)
            
            # CRITICAL: Reset index and ensure clean DataFrame
            df = df.reset_index(drop=True)
            
            # 🔧 FIX #5: Log sample date values to verify they're strings
            for col in df.columns:
                col_lower = col.lower()
                if any(date_term in col_lower for date_term in ['datum', 'date', 'zeit', 'time']):
                    sample_vals = df[col].dropna().head(3).tolist()
                    logger.info(f"📅 Date column {col} sample values: {sample_vals} (types: {[type(v).__name__ for v in sample_vals]})")
            
            logger.info(f"✅ Read {len(df)} records from {table_name} with proper string date handling")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error reading {table_name}: {str(e)}")
            raise

# Initialize database manager
db_manager = DatabaseManager()

# ==============================================================================
# ENHANCED JSON SERIALIZATION - HANDLES ALL DATETIME OBJECTS
# ==============================================================================

def make_json_serializable(obj, _seen=None):
    """
    🔧 ENHANCED: Convert objects that are not JSON serializable to serializable formats
    Now handles ALL datetime object types that might slip through
    """
    if _seen is None:
        _seen = set()
    
    # Check for circular references
    obj_id = id(obj)
    if obj_id in _seen:
        return f"<Circular Reference: {type(obj).__name__}>"
    
    if isinstance(obj, (dict, list, pd.Series, pd.DataFrame)):
        _seen.add(obj_id)
    
    try:
        # 🔧 FIX #6: Handle ALL possible datetime object types
        if isinstance(obj, pd.Timestamp):
            return obj.strftime('%Y-%m-%d')
        elif hasattr(obj, 'strftime'):  # This catches datetime.date, datetime.datetime, etc.
            return obj.strftime('%Y-%m-%d')
        elif str(type(obj)) in ['<class \'datetime.date\'>', '<class \'datetime.datetime\'>']:
            # Fallback for datetime objects
            return obj.strftime('%Y-%m-%d') if hasattr(obj, 'strftime') else str(obj)
        elif isinstance(obj, pd.Series):
            # Convert Series to simple dict, avoiding circular references
            result = {}
            for k, v in obj.items():
                try:
                    if pd.isna(v):
                        result[str(k)] = None
                    else:
                        result[str(k)] = make_json_serializable(v, _seen.copy())
                except:
                    result[str(k)] = str(v) if v is not None else None
            return result
        elif isinstance(obj, pd.DataFrame):
            # Convert DataFrame to simple list of dicts
            try:
                records = obj.to_dict(orient='records')
                result = []
                for record in records:
                    clean_record = {}
                    for k, v in record.items():
                        try:
                            if pd.isna(v):
                                clean_record[str(k)] = None
                            else:
                                clean_record[str(k)] = make_json_serializable(v, _seen.copy())
                        except:
                            clean_record[str(k)] = str(v) if v is not None else None
                    result.append(clean_record)
                return result
            except:
                return f"<DataFrame with {len(obj)} rows>"
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            if np.isnan(obj):
                return None
            return float(obj)
        elif isinstance(obj, np.ndarray):
            try:
                return [make_json_serializable(x, _seen.copy()) for x in obj.tolist()]
            except:
                return f"<Array with {len(obj)} items>"
        elif isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                try:
                    if pd.isna(v):
                        result[str(k)] = None
                    else:
                        result[str(k)] = make_json_serializable(v, _seen.copy())
                except:
                    result[str(k)] = str(v) if v is not None else None
            return result
        elif isinstance(obj, (list, tuple)):
            try:
                return [make_json_serializable(item, _seen.copy()) for item in obj]
            except:
                return f"<List with {len(obj)} items>"
        elif pd.isna(obj):
            return None
        elif hasattr(obj, '__dict__'):
            # For custom objects, convert to simple dict
            try:
                return {k: make_json_serializable(v, _seen.copy()) for k, v in obj.__dict__.items() 
                       if not k.startswith('_')}
            except:
                return str(obj)
        else:
            return obj
    except Exception as e:
        # 🔧 FIX #7: Enhanced fallback for any datetime-like objects
        obj_str = str(obj)
        if 'datetime.date(' in obj_str and ')' in obj_str:
            # Extract date from string representation like "datetime.date(2025, 5, 27)"
            try:
                # Parse the datetime.date string format
                import re
                match = re.search(r'datetime\.date\((\d+),\s*(\d+),\s*(\d+)\)', obj_str)
                if match:
                    year, month, day = match.groups()
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            except:
                pass
        
        # Ultimate fallback
        return str(obj) if obj is not None else None
    finally:
        if obj_id in _seen:
            _seen.discard(obj_id)

class JSONEncoder(json.JSONEncoder):
    """
    🔧 ENHANCED: Custom JSON encoder that handles pandas and numpy types with enhanced datetime support
    """
    def default(self, obj):
        # Handle NaN, infinity, and -infinity
        if isinstance(obj, float):
            if np.isnan(obj):
                return None
            elif np.isinf(obj) and obj > 0:
                return "Infinity"
            elif np.isinf(obj) and obj < 0:
                return "-Infinity"
        
        # 🔧 FIX #8: Enhanced datetime handling in JSON encoder
        if hasattr(obj, 'strftime'):
            return obj.strftime('%Y-%m-%d')
        
        try:
            return make_json_serializable(obj)
        except Exception as e:
            # Enhanced fallback for datetime objects
            obj_str = str(obj)
            if 'datetime.date(' in obj_str:
                try:
                    import re
                    match = re.search(r'datetime\.date\((\d+),\s*(\d+),\s*(\d+)\)', obj_str)
                    if match:
                        year, month, day = match.groups()
                        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except:
                    pass
            
            # Ultimate fallback
            return str(obj) if obj is not None else None

# ==============================================================================
# SIMPLE MAIN FUNCTION FOR TESTING
# ==============================================================================

def main() -> None:
    """
    🔄 SIMPLIFIED: Main function for testing enterprise database connection
    """
    start_time = time.time()
    logger.info('🚀 Enterprise database-integrated processing started at: %s', datetime.now())
    
    try:
        # Step 0: Test database connection
        logger.info("🔌 Testing enterprise database connection...")
        if not db_manager.test_connection():
            raise ConnectionError("Cannot connect to enterprise database")
        
        logger.info("✅ Enterprise database connection successful!")
        logger.info("🎉 MSP-SAP Integration ready for enterprise use!")
        
        elapsed_time = time.time() - start_time
        logger.info('✅ Enterprise setup completed successfully in %.2f seconds at: %s', 
                   elapsed_time, datetime.now())
        
        return {
            "status": "success",
            "message": "Enterprise database connection established",
            "connection_time": elapsed_time
        }
    
    except Exception as e:
        logger.error('❌ Error in enterprise setup: %s', str(e), exc_info=True)
        raise

# For testing locally
if __name__ == "__main__":
    try:
        result = main()
        print(f"✅ Success: {result}")
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        exit(1)