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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("msp_sap_integration")

# 🔄 UPDATED: Simple SQL Authentication Database connection parameters
DB_SERVER = "sql-sap-prod-v2.database.windows.net"
DB_NAME = "sap-integration-db-v2"
DB_USER = "sqladmin"
DB_PASSWORD = os.getenv("DB_PASSWORD")

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
    🔄 UPDATED: Simple SQL Authentication Database Manager
    """
    
    def __init__(self):
        self.connection_string = None
        self.engine = None
        self._setup_connection()
    
    def _setup_connection(self):  # ← FIX: 4 Spaces Indentation!
        """🔄 UPDATED: Back to simple SQL Authentication"""
        try:
            # Validate that password is set
            if not DB_PASSWORD:
                raise ValueError("DB_PASSWORD environment variable is not set or is empty")
            
            logger.info("🔐 Setting up SQL Authentication (simple & reliable)...")
            
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
            logger.info("✅ SQL Authentication database connection configured successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to setup database connection: {str(e)}")
            raise
    
    def test_connection(self):  # ← FIX: 4 Spaces Indentation!
        """🔄 UPDATED: Simple connection test"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test")).fetchone()
                logger.info(f"✅ Database connection test successful")
                logger.info(f"🔐 Connected as: {DB_USER}")
                return True
        except Exception as e:
            logger.error(f"❌ Database connection test failed: {str(e)}")
            return False

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