from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import os
import sys
import json
import pandas as pd
import tempfile
from datetime import datetime
import logging
import time

# Database constants
DB_SERVER = "sql-sap-prod-v2.database.windows.net"
DB_NAME = "sap-integration-db-v2"

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sap_api")

# =============================================================================
# SMART IMPORT SYSTEM
# =============================================================================

# Try different import strategies
PROCESSING_AVAILABLE = False
db_manager = None

def setup_imports():
    """Setup imports with fallback strategies"""
    global PROCESSING_AVAILABLE, db_manager
    
    try:
        # Strategy 1: Local imports (same directory) - for Azure deployment
        logger.info("Trying local imports...")
        from msp_sap_integration_fixed import main as process_data_main
        from database_manager_azure import AzureDatabaseManager
        
        # Use Azure Database Manager
        db_manager = AzureDatabaseManager()
        PROCESSING_AVAILABLE = True
        
        logger.info("✅ Local imports successful - using Azure Database Manager")
        return True
        
    except ImportError as e1:
        logger.warning(f"Local imports failed: {e1}")
        
        try:
            # Strategy 2: Function-app imports - for local development
            logger.info("Trying function-app imports...")
            sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'function-app', 'msp_sap_integration'))
            
            from msp_sap_integration_fixed import DatabaseManager, main as process_data_main
            
            # Use original Database Manager
            db_manager = DatabaseManager()
            PROCESSING_AVAILABLE = True
            
            logger.info("✅ Function-app imports successful - using original Database Manager")
            return True
            
        except ImportError as e2:
            logger.warning(f"Function-app imports failed: {e2}")
            
            # Strategy 3: Fallback - create dummy functions
            logger.warning("Using fallback dummy functions")
            
            class FallbackDatabaseManager:
                def __init__(self):
                    self.engine = None
                def test_connection(self):
                    return False
            
            global process_data_main
            def process_data_main():
                return {"status": "error", "message": "Processing functions not available"}
            
            db_manager = FallbackDatabaseManager()
            PROCESSING_AVAILABLE = False
            
            return False

# Setup imports during module load
setup_imports()

def get_db_manager():
    """Get database manager instance"""
    global db_manager
    return db_manager

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

class JSONEncoder(json.JSONEncoder):
    """Custom JSON encoder for pandas and numpy types"""
    def default(self, obj):
        if isinstance(obj, pd.Timestamp):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, (pd.Series, pd.DataFrame)):
            return obj.to_dict()
        elif hasattr(obj, 'item'):  # numpy types
            return obj.item()
        elif hasattr(obj, 'strftime'):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif pd.isna(obj):
            return None
        return super().default(obj)

def get_frontend_view(view_name):
    """Get frontend view from database"""
    try:
        db_mgr = get_db_manager()
        if not db_mgr or not hasattr(db_mgr, 'engine') or not db_mgr.engine:
            return None
            
        from sqlalchemy import text
        
        query = text("""
            SELECT view_data 
            FROM frontend_views 
            WHERE view_name = :view_name
            ORDER BY created_at DESC
        """)
        
        with db_mgr.engine.connect() as conn:
            result = conn.execute(query, {"view_name": view_name}).fetchone()
            
            if result:
                return json.loads(result[0])
            else:
                return None
                
    except Exception as e:
        logger.error(f"Error getting frontend view {view_name}: {str(e)}")
        return None

def save_frontend_view(view_name, data):
    """Save frontend view to database"""
    try:
        db_mgr = get_db_manager()
        if not db_mgr or not hasattr(db_mgr, 'engine') or not db_mgr.engine:
            return False
            
        from sqlalchemy import text
        
        json_data = json.dumps(data, cls=JSONEncoder, separators=(',', ':'))
        timestamp = datetime.now()
        
        with db_mgr.engine.connect() as conn:
            # Delete previous view
            delete_query = text("DELETE FROM frontend_views WHERE view_name = :view_name")
            conn.execute(delete_query, {"view_name": view_name})
            
            # Insert new view
            insert_query = text("""
                INSERT INTO frontend_views (view_name, view_data, created_at)
                VALUES (:view_name, :view_data, :created_at)
            """)
            
            conn.execute(insert_query, {
                "view_name": view_name,
                "view_data": json_data,
                "created_at": timestamp
            })
            conn.commit()
            
        return True
        
    except Exception as e:
        logger.error(f"Error saving frontend view {view_name}: {str(e)}")
        return False

# =============================================================================
# CORE API ENDPOINTS
# =============================================================================

@app.route('/')
def home():
    """Home endpoint with API information"""
    return jsonify({
        "status": "online",
        "service": "SAP Integration API",
        "version": "4.2.0 (Azure Compatible)",
        "azure_web_app": "app-sap-integration-api-h7hwc9fwaugghnce.germanywestcentral-01.azurewebsites.net",
        "database": {
            "server": DB_SERVER,
            "database": DB_NAME
        },
        "features": [
            "Direct SAP transaction processing",
            "Kostenstelle mapping (HQ + Floor)",
            "Database connectivity with fallback",
            "Azure Linux compatible"
        ],
        "processing_available": PROCESSING_AVAILABLE,
        "database_manager": type(db_manager).__name__ if db_manager else "None",
        "data_categories": ["DIRECT_COST", "OUTLIER"],
        "github_repo": "https://github.com/sadu619/Finanzen",
        "timestamp": datetime.now().isoformat()
    })

@app.route('/api/health', methods=['GET'])
def health_check():
    """Comprehensive health check"""
    try:
        # Test database connection
        db_mgr = get_db_manager()
        db_connected = False
        
        if db_mgr and hasattr(db_mgr, 'test_connection'):
            try:
                db_connected = db_mgr.test_connection()
            except Exception as e:
                logger.warning(f"Database connection test failed: {e}")
        
        # Check environment variables
        db_password_set = bool(os.getenv("DB_PASSWORD"))
        
        # Check if processed table has data
        transaction_count = 0
        data_available = False
        
        if db_connected and db_mgr and hasattr(db_mgr, 'engine') and db_mgr.engine:
            try:
                with db_mgr.engine.connect() as conn:
                    from sqlalchemy import text
                    count_query = text("SELECT COUNT(*) FROM sap_transactions_processed")
                    transaction_count = conn.execute(count_query).scalar()
                    data_available = transaction_count > 0
            except Exception as e:
                logger.warning(f"Could not check transaction count: {str(e)}")
        
        status = "healthy" if (db_connected and db_password_set) else "unhealthy"
        
        return jsonify({
            "status": status,
            "version": "4.2.0 (Azure Compatible)",
            "azure_web_app": {
                "name": "app-sap-integration-api-h7hwc9fwaugghnce",
                "resource_group": "marketing_controlling",
                "region": "Germany West Central"
            },
            "database": {
                "connected": db_connected,
                "password_configured": db_password_set,
                "server": DB_SERVER,
                "database": DB_NAME,
                "manager_type": type(db_mgr).__name__ if db_mgr else "None"
            },
            "data": {
                "available": data_available,
                "total_transactions": transaction_count
            },
            "processing": {
                "available": PROCESSING_AVAILABLE,
                "functions_imported": PROCESSING_AVAILABLE
            },
            "endpoints": [
                "/api/health",
                "/api/process", 
                "/api/transactions-raw",
                "/api/database-test",
                "/api/environment"
            ],
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Health check error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/api/process', methods=['POST'])
def trigger_processing():
    """Manually trigger SAP data processing"""
    try:
        if not PROCESSING_AVAILABLE:
            return jsonify({
                "status": "error",
                "message": "Processing functions not available - check import errors",
                "suggestion": "Ensure msp_sap_integration_fixed.py is in the correct location"
            }), 500
            
        if not os.getenv("DB_PASSWORD"):
            return jsonify({
                "status": "error",
                "message": "DB_PASSWORD not configured in Azure Web App settings"
            }), 500
        
        # Run processing
        start_time = time.time()
        result = process_data_main()
        processing_time = time.time() - start_time
        
        return jsonify({
            "status": "success",
            "message": "SAP processing completed successfully",
            "processing_time_seconds": round(processing_time, 2),
            "result": result if result else "Processing completed",
            "data_categories": ["DIRECT_COST", "OUTLIER"],
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Error in manual processing: {str(e)}")
        return jsonify({
            "status": "error", 
            "message": str(e),
            "troubleshooting": [
                "Check DB_PASSWORD in Azure Web App settings",
                "Verify database connectivity",
                "Check processing logic imports"
            ]
        }), 500

@app.route('/api/transactions-raw', methods=['GET'])
def get_transactions_raw():
    """Get raw transactions directly from database"""
    try:
        db_mgr = get_db_manager()
        
        if not db_mgr or not hasattr(db_mgr, 'engine') or not db_mgr.engine:
            return jsonify({
                "transactions": [],
                "error": "Database connection not available"
            }), 500
        
        from sqlalchemy import text
        
        # Try to get data from processed table first
        try:
            with db_mgr.engine.connect() as conn:
                query = text("SELECT TOP 10 * FROM sap_transactions_processed ORDER BY processing_date DESC")
                result = conn.execute(query)
                
                transactions = []
                for row in result:
                    transaction = {col: str(val) if val is not None else None for col, val in zip(result.keys(), row)}
                    transactions.append(transaction)
                
                return jsonify({
                    "transactions": transactions,
                    "count": len(transactions),
                    "source": "sap_transactions_processed",
                    "message": "Raw data from processed table"
                })
                
        except Exception as e:
            # Fallback to raw input table
            try:
                with db_mgr.engine.connect() as conn:
                    query = text("SELECT TOP 10 * FROM sap_transactions ORDER BY upload_date DESC")
                    result = conn.execute(query)
                    
                    transactions = []
                    for row in result:
                        transaction = {col: str(val) if val is not None else None for col, val in zip(result.keys(), row)}
                        transactions.append(transaction)
                    
                    return jsonify({
                        "transactions": transactions,
                        "count": len(transactions),
                        "source": "sap_transactions",
                        "message": "Raw data from input table"
                    })
            except Exception as e2:
                return jsonify({
                    "transactions": [],
                    "error": f"Could not query any table: {str(e2)}"
                }), 500
        
    except Exception as e:
        logger.error(f"Error fetching transactions: {str(e)}")
        return jsonify({
            "transactions": [],
            "error": str(e)
        }), 500

@app.route('/api/database-test', methods=['GET'])
def database_test():
    """Test database connectivity and show table info"""
    try:
        db_mgr = get_db_manager()
        
        if not db_mgr:
            return jsonify({
                "status": "error",
                "message": "DatabaseManager not available"
            })
        
        results = {
            "connection_test": False,
            "tables": {},
            "errors": [],
            "database_manager_type": type(db_mgr).__name__
        }
        
        # Test connection
        try:
            if hasattr(db_mgr, 'test_connection'):
                results["connection_test"] = db_mgr.test_connection()
            elif hasattr(db_mgr, 'engine') and db_mgr.engine:
                with db_mgr.engine.connect() as conn:
                    from sqlalchemy import text
                    conn.execute(text("SELECT 1"))
                results["connection_test"] = True
        except Exception as e:
            results["errors"].append(f"Connection test failed: {str(e)}")
        
        # Test tables
        if results["connection_test"] and hasattr(db_mgr, 'engine') and db_mgr.engine:
            tables_to_check = [
                'sap_transactions',
                'sap_transactions_processed',
                'kostenstelle_mapping_floor',
                'kostenstelle_mapping_hq',
                'frontend_views'
            ]
            
            from sqlalchemy import text
            
            for table in tables_to_check:
                try:
                    with db_mgr.engine.connect() as conn:
                        count_query = text(f"SELECT COUNT(*) as count FROM {table}")
                        result = conn.execute(count_query).fetchone()
                        results["tables"][table] = {
                            "exists": True,
                            "row_count": result[0] if result else 0
                        }
                except Exception as e:
                    results["tables"][table] = {
                        "exists": False,
                        "error": str(e)
                    }
        
        return jsonify(results)
        
    except Exception as e:
        logger.error(f"Database test error: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/environment', methods=['GET'])
def environment_info():
    """Show environment information for debugging"""
    try:
        return jsonify({
            "environment_variables": {
                "DB_PASSWORD": "SET" if os.getenv("DB_PASSWORD") else "NOT SET",
                "DB_SERVER": os.getenv("DB_SERVER", "Using default"),
                "DB_NAME": os.getenv("DB_NAME", "Using default"),
                "DB_USER": os.getenv("DB_USER", "Using default"),
                "PYTHONPATH": os.getenv("PYTHONPATH", "Not set")
            },
            "python_info": {
                "version": sys.version,
                "path": sys.path[:5]  # First 5 entries
            },
            "processing_status": {
                "functions_available": PROCESSING_AVAILABLE,
                "database_manager": type(db_manager).__name__ if db_manager else "Not available"
            },
            "current_directory": os.getcwd(),
            "files_in_directory": os.listdir('.'),
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

# =============================================================================
# ERROR HANDLERS
# =============================================================================

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "status": "error",
        "message": "Endpoint not found",
        "available_endpoints": [
            "/",
            "/api/health",
            "/api/process",
            "/api/transactions-raw",
            "/api/database-test",
            "/api/environment"
        ]
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "status": "error",
        "message": "Internal server error",
        "timestamp": datetime.now().isoformat()
    }), 500

# =============================================================================
# STARTUP
# =============================================================================

if __name__ == '__main__':
    print("\n" + "="*60)
    print("🚀 SAP INTEGRATION API v4.2.0 (Azure Compatible)")
    print("="*60)
    print(f"🌐 Azure Web App: app-sap-integration-api-h7hwc9fwaugghnce")
    print(f"📊 Database: {DB_SERVER}")
    print(f"🗄️  Database: {DB_NAME}")
    print(f"⚙️  Processing Available: {PROCESSING_AVAILABLE}")
    print(f"🔧 Database Manager: {type(db_manager).__name__ if db_manager else 'None'}")
    print("\n🔗 Endpoints:")
    print("   - GET  /              - API info")
    print("   - GET  /api/health    - Health check")
    print("   - POST /api/process   - Trigger processing")
    print("   - GET  /api/transactions-raw - Raw data")
    print("   - GET  /api/database-test - DB test")
    print("   - GET  /api/environment - Debug info")
    print("="*60)
    
    if not os.getenv("DB_PASSWORD"):
        print("⚠️  WARNING: DB_PASSWORD not set!")
    else:
        print("✅ Database password configured")
    
    print(f"🚀 Starting Flask server...")
    print("="*60 + "\n")
    
    app.run(host='0.0.0.0', port=8000, debug=False)