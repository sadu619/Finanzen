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

# Import from function-app directory (angepasst für Ihre Struktur)
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'function-app', 'msp_sap_integration'))

try:
    from msp_sap_integration_fixed import (
        DatabaseManager,
        get_processed_transactions,
        get_department_summary,
        get_outlier_analysis,
        main as process_data_main
    )
    print("✅ Successfully imported from function-app/msp_sap_integration/")
except ImportError:
    # Fallback: Try to import from same directory (for deployment)
    try:
        from msp_sap_integration_fixed import (
            DatabaseManager,
            get_processed_transactions, 
            get_department_summary,
            get_outlier_analysis,
            main as process_data_main
        )
        print("✅ Successfully imported from current directory")
    except ImportError as e:
        print(f"❌ Could not import processing functions: {e}")
        print("Available files:", os.listdir('.'))
        # Define dummy functions to prevent startup errors
        class DatabaseManager:
            def test_connection(self):
                return False
        
        def get_processed_transactions():
            return pd.DataFrame()
        
        def get_department_summary():
            return pd.DataFrame()
            
        def get_outlier_analysis():
            return pd.DataFrame()
            
        def process_data_main():
            return "Processing functions not available"

# Database constants - Ihre Konfiguration
DB_SERVER = "sql-sap-prod-v2.database.windows.net"
DB_NAME = "sap-integration-db-v2"

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sap_integration_api")

# Initialize database manager
db_manager = None

def get_db_manager():
    """Get or create database manager instance"""
    global db_manager
    if db_manager is None:
        try:
            db_manager = DatabaseManager()
        except Exception as e:
            logger.error(f"Could not initialize DatabaseManager: {e}")
            db_manager = None
    return db_manager

# Custom JSON encoder for pandas/numpy objects
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
        if not db_mgr:
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
        if not db_mgr:
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
        "version": "4.0.0",
        "azure_web_app": "app-sap-integration-api-h7hwc9fwaugghnce.germanywestcentral-01.azurewebsites.net",
        "database": {
            "server": DB_SERVER,
            "database": DB_NAME
        },
        "features": [
            "Direct SAP transaction processing",
            "Kostenstelle mapping (HQ + Floor)",
            "Department & Region analysis", 
            "Outlier detection",
            "Budget allocation management"
        ],
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
        db_connected = db_mgr.test_connection() if db_mgr else False
        
        # Check environment variables
        db_password_set = bool(os.getenv("DB_PASSWORD"))
        
        # Check if processed table has data
        transaction_count = 0
        data_available = False
        
        if db_connected and db_mgr:
            try:
                with db_mgr.engine.connect() as conn:
                    from sqlalchemy import text
                    count_query = text("SELECT COUNT(*) FROM sqlsap_transactions_processed")
                    transaction_count = conn.execute(count_query).scalar()
                    data_available = transaction_count > 0
            except Exception as e:
                logger.warning(f"Could not check transaction count: {str(e)}")
        
        status = "healthy" if (db_connected and db_password_set and data_available) else "unhealthy"
        
        return jsonify({
            "status": status,
            "version": "4.0.0",
            "azure_web_app": {
                "name": "app-sap-integration-api-h7hwc9fwaugghnce",
                "resource_group": "marketing_controlling",
                "region": "Germany West Central"
            },
            "database": {
                "connected": db_connected,
                "password_configured": db_password_set,
                "server": DB_SERVER,
                "database": DB_NAME
            },
            "data": {
                "available": data_available,
                "total_transactions": transaction_count
            },
            "processing_integration": {
                "azure_function": "msp_sap_integration",
                "github_actions": "Auto-deploy enabled",
                "shared_database": "Azure SQL"
            },
            "endpoints": [
                "/api/health",
                "/api/process", 
                "/api/transactions",
                "/api/departments",
                "/api/outliers",
                "/api/data",
                "/api/budget-allocation"
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
        logger.info("🚀 Manual processing trigger started...")
        
        # Check database password
        if not os.getenv("DB_PASSWORD"):
            return jsonify({
                "status": "error", 
                "message": "Database password not configured. Please set DB_PASSWORD environment variable in Azure Web App settings."
            }), 500
        
        # Run the main processing function
        start_time = time.time()
        result = process_data_main()
        processing_time = time.time() - start_time
        
        return jsonify({
            "status": "success",
            "message": "SAP processing completed successfully",
            "processing_time_seconds": round(processing_time, 2),
            "result": result if result else "Processing completed",
            "data_categories": ["DIRECT_COST", "OUTLIER"],
            "azure_function_integration": "Direct call to processing logic",
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
                "Check Azure Function logs for details"
            ]
        }), 500

@app.route('/api/transactions', methods=['GET'])
def get_transactions():
    """Get processed transactions with filtering"""
    try:
        logger.info("📊 Fetching transactions...")
        
        # Get query parameters
        department = request.args.get('department')
        region = request.args.get('region')
        category = request.args.get('category')
        limit = request.args.get('limit', 1000, type=int)
        
        # Get transactions using processing functions
        df = get_processed_transactions()
        
        if df.empty:
            return jsonify({
                "transactions": [],
                "summary": {
                    "total_count": 0,
                    "message": "No processed transactions found. Try running /api/process first."
                }
            })
        
        # Apply filters
        filtered_df = df.copy()
        
        if department:
            filtered_df = filtered_df[filtered_df['department'] == department]
        if region:
            filtered_df = filtered_df[filtered_df['region'] == region]
        if category:
            filtered_df = filtered_df[filtered_df['category'] == category]
        
        # Limit results
        if len(filtered_df) > limit:
            filtered_df = filtered_df.head(limit)
        
        # Convert to JSON-friendly format
        transactions = []
        for _, row in filtered_df.iterrows():
            transaction = {
                'transaction_id': str(row.get('transaction_id', '')),
                'amount': float(row.get('amount', 0)),
                'kostenstelle': str(row.get('kostenstelle', '')),
                'text_description': str(row.get('text_description', '')),
                'booking_date': str(row.get('booking_date', '')),
                'department': str(row.get('department', '')),
                'region': str(row.get('region', '')),
                'district': str(row.get('district', '')),
                'location_type': str(row.get('location_type', '')),
                'category': str(row.get('category', '')),
                'status': str(row.get('status', 'processed')),
                'batch_id': str(row.get('batch_id', '')),
                'processing_date': str(row.get('processing_date', ''))
            }
            transactions.append(transaction)
        
        # Calculate summary statistics
        direct_costs = [t for t in transactions if t['category'] == 'DIRECT_COST']
        outliers = [t for t in transactions if t['category'] == 'OUTLIER']
        
        total_amount = sum(t['amount'] for t in transactions)
        
        response_data = {
            "transactions": transactions,
            "direct_costs": direct_costs,
            "outliers": outliers,
            "summary": {
                "total_transactions": len(transactions),
                "direct_costs_count": len(direct_costs),
                "outliers_count": len(outliers),
                "total_amount": total_amount,
                "filters_applied": {
                    "department": department,
                    "region": region,
                    "category": category
                }
            },
            "pagination": {
                "limit": limit,
                "returned": len(transactions),
                "has_more": len(df) > limit
            }
        }
        
        logger.info(f"✅ Returning {len(transactions)} transactions")
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Error fetching transactions: {str(e)}")
        return jsonify({
            "transactions": [],
            "error": str(e),
            "suggestion": "Try running /api/process to generate processed data"
        }), 500

@app.route('/api/departments', methods=['GET'])
def get_departments():
    """Get department summary"""
    try:
        logger.info("🏢 Fetching department summary...")
        
        # Use the processing function
        departments_df = get_department_summary()
        
        if departments_df.empty:
            return jsonify({
                "departments": [],
                "summary": {"message": "No department data available. Try running /api/process first."}
            })
        
        # Convert to JSON format
        departments = []
        for _, row in departments_df.iterrows():
            department = {
                'name': str(row.get('department', '')),
                'location_type': str(row.get('location_type', '')),
                'transaction_count': int(row.get('transaction_count', 0)),
                'total_amount': float(row.get('total_amount', 0)),
                'average_amount': float(row.get('avg_amount', 0)),
                'category': str(row.get('category', ''))
            }
            departments.append(department)
        
        # Group by department for summary
        dept_summary = {}
        for dept in departments:
            key = dept['name']
            if key not in dept_summary:
                dept_summary[key] = {
                    'name': key,
                    'total_amount': 0,
                    'transaction_count': 0,
                    'location_types': set()
                }
            
            dept_summary[key]['total_amount'] += dept['total_amount']
            dept_summary[key]['transaction_count'] += dept['transaction_count']
            dept_summary[key]['location_types'].add(dept['location_type'])
        
        # Convert sets to lists for JSON serialization
        for dept in dept_summary.values():
            dept['location_types'] = list(dept['location_types'])
        
        response_data = {
            "departments": departments,
            "department_summary": list(dept_summary.values()),
            "summary": {
                "total_departments": len(dept_summary),
                "total_amount": sum(d['total_amount'] for d in departments),
                "total_transactions": sum(d['transaction_count'] for d in departments)
            }
        }
        
        logger.info(f"✅ Returning {len(departments)} department entries")
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Error fetching departments: {str(e)}")
        return jsonify({
            "departments": [],
            "error": str(e)
        }), 500

@app.route('/api/outliers', methods=['GET'])
def get_outliers():
    """Get outlier analysis - unmapped kostenstellen"""
    try:
        logger.info("🔍 Fetching outlier analysis...")
        
        # Use the processing function
        outliers_df = get_outlier_analysis()
        
        if outliers_df.empty:
            return jsonify({
                "outliers": [],
                "summary": {"message": "No outliers found"}
            })
        
        # Convert to JSON format
        outliers = []
        for _, row in outliers_df.iterrows():
            outlier = {
                'kostenstelle': str(row.get('kostenstelle', '')),
                'transaction_count': int(row.get('transaction_count', 0)),
                'total_amount': float(row.get('total_amount', 0)),
                'average_amount': float(row.get('avg_amount', 0)),
                'first_booking': str(row.get('first_booking', '')),
                'last_booking': str(row.get('last_booking', ''))
            }
            outliers.append(outlier)
        
        response_data = {
            "outliers": outliers,
            "summary": {
                "total_unmapped_kostenstellen": len(outliers),
                "total_outlier_amount": sum(o['total_amount'] for o in outliers),
                "total_outlier_transactions": sum(o['transaction_count'] for o in outliers),
                "largest_outlier": max(outliers, key=lambda x: x['total_amount']) if outliers else None
            }
        }
        
        logger.info(f"✅ Returning {len(outliers)} unmapped kostenstellen")
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Error fetching outliers: {str(e)}")
        return jsonify({
            "outliers": [],
            "error": str(e)
        }), 500

@app.route('/api/data', methods=['GET'])
def get_all_data():
    """Get comprehensive data for frontend dashboard"""
    try:
        logger.info("📊 Fetching comprehensive dashboard data...")
        
        # Get departments
        try:
            departments_data = get_frontend_view("frontend_departments")
            departments = departments_data.get('departments', []) if departments_data else []
        except:
            departments = []
        
        # Get regions  
        try:
            regions_data = get_frontend_view("frontend_regions")
            regions = regions_data.get('regions', []) if regions_data else []
        except:
            regions = []
        
        # Get budget allocation
        try:
            budget_data = get_frontend_view("budget_allocation")
            budget_allocation = budget_data if budget_data else {"departments": {}, "regions": {}}
        except:
            budget_allocation = {"departments": {}, "regions": {}}
        
        # Get transaction statistics
        transaction_stats = {}
        try:
            db_mgr = get_db_manager()
            if db_mgr:
                with db_mgr.engine.connect() as conn:
                    from sqlalchemy import text
                    
                    # Overall stats
                    stats_query = text("""
                        SELECT 
                            category,
                            COUNT(*) as count,
                            SUM(amount) as total_amount
                        FROM sqlsap_transactions_processed
                        GROUP BY category
                    """)
                    
                    result = conn.execute(stats_query)
                    for row in result:
                        category, count, amount = row
                        transaction_stats[f"{category.lower()}_count"] = count
                        transaction_stats[f"total_{category.lower()}_amount"] = float(amount) if amount else 0
                    
                    # Total stats
                    total_query = text("SELECT COUNT(*), SUM(amount) FROM sqlsap_transactions_processed")
                    total_result = conn.execute(total_query).fetchone()
                    transaction_stats['total_transactions'] = total_result[0]
                    transaction_stats['total_amount'] = float(total_result[1]) if total_result[1] else 0
                    
        except Exception as e:
            logger.warning(f"Could not fetch transaction stats: {str(e)}")
        
        response_data = {
            "departments": departments,
            "regions": regions,
            "budget_allocation": budget_allocation,
            "transaction_stats": transaction_stats,
            "system_info": {
                "version": "4.0.0",
                "data_categories": ["DIRECT_COST", "OUTLIER"],
                "azure_web_app": "app-sap-integration-api-h7hwc9fwaugghnce",
                "last_updated": datetime.now().isoformat()
            }
        }
        
        logger.info(f"✅ Dashboard data: {len(departments)} departments, {len(regions)} regions")
        return jsonify(response_data)
        
    except Exception as e:
        logger.error(f"Error fetching dashboard data: {str(e)}")
        return jsonify({
            "departments": [],
            "regions": [],
            "budget_allocation": {},
            "transaction_stats": {},
            "error": str(e)
        }), 500

@app.route('/api/budget-allocation', methods=['GET', 'POST'])
def budget_allocation():
    """Get or update budget allocations"""
    try:
        if request.method == 'GET':
            budget_data = get_frontend_view("budget_allocation")
            if not budget_data:
                budget_data = {
                    "departments": {},
                    "regions": {},
                    "last_updated": None
                }
            
            return jsonify(budget_data)
        
        elif request.method == 'POST':
            data = request.get_json()
            
            if not data:
                return jsonify({"status": "error", "message": "No data provided"}), 400
            
            # Validate structure
            if 'departments' not in data or 'regions' not in data:
                return jsonify({"status": "error", "message": "Invalid data structure"}), 400
            
            # Add timestamp
            data['last_updated'] = datetime.now().isoformat()
            
            # Save to database
            success = save_frontend_view("budget_allocation", data)
            
            if success:
                return jsonify({
                    "status": "success",
                    "message": "Budget allocation saved successfully"
                })
            else:
                return jsonify({
                    "status": "error",
                    "message": "Failed to save budget allocation"
                }), 500
    
    except Exception as e:
        logger.error(f"Budget allocation error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

# =============================================================================
# UTILITY ENDPOINTS
# =============================================================================

@app.route('/api/database-status', methods=['GET'])
def database_status():
    """Check database connection and table status"""
    try:
        db_mgr = get_db_manager()
        connection_ok = db_mgr.test_connection() if db_mgr else False
        
        table_status = {}
        if connection_ok and db_mgr:
            with db_mgr.engine.connect() as conn:
                from sqlalchemy import text
                
                # Check main tables
                tables_to_check = [
                    'sqlsap_transactions',
                    'sqlsap_transactions_processed', 
                    'kostenstelle_mapping_floor',
                    'kostenstelle_mapping_hq',
                    'frontend_views'
                ]
                
                for table in tables_to_check:
                    try:
                        count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                        table_status[table] = {
                            "exists": True,
                            "row_count": count
                        }
                    except:
                        table_status[table] = {"exists": False}
        
        return jsonify({
            "database_connected": connection_ok,
            "server": DB_SERVER,
            "database": DB_NAME,
            "tables": table_status,
            "azure_config": {
                "resource_group": "marketing_controlling",
                "web_app": "app-sap-integration-api-h7hwc9fwaugghnce"
            },
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Database status error: {str(e)}")
        return jsonify({
            "database_connected": False,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/api/export/<data_type>', methods=['GET'])
def export_data(data_type):
    """Export data as JSON file"""
    try:
        if data_type == "transactions":
            df = get_processed_transactions()
            data = df.to_dict(orient='records')
        elif data_type == "departments":
            df = get_department_summary()
            data = df.to_dict(orient='records')
        elif data_type == "outliers":
            df = get_outlier_analysis()
            data = df.to_dict(orient='records')
        elif data_type == "budget":
            data = get_frontend_view("budget_allocation")
        else:
            return jsonify({"error": f"Unknown data type: {data_type}"}), 404
        
        if not data:
            return jsonify({"error": f"No data found for {data_type}"}), 404
        
        # Create temporary file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as temp_file:
            json.dump(data, temp_file, cls=JSONEncoder, indent=2)
            temp_path = temp_file.name
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"sap_{data_type}_{timestamp}.json"
        
        return send_file(
            temp_path,
            mimetype='application/json',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        logger.error(f"Export error: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/performance', methods=['GET'])
def performance_stats():
    """Get API performance statistics"""
    try:
        db_mgr = get_db_manager()
        
        if not db_mgr:
            return jsonify({"error": "Database manager not available"}), 500
        
        with db_mgr.engine.connect() as conn:
            from sqlalchemy import text
            
            # Test query performance
            start_time = time.time()
            
            perf_query = text("""
                SELECT 
                    department,
                    location_type,
                    category,
                    COUNT(*) as tx_count,
                    SUM(amount) as total_amount
                FROM sqlsap_transactions_processed
                WHERE department IS NOT NULL
                GROUP BY department, location_type, category
                ORDER BY total_amount DESC
            """)
            
            result = conn.execute(perf_query)
            performance_data = []
            
            for row in result:
                performance_data.append({
                    'department': row[0],
                    'location_type': row[1],
                    'category': row[2],
                    'transaction_count': row[3],
                    'total_amount': float(row[4]) if row[4] else 0
                })
            
            query_time = time.time() - start_time
        
        return jsonify({
            "performance": {
                "query_time_seconds": round(query_time, 4),
                "rows_processed": len(performance_data),
                "rating": "EXCELLENT" if query_time < 1 else "GOOD" if query_time < 3 else "NEEDS_OPTIMIZATION"
            },
            "sample_data": performance_data[:10],
            "api_version": "4.0.0",
            "azure_optimization": {
                "web_app": "app-sap-integration-api-h7hwc9fwaugghnce",
                "database_optimization": "Direct SQL with indexes",
                "caching": "Frontend views table"
            }
        })
        
    except Exception as e:
        logger.error(f"Performance stats error: {str(e)}")
        return jsonify({"error": str(e)}), 500

# =============================================================================
# AZURE-SPECIFIC ENDPOINTS
# =============================================================================

@app.route('/api/azure-info', methods=['GET'])
def azure_info():
    """Get Azure-specific deployment information"""
    return jsonify({
        "azure_web_app": {
            "name": "app-sap-integration-api-h7hwc9fwaugghnce",
            "url": "https://app-sap-integration-api-h7hwc9fwaugghnce.germanywestcentral-01.azurewebsites.net",
            "resource_group": "marketing_controlling",
            "subscription": "Marketing-Kosten-Analytics",
            "app_service_plan": "ASP-marketingcontrolling-944f",
            "region": "Germany West Central",
            "operating_system": "Linux"
        },
        "github_integration": {
            "repository": "https://github.com/sadu619/Finanzen",
            "auto_deploy": "Enabled via GitHub Actions",
            "workflow_file": ".github/workflows/api-deploy.yml"
        },
        "database_integration": {
            "azure_function": "msp_sap_integration",
            "shared_database": "Azure SQL Database",
            "processing_sync": "Direct import from function-app logic"
        },
        "endpoints_overview": {
            "health_check": "/api/health",
            "data_processing": "/api/process",
            "transactions": "/api/transactions",
            "departments": "/api/departments",
            "outliers": "/api/outliers",
            "dashboard_data": "/api/data",
            "budget_management": "/api/budget-allocation"
        },
        "deployment_info": {
            "last_updated": datetime.now().isoformat(),
            "version": "4.0.0",
            "python_version": "3.11",
            "startup_command": "gunicorn --bind=0.0.0.0:8000 --timeout 600 --workers 1 app:app"
        }
    })

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
            "/api/transactions",
            "/api/departments",
            "/api/outliers",
            "/api/data",
            "/api/budget-allocation",
            "/api/database-status",
            "/api/azure-info"
        ],
        "azure_web_app": "app-sap-integration-api-h7hwc9fwaugghnce.germanywestcentral-01.azurewebsites.net"
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "status": "error",
        "message": "Internal server error",
        "troubleshooting": [
            "Check Azure Web App logs",
            "Verify DB_PASSWORD environment variable",
            "Ensure database connectivity",
            "Check GitHub Actions deployment status"
        ],
        "timestamp": datetime.now().isoformat()
    }), 500

# =============================================================================
# STARTUP INFORMATION
# =============================================================================

if __name__ == '__main__':
    # Startup checks and information
    print("\n" + "="*70)
    print("🚀 SAP INTEGRATION API v4.0.0")
    print("="*70)
    print(f"🌐 Azure Web App: app-sap-integration-api-h7hwc9fwaugghnce")
    print(f"🏢 Resource Group: marketing_controlling") 
    print(f"📊 Database: {DB_SERVER}")
    print(f"🗄️  Database: {DB_NAME}")
    print(f"🔗 GitHub: https://github.com/sadu619/Finanzen")
    print("\n🔍 Key Endpoints:")
    print("   - GET  /api/health            - System health")  
    print("   - POST /api/process           - Trigger processing")
    print("   - GET  /api/transactions      - Get transactions")
    print("   - GET  /api/departments       - Department summary")
    print("   - GET  /api/outliers          - Outlier analysis")
    print("   - GET  /api/data              - Dashboard data")
    print("   - GET  /api/azure-info        - Azure deployment info")
    print("="*70)
    
    # Environment checks
    if not os.getenv("DB_PASSWORD"):
        print("⚠️  WARNING: DB_PASSWORD not set in Azure Web App settings!")
        print("   Configure via: az webapp config appsettings set ...")
    else:
        print("✅ Database password configured")
    
    # Import checks
    try:
        # Test database manager
        test_db = get_db_manager()
        if test_db:
            print("✅ DatabaseManager imported successfully")
        else:
            print("⚠️  DatabaseManager import failed")
    except Exception as e:
        print(f"⚠️  Database setup issue: {e}")
    
    print(f"\n🚀 Starting Flask server on port 8000...")
    print("🌐 URL: https://app-sap-integration-api-h7hwc9fwaugghnce.germanywestcentral-01.azurewebsites.net")
    print("="*70 + "\n")
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=8000, debug=False)  # Debug=False for production