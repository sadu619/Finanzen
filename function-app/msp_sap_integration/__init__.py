import logging
import azure.functions as func
import os

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🧪 Testing database connection with new SAP code")
    
    try:
        # Test dependencies
        import pandas as pd
        import pyodbc
        from sqlalchemy import create_engine
        logging.info("✅ Dependencies loaded!")
        
        # Import new SAP code
        from . import msp_sap_integration_fixed
        logging.info("✅ New SAP code imported!")
        
        # Test database connection with NEW code
        logging.info("🔍 Testing database connection with new DatabaseManager...")
        db_connection_works = msp_sap_integration_fixed.db_manager.test_connection()
        
        if db_connection_works:
            logging.info("✅ NEW database connection successful!")
            
            return func.HttpResponse(
                "✅ SUCCESS: New SAP code + Database connection both work!",
                status_code=200
            )
        else:
            logging.error("❌ NEW database connection failed")
            return func.HttpResponse(
                "❌ Database connection failed with new code",
                status_code=500
            )
        
    except Exception as e:
        logging.error(f"❌ Error testing new database connection: {str(e)}")
        return func.HttpResponse(
            f"❌ Database Test Error: {str(e)}",
            status_code=500
        )