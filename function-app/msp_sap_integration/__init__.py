import logging
import azure.functions as func
import os

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🧪 Testing new SAP code import")
    
    try:
        # Test all dependencies first
        import pandas as pd
        import numpy as np
        import pyodbc
        from sqlalchemy import create_engine
        logging.info("✅ Dependencies loaded!")
        
        # Test import of your new SAP code
        logging.info("🔍 Attempting to import msp_sap_integration_fixed...")
        from . import msp_sap_integration_fixed
        logging.info("✅ New SAP code import successful!")
        
        # Test if main function exists
        if hasattr(msp_sap_integration_fixed, 'main'):
            logging.info("✅ main() function found in new code!")
        else:
            logging.warning("⚠️ main() function not found in new code")
        
        return func.HttpResponse(
            "✅ SUCCESS: New SAP code can be imported and is ready!",
            status_code=200
        )
        
    except ImportError as e:
        logging.error(f"❌ Import failed: {str(e)}")
        return func.HttpResponse(
            f"❌ Import Error: {str(e)}",
            status_code=500
        )
    except Exception as e:
        logging.error(f"❌ Other error: {str(e)}")
        return func.HttpResponse(
            f"❌ Error: {str(e)}",
            status_code=500
        )