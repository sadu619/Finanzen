import logging
import azure.functions as func
import os

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🧪 HTTP trigger function - Debug mode")

    try:
        # 🔍 DEBUG: Check if environment variable exists
        db_password = os.getenv("DB_PASSWORD")
        logging.info(f"🔍 DB_PASSWORD exists: {db_password is not None}")
        logging.info(f"🔍 DB_PASSWORD length: {len(db_password) if db_password else 0}")
        
        if not db_password:
            return func.HttpResponse(
                "❌ ERROR: DB_PASSWORD environment variable is not set!",
                status_code=500
            )
        
        # Only try import if password exists
        from . import msp_sap_integration_fixed
        
        # Call the main function
        result = msp_sap_integration_fixed.main()
        
        # Return success message
        return func.HttpResponse(
            f"✅ SUCCESS: Database connection established! Message: {result.get('message', '')}",
            status_code=200
        )
        
    except ImportError as e:
        logging.error(f"💥 Import failed: {str(e)}")
        return func.HttpResponse(
            f"❌ IMPORT ERROR: {str(e)}",
            status_code=500
        )
    except Exception as e:
        logging.error(f"💥 Test failed: {str(e)}")
        return func.HttpResponse(
            f"❌ ERROR: {str(e)}",
            status_code=500
        )