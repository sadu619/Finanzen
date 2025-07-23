import logging
import azure.functions as func
import json
from . import msp_sap_integration_fixed

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🧪 HTTP trigger function - Database connection test")

    try:
        # Call the minimal test function
        result = msp_sap_integration_fixed.main()
        
        # Return result as JSON
        return func.HttpResponse(
            json.dumps(result, indent=2),
            status_code=200,
            mimetype="application/json"
        )
        
    except Exception as e:
        logging.error(f"💥 Test failed: {str(e)}")
        return func.HttpResponse(
            json.dumps({
                "status": "error", 
                "message": f"Function error: {str(e)}"
            }, indent=2),
            status_code=500,
            mimetype="application/json"
        )