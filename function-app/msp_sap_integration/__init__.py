import azure.functions as func
import logging
import json
from datetime import datetime

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('🌐 HTTP triggered SAP processing started')
    
    try:
        from .msp_sap_integration_fixed import main as sap_main
        result = sap_main()
        
        return func.HttpResponse(
            json.dumps(result, indent=2),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )
    
    except Exception as e:
        logging.error(f'💥 Error: {str(e)}')
        return func.HttpResponse(
            f"Error: {str(e)}", 
            status_code=500
        )