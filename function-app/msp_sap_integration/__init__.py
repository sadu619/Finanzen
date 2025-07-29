import azure.functions as func
import logging
import json
from datetime import datetime
from .msp_sap_integration_fixed import main

def main_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP-triggered Azure Function für manuelles SAP Processing"""
    
    logging.info('🌐 HTTP triggered SAP processing started at: %s', datetime.now())
    
    try:
        result = main()
        
        if result.get('status') == 'success':
            transactions_saved = result.get('details', {}).get('transactions_saved', 0)
            
            if transactions_saved > 0:
                response_data = {
                    "status": "success",
                    "message": f"Processing completed - {transactions_saved} transactions processed",
                    "data": result,
                    "timestamp": datetime.now().isoformat()
                }
                logging.info(f'🎉 HTTP processing completed: {transactions_saved} transactions')
            else:
                response_data = {
                    "status": "success", 
                    "message": "No new transactions found - system is up to date",
                    "data": result,
                    "timestamp": datetime.now().isoformat()
                }
        else:
            response_data = {
                "status": "error",
                "message": result.get("message", "Processing failed"),
                "data": result,
                "timestamp": datetime.now().isoformat()
            }
        
        return func.HttpResponse(
            json.dumps(response_data, indent=2),
            status_code=200 if result.get('status') == 'success' else 500,
            headers={
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            }
        )
    
    except Exception as e:
        error_response = {
            "status": "error",
            "message": f"Fatal error: {str(e)}",
            "timestamp": datetime.now().isoformat()
        }
        
        return func.HttpResponse(
            json.dumps(error_response, indent=2),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )