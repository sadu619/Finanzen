import logging
import azure.functions as func
from . import msp_sap_integration_fixed

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("HTTP trigger function received a request.")

    try:
        msp_sap_integration_fixed.main()
        return func.HttpResponse("Data processed successfully.", status_code=200)
    except Exception as e:
        logging.error(f"Processing failed: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)