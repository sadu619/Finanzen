import logging
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("HTTP trigger function received a request.")

    try:
        # Temporär: Keine Database Connection - nur Test
        logging.info("✅ Function is running successfully!")
        
        return func.HttpResponse(
            "✅ Function is working! Database connection will be added next.", 
            status_code=200
        )
    except Exception as e:
        logging.error(f"Processing failed: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)