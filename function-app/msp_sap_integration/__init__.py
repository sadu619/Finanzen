import logging
import azure.functions as func
import os

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🚀 Running FULL SAP processing with extended fields!")
    
    try:
        # Import new SAP code
        from . import msp_sap_integration_fixed
        logging.info("✅ New SAP code imported!")
        
        # Run the complete processing with all extended SAP fields
        logging.info("⚡ Starting complete SAP processing...")
        result = msp_sap_integration_fixed.main()
        
        logging.info("🎉 SAP processing completed successfully!")
        
        return func.HttpResponse(
            f"✅ SUCCESS: {result.get('message', 'Processing completed')} | "
            f"Transactions: {result.get('details', {}).get('transactions_saved', 'N/A')} | "
            f"Time: {result.get('processing_time', 'N/A'):.1f}s | "
            f"Batch: {result.get('details', {}).get('batch_id', 'N/A')}",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"❌ SAP processing failed: {str(e)}")
        return func.HttpResponse(
            f"❌ Processing Error: {str(e)}",
            status_code=500
        )