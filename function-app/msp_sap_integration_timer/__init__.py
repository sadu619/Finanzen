import azure.functions as func
import logging
import sys
import os
import traceback
from datetime import datetime

# Import aus dem anderen Ordner
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'msp_sap_integration'))

try:
    from msp_sap_integration_fixed import main
except ImportError as e:
    logging.error(f"❌ Could not import main function: {str(e)}")
    raise

def main_timer(mytimer: func.TimerRequest) -> None:
    """Timer-triggered Azure Function für automatisches SAP Processing"""
    
    logging.info('🕐 Timer triggered SAP processing started at: %s', datetime.now())
    
    if mytimer.past_due:
        logging.warning('⚠️ Timer function was past due!')
    
    try:
        result = main()
        
        if result.get('status') == 'success':
            transactions_saved = result.get('details', {}).get('transactions_saved', 0)
            processing_time = result.get('processing_time', 0)
            
            if transactions_saved > 0:
                logging.info(f'🎉 Timer processing completed successfully!')
                logging.info(f'   📊 Transactions processed: {transactions_saved}')
                logging.info(f'   ⏱️ Processing time: {processing_time:.2f} seconds')
            else:
                logging.info('✅ Timer processing completed - no new transactions found')
        else:
            logging.error(f'❌ Timer processing failed: {result.get("message", "Unknown error")}')
    
    except Exception as e:
        logging.error(f'💥 Fatal error in timer processing: {str(e)}')
        logging.error(f'🔍 Traceback: {traceback.format_exc()}')
        raise