import azure.functions as func
import json
import logging
import os
import pyodbc
from datetime import datetime
from typing import Dict, List, Any
import traceback

# Database connection parameters (gleiche wie im Processing Code)
DB_SERVER = "sql-sap-prod-v2.database.windows.net"
DB_NAME = "sap-integration-db-v2"
DB_USER = "sqladmin"
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("sap_api_interface")

def get_db_connection():
    """Create database connection"""
    try:
        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={DB_SERVER};"
            f"DATABASE={DB_NAME};"
            f"UID={DB_USER};"
            f"PWD={DB_PASSWORD};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
            f"Connection Timeout=30;"
        )
        
        conn = pyodbc.connect(connection_string)
        return conn
        
    except Exception as e:
        logger.error(f"❌ Database connection failed: {str(e)}")
        raise

def validate_transaction_data(transaction: Dict[str, Any], transaction_type: str) -> Dict[str, Any]:
    """Validate and clean transaction data"""
    
    if transaction_type == "FAGLL03":
        # Required fields für FAGLL03
        required_fields = ["belegnummer", "betrag_in_hauswaehrung"]
        
        # Check required fields
        for field in required_fields:
            if field not in transaction or not transaction[field]:
                raise ValueError(f"Required field missing: {field}")
        
        # Clean and validate data
        cleaned = {
            'buchungskreis': str(transaction.get('buchungskreis', '')).strip() or None,
            'hauptbuchkonto': str(transaction.get('hauptbuchkonto', '')).strip() or None,
            'geschaeftsjahr': int(transaction['geschaeftsjahr']) if transaction.get('geschaeftsjahr') else None,
            'buchungsperiode': int(transaction['buchungsperiode']) if transaction.get('buchungsperiode') else None,
            'belegart': str(transaction.get('belegart', '')).strip() or None,
            'belegnummer': str(transaction['belegnummer']).strip(),
            'buchungsdatum': transaction.get('buchungsdatum'),
            'belegdatum': transaction.get('belegdatum'),
            'text_field': str(transaction.get('text', '')).strip() or None,
            'soll_haben_kennz': str(transaction.get('soll_haben_kennz', '')).strip() or None,
            'buchungsschluessel': str(transaction.get('buchungsschluessel', '')).strip() or None,
            'betrag_in_hauswaehrung': float(transaction['betrag_in_hauswaehrung']),
            'kostenstelle': str(transaction.get('kostenstelle', '')).strip() or None,
            'auftrag': str(transaction.get('auftrag', '')).strip() or None,
            'psp_element': str(transaction.get('psp_element', '')).strip() or None,
            'einkaufsbeleg': str(transaction.get('einkaufsbeleg', '')).strip() or None,
            'steuerkennzeichen': str(transaction.get('steuerkennzeichen', '')).strip() or None,
            'geschaeftsbereich': str(transaction.get('geschaeftsbereich', '')).strip() or None,
            'ausgleichsbeleg': str(transaction.get('ausgleichsbeleg', '')).strip() or None,
            'konto_gegenbuchung': str(transaction.get('konto_gegenbuchung', '')).strip() or None,
            'material': str(transaction.get('material', '')).strip() or None
        }
        
        return cleaned
    
    else:
        raise ValueError(f"Unsupported transaction type: {transaction_type}")

def save_transactions_to_db(transactions: List[Dict[str, Any]], batch_id: str, transaction_type: str) -> int:
    """Save transactions to sap_transactions table"""
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        success_count = 0
        upload_date = datetime.now()
        
        # Insert query for sap_transactions table
        insert_query = """
            INSERT INTO sap_transactions (
                buchungskreis, hauptbuchkonto, geschaeftsjahr, buchungsperiode, belegart,
                belegnummer, buchungsdatum, belegdatum, text_field, soll_haben_kennz,
                buchungsschluessel, betrag_in_hauswaehrung, kostenstelle, auftrag, psp_element,
                einkaufsbeleg, steuerkennzeichen, geschaeftsbereich, ausgleichsbeleg,
                konto_gegenbuchung, material, batch_id, upload_date, source_system
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """
        
        for transaction in transactions:
            try:
                # Validate transaction data
                cleaned_tx = validate_transaction_data(transaction, transaction_type)
                
                # Prepare values for database
                values = (
                    cleaned_tx.get('buchungskreis'),
                    cleaned_tx.get('hauptbuchkonto'),
                    cleaned_tx.get('geschaeftsjahr'),
                    cleaned_tx.get('buchungsperiode'),
                    cleaned_tx.get('belegart'),
                    cleaned_tx.get('belegnummer'),
                    cleaned_tx.get('buchungsdatum'),
                    cleaned_tx.get('belegdatum'),
                    cleaned_tx.get('text_field'),
                    cleaned_tx.get('soll_haben_kennz'),
                    cleaned_tx.get('buchungsschluessel'),
                    cleaned_tx.get('betrag_in_hauswaehrung'),
                    cleaned_tx.get('kostenstelle'),
                    cleaned_tx.get('auftrag'),
                    cleaned_tx.get('psp_element'),
                    cleaned_tx.get('einkaufsbeleg'),
                    cleaned_tx.get('steuerkennzeichen'),
                    cleaned_tx.get('geschaeftsbereich'),
                    cleaned_tx.get('ausgleichsbeleg'),
                    cleaned_tx.get('konto_gegenbuchung'),
                    cleaned_tx.get('material'),
                    batch_id,
                    upload_date,
                    f'SAP_API_{transaction_type}'
                )
                
                cursor.execute(insert_query, values)
                success_count += 1
                
            except Exception as row_error:
                logger.warning(f"⚠️ Skipped invalid transaction: {str(row_error)}")
                logger.warning(f"🔍 Transaction data: {cleaned_tx}")
                logger.warning(f"🔍 Values being inserted: {values}")
                continue
        
        # Commit all transactions
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Successfully saved {success_count}/{len(transactions)} transactions")
        return success_count
        
    except Exception as e:
        logger.error(f"❌ Error saving transactions: {str(e)}")
        if 'conn' in locals():
            conn.close()
        raise

def main(req: func.HttpRequest) -> func.HttpResponse:
    """Main API function for SAP data upload"""
    
    logger.info('🚀 SAP API called at: %s', datetime.now())
    
    try:
        # Check if request has JSON content
        if not req.get_json():
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "message": "No JSON data provided",
                    "timestamp": datetime.now().isoformat()
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )
        
        # Parse request data
        request_data = req.get_json()
        
        # Validate required fields
        required_fields = ["transaction_type", "batch_id", "transactions"]
        for field in required_fields:
            if field not in request_data:
                return func.HttpResponse(
                    json.dumps({
                        "status": "error",
                        "message": f"Required field missing: {field}",
                        "timestamp": datetime.now().isoformat()
                    }),
                    status_code=400,
                    headers={"Content-Type": "application/json"}
                )
        
        transaction_type = request_data["transaction_type"]
        batch_id = request_data["batch_id"]
        transactions = request_data["transactions"]
        
        # Validate transaction type
        if transaction_type not in ["FAGLL03"]:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "message": f"Unsupported transaction type: {transaction_type}. Supported: FAGLL03",
                    "timestamp": datetime.now().isoformat()
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )
        
        # Validate transactions data
        if not isinstance(transactions, list) or len(transactions) == 0:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "message": "Transactions must be a non-empty array",
                    "timestamp": datetime.now().isoformat()
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )
        
        # Check batch size (limit to 1000 transactions per call)
        if len(transactions) > 1000:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "message": f"Too many transactions in batch: {len(transactions)}. Maximum: 1000",
                    "timestamp": datetime.now().isoformat()
                }),
                status_code=400,
                headers={"Content-Type": "application/json"}
            )
        
        logger.info(f"📊 Processing {len(transactions)} {transaction_type} transactions for batch {batch_id}")
        
        # Save transactions to database
        success_count = save_transactions_to_db(transactions, batch_id, transaction_type)
        
        # Return success response
        response_data = {
            "status": "success",
            "message": "Transactions processed successfully",
            "details": {
                "transaction_type": transaction_type,
                "batch_id": batch_id,
                "total_received": len(transactions),
                "successfully_saved": success_count,
                "failed": len(transactions) - success_count
            },
            "timestamp": datetime.now().isoformat()
        }
        
        logger.info(f"🎉 API call completed successfully: {success_count} transactions saved")
        
        return func.HttpResponse(
            json.dumps(response_data),
            status_code=200,
            headers={"Content-Type": "application/json"}
        )
    
    except Exception as e:
        logger.error(f"❌ Fatal error in SAP API: {str(e)}")
        logger.error(f"🔍 Traceback: {traceback.format_exc()}")
        
        return func.HttpResponse(
            json.dumps({
                "status": "error",
                "message": f"Internal server error: {str(e)}",
                "timestamp": datetime.now().isoformat()
            }),
            status_code=500,
            headers={"Content-Type": "application/json"}
        )