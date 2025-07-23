import logging
import pyodbc
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("database_test")

# Database connection parameters
DB_SERVER = "sql-sap-integration-prod.database.windows.net"
DB_NAME = "sap-integration-db"

def main():
    """
    🧪 MINIMAL TEST: Only test database connection
    """
    logger.info("🚀 Starting minimal database connection test...")
    
    try:
        # Step 1: Get authentication token
        logger.info("🔐 Getting authentication token...")
        
        try:
            # Try Managed Identity first (works in Azure Function)
            credential = ManagedIdentityCredential()
            token = credential.get_token("https://database.windows.net/.default")
            logger.info("✅ Got Managed Identity token")
        except Exception as e:
            # Fallback to Default credential
            logger.info(f"⚠️ Managed Identity failed: {str(e)}")
            credential = DefaultAzureCredential()
            token = credential.get_token("https://database.windows.net/.default")
            logger.info("✅ Got Default Azure credential token")
        
        # Step 2: Test different ODBC drivers
        drivers_to_test = [
            "ODBC Driver 18 for SQL Server",
            "ODBC Driver 17 for SQL Server", 
            "ODBC Driver 13 for SQL Server"
        ]
        
        connection_success = False
        
        for driver in drivers_to_test:
            try:
                logger.info(f"🔧 Testing driver: {driver}")
                
                # Build connection string
                connection_string = (
                    f"DRIVER={{{driver}}};"
                    f"SERVER={DB_SERVER};"
                    f"DATABASE={DB_NAME};"
                    f"Encrypt=yes;"
                )
                
                # Try to connect
                conn = pyodbc.connect(
                    connection_string,
                    attrs_before={
                        1256: token.token  # SQL_COPT_SS_ACCESS_TOKEN
                    }
                )
                
                # Test simple query
                cursor = conn.cursor()
                cursor.execute("SELECT 1 AS test, SUSER_NAME() AS current_user")
                result = cursor.fetchone()
                
                logger.info(f"✅ SUCCESS with {driver}!")
                logger.info(f"🔐 Connected as: {result[1]}")
                logger.info(f"📊 Test query result: {result[0]}")
                
                cursor.close()
                conn.close()
                
                connection_success = True
                break
                
            except Exception as e:
                logger.error(f"❌ Driver {driver} failed: {str(e)}")
                continue
        
        if connection_success:
            logger.info("🎉 Database connection test SUCCESSFUL!")
            return {"status": "success", "message": "Database connection works!"}
        else:
            logger.error("❌ All ODBC drivers failed!")
            return {"status": "error", "message": "No working ODBC driver found"}
            
    except Exception as e:
        logger.error(f"💥 Connection test failed: {str(e)}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    result = main()
    print(f"Result: {result}")