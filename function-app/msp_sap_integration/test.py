import pyodbc
import sys

# Database connection parameters
DB_SERVER = "sql-sap-prod-v2.database.windows.net"
DB_NAME = "sap-integration-db-v2"
DB_USER = "sqladmin"
DB_PASSWORD = "Leberwurst12345+"

def test_database_connection():
    try:
        print("🔌 Attempting to connect to database...")
        print(f"Server: {DB_SERVER}")
        print(f"Database: {DB_NAME}")
        print(f"User: {DB_USER}")
        
        # Build connection string
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
        
        # Try to connect
        connection = pyodbc.connect(connection_string)
        cursor = connection.cursor()
        
        # 🔧 ULTRA SIMPLE: Just select a number
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        
        print("✅ SUCCESS! Database connection works!")
        print(f"Test result: {result[0]}")
        
        # Test another simple query
        cursor.execute("SELECT GETDATE()")
        date_result = cursor.fetchone()
        print(f"Current server time: {date_result[0]}")
        
        # Close connection
        cursor.close()
        connection.close()
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: Database connection failed!")
        print(f"Error details: {str(e)}")
        return False

if __name__ == "__main__":
    print("🧪 Ultra Simple Database Connection Test")
    print("=" * 60)
    
    success = test_database_connection()
    
    if success:
        print("\n🎉 PERFECT! Database connection confirmed!")
        print("✅ Ready to update Function App code!")
        sys.exit(0)
    else:
        print("\n💥 Still having connection issues!")
        sys.exit(1)