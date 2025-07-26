# Erstellen Sie diese neue Datei: api/database_manager_azure.py

import os
import logging
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

logger = logging.getLogger("azure_db_manager")

class AzureDatabaseManager:
    """Azure-kompatible Database Manager ohne ODBC Abhängigkeiten"""
    
    def __init__(self):
        self.engine = None
        self._setup_connection()
    
    def _setup_connection(self):
        """Setup Azure SQL connection using pymssql (kein ODBC erforderlich)"""
        try:
            # Environment variables
            server = os.getenv("DB_SERVER", "sql-sap-prod-v2.database.windows.net")
            database = os.getenv("DB_NAME", "sap-integration-db-v2")
            username = os.getenv("DB_USER", "sqladmin")
            password = os.getenv("DB_PASSWORD")
            
            if not password:
                raise ValueError("DB_PASSWORD environment variable not set")
            
            # Verschiedene Connection String Formate für Azure probieren
            connection_strings = [
                # Format 1: pymssql (funktioniert in Azure Linux)
                f"mssql+pymssql://{username}:{quote_plus(password)}@{server}/{database}?charset=utf8",
                
                # Format 2: pyodbc mit verfügbaren Treibern
                f"mssql+pyodbc://{username}:{quote_plus(password)}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes",
                
                # Format 3: pyodbc mit SQL Server Native Client
                f"mssql+pyodbc://{username}:{quote_plus(password)}@{server}/{database}?driver=SQL+Server&Encrypt=yes&TrustServerCertificate=yes"
            ]
            
            # Probiere Connection Strings nacheinander
            last_error = None
            for i, conn_str in enumerate(connection_strings):
                try:
                    logger.info(f"Trying connection format {i+1}...")
                    self.engine = create_engine(conn_str, echo=False)
                    
                    # Test connection
                    with self.engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                    
                    logger.info(f"✅ Database connection successful with format {i+1}")
                    return
                    
                except Exception as e:
                    last_error = e
                    logger.warning(f"⚠️ Connection format {i+1} failed: {str(e)}")
                    continue
            
            # Wenn alle fehlschlagen
            raise Exception(f"All connection formats failed. Last error: {last_error}")
            
        except Exception as e:
            logger.error(f"❌ Database connection setup failed: {str(e)}")
            self.engine = None
            raise
    
    def test_connection(self):
        """Test database connection"""
        try:
            if not self.engine:
                return False
                
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test")).fetchone()
                logger.info("✅ Database connection test successful")
                return True
                
        except Exception as e:
            logger.error(f"❌ Database connection test failed: {str(e)}")
            return False
    
    def execute_query(self, query, params=None):
        """Execute a SQL query"""
        try:
            with self.engine.connect() as conn:
                if params:
                    result = conn.execute(text(query), params)
                else:
                    result = conn.execute(text(query))
                return result
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise