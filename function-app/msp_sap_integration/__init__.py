import logging
import azure.functions as func
import os

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("🧪 Testing dependencies from requirements.txt")
    
    try:
        # Test neue imports
        import pandas as pd
        import numpy as np
        import pyodbc
        from sqlalchemy import create_engine
        
        logging.info("✅ All dependencies loaded successfully!")
        
        return func.HttpResponse(
            "✅ All dependencies work! Ready for extended processing.",
            status_code=200
        )
        
    except Exception as e:
        logging.error(f"❌ Dependency error: {str(e)}")
        return func.HttpResponse(
            f"❌ Dependency Error: {str(e)}",
            status_code=500
        )