# test_api.py - Simpelster Azure Test
from fastapi import FastAPI
import os
from datetime import datetime

# Super einfache API
app = FastAPI(title="Azure Connection Test")

@app.get("/")
def root():
    return {
        "message": "🧪 Azure Test API is running!",
        "timestamp": datetime.now().isoformat(),
        "environment": "Azure" if os.getenv("WEBSITE_SITE_NAME") else "Local",
        "status": "✅ Working"
    }

@app.get("/test-azure")
def test_azure():
    return {
        "azure_test": "success",
        "app_service": os.getenv("WEBSITE_SITE_NAME", "not_deployed"),
        "port": os.getenv("PORT", "8000"),
        "message": "If you see this, Azure App Service works!"
    }

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)