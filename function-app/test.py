import requests
import json

test_data = {
    "transaction_type": "FAGLL03",
    "batch_id": "TEST_DETAILED_20250811",
    "transactions": [
        {
            "belegnummer": "TEST002",
            "betrag_in_hauswaehrung": 2500.00,
            "kostenstelle": "10001234", 
            "hauptbuchkonto": "7108000",
            "buchungskreis": "1000",
            "text": "Vollständige Test-Transaktion",
            "buchungsdatum": "2025-08-11",
            "belegdatum": "2025-08-11",
            "geschaeftsjahr": 2025,
            "buchungsperiode": 8,
            "belegart": "DR",
            "soll_haben_kennz": "S",
            "buchungsschluessel": "40"
        }
    ]
}

# API-Key aus Environment Variable holen
import os
API_KEY = os.getenv("API_KEY", "IHREN_KEY_HIER_EINTRAGEN")

response = requests.post(
    "https://func-sap-processing-ffd6h6ghdkf0f0dw.germanywestcentral-01.azurewebsites.net/api/sap-upload",
    json=test_data,
    headers={
        "x-functions-key": API_KEY,  # ← Jetzt sicher!
        "Content-Type": "application/json"
    }
)

print(f"Status: {response.status_code}")
print(f"Response: {response.text}")