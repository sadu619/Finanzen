import requests
import json
import os

# Test-Daten mit allen Feldern aus den tatsächlichen Daten
test_data = {
    "transaction_type": "FAGLL03",
    "batch_id": "TEST_BATCH_002",  # Unique batch
    "transactions": [
        {
            "buchungskreis": "001",  # Angepasst von "1000" auf "001"
            "hauptbuchkonto": "7108000",
            "geschaeftsjahr": 2025,
            "buchungsperiode": 1,  # Angepasst von 8 auf 1
            "belegart": "KF",  # Angepasst von "DR" auf "KF"
            "belegnummer": f"DEBUG_{int(__import__('time').time())}",  # Unique
            "buchungsdatum": "2025-01-01",  # Angepasst
            "belegdatum": "2024-12-16",  # Angepasst
            "text": "DEBUG Test Transaktion - Giessen 46ers Ticket",  # Angepasst
            "soll_haben_kennz": "S",
            "buchungsschluessel": "40",
            "betrag_in_hauswaehrung": 777.77,  # Angepasst von 999.99
            "kostenstelle": "10061000",  # Angepasst von "10001234"
            
            # Zusätzliche Felder aus den tatsächlichen Daten
            "auftrag": "-",
            "psp_element": "-",
            "einkaufsbeleg": "-",
            "steuerkennzeichen": "N0",
            "geschaeftsbereich": "-",
            "ausgleichsbeleg": "-",
            "konto_gegenbuchung": "831044",
            "material": "-"
        }
    ]
}

API_KEY = os.getenv("API_KEY", "KEY_FEHLT")

print(f"🔍 DEBUG INFO:")
print(f"   API Key: {API_KEY[:10]}...")
print(f"   Batch ID: {test_data['batch_id']}")
print(f"   Belegnummer: {test_data['transactions'][0]['belegnummer']}")
print(f"   Buchungskreis: {test_data['transactions'][0]['buchungskreis']}")
print(f"   Belegart: {test_data['transactions'][0]['belegart']}")
print(f"   Kostenstelle: {test_data['transactions'][0]['kostenstelle']}")
print()

# API-Test mit erweiterten Headers für Debugging
response = requests.post(
    "https://func-sap-processing-ffd6h6ghdkf0f0dw.germanywestcentral-01.azurewebsites.net/api/sap-upload",
    json=test_data,
    headers={
        "x-functions-key": API_KEY,
        "Content-Type": "application/json",
        "User-Agent": "DEBUG-Test-Client"
    },
    timeout=60
)

print(f"📊 RESPONSE ANALYSIS:")
print(f"   Status Code: {response.status_code}")
print(f"   Response Headers: {dict(response.headers)}")
print(f"   Response Size: {len(response.text)} chars")
print()

try:
    response_json = response.json()
    print(f"📋 DETAILED RESPONSE:")
    print(json.dumps(response_json, indent=2))
    
    # Spezifische Analyse
    if response_json.get('status') == 'success':
        details = response_json.get('details', {})
        total = details.get('total_received', 0)
        saved = details.get('successfully_saved', 0)
        failed = details.get('failed', 0)
        
        print(f"\n🎯 TRANSACTION ANALYSIS:")
        print(f"   Total Received: {total}")
        print(f"   Successfully Saved: {saved}")
        print(f"   Failed: {failed}")
        
        if failed > 0:
            print(f"   ❌ PROBLEM: {failed} transactions failed to save!")
            print(f"   → Check Azure Function logs for SQL errors")
            print(f"   → Possible issues: constraints, permissions, data types")
        else:
            print(f"   ✅ SUCCESS: All transactions saved!")
            
except json.JSONDecodeError:
    print(f"❌ RESPONSE IS NOT JSON:")
    print(f"Raw response: {response.text}")
    
except Exception as e:
    print(f"❌ ERROR PARSING RESPONSE: {e}")

# Zusätzlicher Database-Permission Test
print(f"\n🔍 SUGGESTED CHECKS:")
print(f"1. Check Azure Function logs in portal")
print(f"2. Verify SQL user permissions:")
print(f"   SELECT HAS_PERMS_BY_NAME('sap_transactions', 'OBJECT', 'INSERT')")
print(f"3. Test manual insert with new fields:")
print(f"   INSERT INTO sap_transactions (belegnummer, batch_id, buchungskreis, belegart, auftrag, steuerkennzeichen) VALUES ('MANUAL_TEST', 'MANUAL_BATCH', '001', 'KF', '-', 'N0')")
print(f"4. Check table constraints:")
print(f"   SELECT * FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_NAME = 'sap_transactions'")
print(f"5. Check if database table has columns for all new fields:")
print(f"   SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'sap_transactions'")

print(f"\n📝 FIELD MAPPING SUMMARY:")
print(f"   Added fields: auftrag, psp_element, einkaufsbeleg, steuerkennzeichen,")
print(f"                geschaeftsbereich, ausgleichsbeleg, konto_gegenbuchung, material")
print(f"   Updated values: buchungskreis (001), belegart (KF), kostenstelle (33010100)")
print(f"   Note: Fields with '-' indicate empty/null values in original data")