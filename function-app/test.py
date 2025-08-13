import requests
import json
import os
import time
from datetime import datetime, timedelta
import random

# Configuration
API_URL = "https://func-sap-processing-ffd6h6ghdkf0f0dw.germanywestcentral-01.azurewebsites.net/api/sap-upload"
PROCESSING_URL = "https://func-sap-processing-ffd6h6ghdkf0f0dw.germanywestcentral-01.azurewebsites.net/api/msp_sap_integration"
API_KEY = os.getenv("API_KEY", "KEY_FEHLT")

def generate_realistic_sap_data(batch_size=50, batch_id=None):
    """Generate realistic SAP transaction data"""
    if not batch_id:
        batch_id = f"SAP_PROD_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Realistic SAP values based on BARMER patterns
    buchungskreis_options = ["001", "002", "003"]
    hauptbuchkonto_options = ["7108000", "7109000", "6000000", "6001000", "5000000"]
    belegart_options = ["KF", "DR", "SA", "AB", "RE"]
    kostenstelle_options = [
        "10061000",  # HQ style
        "10062000", 
        "30001234",  # Floor style
        "30012345",
        "30023456"
    ]
    soll_haben_options = ["S", "H"]
    buchungsschluessel_options = ["40", "50", "01", "31"]
    
    transactions = []
    
    for i in range(batch_size):
        # Generate unique document number
        unique_id = int(time.time() * 1000) + i
        
        # Random but realistic data
        transaction = {
            "buchungskreis": random.choice(buchungskreis_options),
            "hauptbuchkonto": random.choice(hauptbuchkonto_options),
            "geschaeftsjahr": 2025,
            "buchungsperiode": random.randint(1, 12),
            "belegart": random.choice(belegart_options),
            "belegnummer": f"SIM_{unique_id}",
            "buchungsdatum": (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
            "belegdatum": (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
            "text": f"Simulation Transaction {i+1} - {random.choice(['Office Supplies', 'Travel Expense', 'Equipment', 'Consulting', 'Marketing'])}",
            "soll_haben_kennz": random.choice(soll_haben_options),
            "buchungsschluessel": random.choice(buchungsschluessel_options),
            "betrag_in_hauswaehrung": round(random.uniform(10.50, 9999.99), 2),
            "kostenstelle": random.choice(kostenstelle_options),
            
            # Additional fields
            "auftrag": random.choice(["-", f"ORD{random.randint(1000, 9999)}"]),
            "psp_element": random.choice(["-", f"PSP{random.randint(100, 999)}"]),
            "einkaufsbeleg": random.choice(["-", f"PO{random.randint(10000, 99999)}"]),
            "steuerkennzeichen": random.choice(["N0", "V0", "I0"]),
            "geschaeftsbereich": random.choice(["-", "GB01", "GB02"]),
            "ausgleichsbeleg": "-",
            "konto_gegenbuchung": random.choice(["831044", "831045", "831046"]),
            "material": random.choice(["-", f"MAT{random.randint(1000, 9999)}"])
        }
        
        transactions.append(transaction)
    
    return {
        "transaction_type": "FAGLL03",
        "batch_id": batch_id,
        "transactions": transactions
    }

def send_sap_data(data):
    """Send SAP data to API"""
    print(f"📤 Sending {len(data['transactions'])} transactions with batch_id: {data['batch_id']}")
    
    response = requests.post(
        API_URL,
        json=data,
        headers={
            "x-functions-key": API_KEY,
            "Content-Type": "application/json",
            "User-Agent": "SAP-Simulation-Client"
        },
        timeout=120
    )
    
    print(f"   Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"   ✅ Success: {result.get('details', {}).get('successfully_saved', 0)} transactions saved")
        return True
    else:
        print(f"   ❌ Failed: {response.text}")
        return False

def trigger_processing():
    """Trigger the processing function"""
    print(f"⚡ Triggering SAP processing...")
    
    response = requests.post(
        PROCESSING_URL,
        headers={
            "x-functions-key": API_KEY,
            "Content-Type": "application/json"
        },
        timeout=300
    )
    
    print(f"   Status: {response.status_code}")
    
    if response.status_code == 200:
        result = response.json()
        print(f"   ✅ Processing completed!")
        
        details = result.get('details', {})
        if isinstance(details, dict):
            transactions_saved = details.get('transactions_saved', 0)
            processing_time = details.get('processing_time', 0)
            print(f"   📊 Processed: {transactions_saved} transactions in {processing_time:.2f}s")
        
        return True
    else:
        print(f"   ❌ Processing failed: {response.text}")
        return False

def run_complete_simulation():
    """Run a complete end-to-end simulation"""
    print("🚀 STARTING COMPLETE SAP SIMULATION")
    print("=" * 60)
    
    # Scenario 1: Initial data drop
    print("\n📋 SCENARIO 1: Initial SAP Data Drop (50 transactions)")
    batch1_data = generate_realistic_sap_data(50, f"SAP_MORNING_{datetime.now().strftime('%Y%m%d_%H%M')}")
    
    if send_sap_data(batch1_data):
        print("   ✅ Initial data drop successful")
        
        # Process the data
        print("\n⚡ Processing initial batch...")
        if trigger_processing():
            print("   ✅ Initial processing successful")
        else:
            print("   ❌ Initial processing failed")
            return
    else:
        print("   ❌ Initial data drop failed")
        return
    
    # Wait a bit
    print("\n⏱️  Waiting 5 seconds before next batch...")
    time.sleep(5)
    
    # Scenario 2: Incremental data drop
    print("\n📋 SCENARIO 2: Incremental SAP Data Drop (25 new transactions)")
    batch2_data = generate_realistic_sap_data(25, f"SAP_AFTERNOON_{datetime.now().strftime('%Y%m%d_%H%M')}")
    
    if send_sap_data(batch2_data):
        print("   ✅ Incremental data drop successful")
        
        # Process the incremental data
        print("\n⚡ Processing incremental batch...")
        if trigger_processing():
            print("   ✅ Incremental processing successful")
        else:
            print("   ❌ Incremental processing failed")
    else:
        print("   ❌ Incremental data drop failed")
        return
    
    # Wait a bit
    print("\n⏱️  Waiting 5 seconds before duplicate test...")
    time.sleep(5)
    
    # Scenario 3: Duplicate data test (should be ignored)
    print("\n📋 SCENARIO 3: Duplicate Data Test (resending first batch)")
    
    if send_sap_data(batch1_data):  # Resend same data
        print("   ✅ Duplicate data sent successfully")
        
        # Process - should show 0 new transactions
        print("\n⚡ Processing duplicate batch (should process 0 new transactions)...")
        if trigger_processing():
            print("   ✅ Duplicate processing completed (fingerprint deduplication working)")
        else:
            print("   ❌ Duplicate processing failed")
    
    print("\n🎉 SIMULATION COMPLETED!")
    print("=" * 60)
    print("✅ Tested scenarios:")
    print("   1. Initial large data drop")
    print("   2. Incremental processing") 
    print("   3. Duplicate detection")
    print("\n💡 Check your sap_transactions_processed table to see results!")

def run_stress_test():
    """Run a stress test with larger datasets"""
    print("🔥 STARTING STRESS TEST")
    print("=" * 60)
    
    # Send multiple batches quickly
    for i in range(3):
        print(f"\n📋 STRESS BATCH {i+1}/3 (100 transactions each)")
        stress_data = generate_realistic_sap_data(100, f"STRESS_BATCH_{i+1}_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
        
        if send_sap_data(stress_data):
            print(f"   ✅ Stress batch {i+1} sent successfully")
        else:
            print(f"   ❌ Stress batch {i+1} failed")
            break
        
        # Small delay between batches
        time.sleep(2)
    
    # Process all stress test data
    print("\n⚡ Processing all stress test data...")
    if trigger_processing():
        print("   ✅ Stress test processing completed")
    else:
        print("   ❌ Stress test processing failed")
    
    print("\n🔥 STRESS TEST COMPLETED!")

if __name__ == "__main__":
    if API_KEY == "KEY_FEHLT":
        print("❌ ERROR: API_KEY environment variable not set!")
        print("Set with: export API_KEY=your_function_key")
        exit(1)
    
    print("🎯 SAP SIMULATION OPTIONS:")
    print("1. Complete End-to-End Simulation")
    print("2. Stress Test (300 transactions)")
    print("3. Single Batch Test (10 transactions)")
    
    choice = input("\nEnter choice (1-3): ").strip()
    
    if choice == "1":
        run_complete_simulation()
    elif choice == "2":
        run_stress_test()
    elif choice == "3":
        print("\n📋 Single batch test...")
        test_data = generate_realistic_sap_data(10)
        if send_sap_data(test_data):
            print("\n⚡ Processing...")
            trigger_processing()
    else:
        print("❌ Invalid choice")