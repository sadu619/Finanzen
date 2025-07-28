// src/services/apiService.js
const API_BASE = 'https://app-sap-integration-api-h7hwc9fwaugghnce.germanywestcentral-01.azurewebsites.net';

class SAPApiService {
  
  // Health Check
  async getHealth() {
    try {
      const response = await fetch(`${API_BASE}/api/health`);
      return await response.json();
    } catch (error) {
      console.error('Health check failed:', error);
      return { status: 'error', message: error.message };
    }
  }
  
  // Get all transactions  
  async getTransactions(limit = 100) {
    try {
      const response = await fetch(`${API_BASE}/api/transactions-raw?limit=${limit}`);
      return await response.json();
    } catch (error) {
      console.error('Get transactions failed:', error);
      return { transactions: [], error: error.message };
    }
  }
  
  // Get database status
  async getDatabaseStatus() {
    try {
      const response = await fetch(`${API_BASE}/api/database-test`);
      return await response.json();
    } catch (error) {
      console.error('Database status failed:', error);
      return { connection_test: false, error: error.message };
    }
  }
  
  // Trigger processing
  async triggerProcessing() {
    try {
      const response = await fetch(`${API_BASE}/api/process`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        }
      });
      return await response.json();
    } catch (error) {
      console.error('Trigger processing failed:', error);
      return { status: 'error', message: error.message };
    }
  }
  
  // Get environment info (for debugging)
  async getEnvironment() {
    try {
      const response = await fetch(`${API_BASE}/api/environment`);
      return await response.json();
    } catch (error) {
      console.error('Get environment failed:', error);
      return { error: error.message };
    }
  }
}

export default new SAPApiService();