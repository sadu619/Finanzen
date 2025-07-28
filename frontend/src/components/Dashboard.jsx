// src/components/Dashboard.jsx
import React, { useState, useEffect } from 'react';
import apiService from '../services/apiService';

function Dashboard({ healthStatus }) {
  const [transactions, setTransactions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [processing, setProcessing] = useState(false);

  useEffect(() => {
    loadTransactions();
  }, []);

  const loadTransactions = async () => {
    try {
      setLoading(true);
      const data = await apiService.getTransactions(10);
      setTransactions(data.transactions || []);
    } catch (error) {
      console.error('Failed to load transactions:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleTriggerProcessing = async () => {
    try {
      setProcessing(true);
      const result = await apiService.triggerProcessing();
      
      if (result.status === 'success') {
        alert('Processing completed successfully!');
        // Reload transactions after processing
        await loadTransactions();
      } else {
        alert(`Processing failed: ${result.message}`);
      }
    } catch (error) {
      alert(`Processing error: ${error.message}`);
    } finally {
      setProcessing(false);
    }
  };

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <h2>📊 SAP Dashboard</h2>
        <button 
          onClick={handleTriggerProcessing}
          disabled={processing}
          className="process-button"
        >
          {processing ? '⏳ Processing...' : '🔄 Trigger Processing'}
        </button>
      </div>

      {/* Quick Stats */}
      <div className="stats-grid">
        <div className="stat-card">
          <h3>API Status</h3>
          <p className={`status ${healthStatus?.status}`}>
            {healthStatus?.status || 'Unknown'}
          </p>
        </div>
        
        <div className="stat-card">
          <h3>Database</h3>
          <p className={healthStatus?.database?.connected ? 'connected' : 'disconnected'}>
            {healthStatus?.database?.connected ? '✅ Connected' : '❌ Disconnected'}
          </p>
        </div>
        
        <div className="stat-card">
          <h3>Total Transactions</h3>
          <p>{healthStatus?.data?.total_transactions || 0}</p>
        </div>
        
        <div className="stat-card">
          <h3>Processing</h3>
          <p className={healthStatus?.processing?.available ? 'available' : 'unavailable'}>
            {healthStatus?.processing?.available ? '✅ Available' : '❌ Unavailable'}
          </p>
        </div>
      </div>

      {/* Recent Transactions */}
      <div className="transactions-section">
        <div className="section-header">
          <h3>Recent Transactions</h3>
          <button onClick={loadTransactions} className="refresh-button">
            🔄 Refresh
          </button>
        </div>
        
        {loading ? (
          <div className="loading">Loading transactions...</div>
        ) : transactions.length > 0 ? (
          <div className="transactions-table">
            <table>
              <thead>
                <tr>
                  <th>Transaction ID</th>
                  <th>Amount</th>
                  <th>Kostenstelle</th>
                  <th>Department</th>
                  <th>Category</th>
                  <th>Date</th>
                </tr>
              </thead>
              <tbody>
                {transactions.map((tx, index) => (
                  <tr key={tx.transaction_id || index}>
                    <td>{tx.transaction_id || 'N/A'}</td>
                    <td>€{parseFloat(tx.amount || 0).toLocaleString()}</td>
                    <td>{tx.kostenstelle || 'N/A'}</td>
                    <td>{tx.department || 'Unknown'}</td>
                    <td>
                      <span className={`category ${tx.category?.toLowerCase()}`}>
                        {tx.category || 'N/A'}
                      </span>
                    </td>
                    <td>{tx.booking_date || 'N/A'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="no-data">
            <p>No transactions found.</p>
            <p>Try triggering processing to generate data.</p>
          </div>
        )}
      </div>
    </div>
  );
}

export default Dashboard;