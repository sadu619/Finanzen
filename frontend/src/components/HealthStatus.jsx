// src/components/HealthStatus.jsx
import React, { useState } from 'react';
import apiService from '../services/apiService';

function HealthStatus({ healthStatus, onRefresh }) {
  const [dbStatus, setDbStatus] = useState(null);
  const [loading, setLoading] = useState(false);

  const checkDatabaseStatus = async () => {
    try {
      setLoading(true);
      const status = await apiService.getDatabaseStatus();
      setDbStatus(status);
    } catch (error) {
      console.error('Database check failed:', error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="health-status">
      <div className="health-header">
        <h2>🏥 System Health</h2>
        <button onClick={onRefresh} className="refresh-button">
          🔄 Refresh Health
        </button>
      </div>

      {/* API Health */}
      <div className="health-section">
        <h3>API Health</h3>
        <div className="health-grid">
          <div className="health-item">
            <span className="label">Status:</span>
            <span className={`value status ${healthStatus?.status}`}>
              {healthStatus?.status || 'Unknown'}
            </span>
          </div>
          
          <div className="health-item">
            <span className="label">Version:</span>
            <span className="value">{healthStatus?.version || 'Unknown'}</span>
          </div>
          
          <div className="health-item">
            <span className="label">Database Connected:</span>
            <span className={`value ${healthStatus?.database?.connected ? 'connected' : 'disconnected'}`}>
              {healthStatus?.database?.connected ? '✅ Yes' : '❌ No'}
            </span>
          </div>
          
          <div className="health-item">
            <span className="label">Processing Available:</span>
            <span className={`value ${healthStatus?.processing?.available ? 'available' : 'unavailable'}`}>
              {healthStatus?.processing?.available ? '✅ Yes' : '❌ No'}
            </span>
          </div>
        </div>
      </div>

      {/* Database Health */}
      <div className="health-section">
        <div className="section-header">
          <h3>Database Health</h3>
          <button 
            onClick={checkDatabaseStatus}
            disabled={loading}
            className="test-button"
          >
            {loading ? '⏳ Testing...' : '🔬 Test Database'}
          </button>
        </div>
        
        {dbStatus && (
          <div className="health-grid">
            <div className="health-item">
              <span className="label">Connection Test:</span>
              <span className={`value ${dbStatus.connection_test ? 'connected' : 'disconnected'}`}>
                {dbStatus.connection_test ? '✅ Success' : '❌ Failed'}
              </span>
            </div>
            
            <div className="health-item">
              <span className="label">Manager Type:</span>
              <span className="value">{dbStatus.database_manager_type || 'Unknown'}</span>
            </div>
          </div>
        )}
        
        {dbStatus?.tables && (
          <div className="tables-status">
            <h4>Database Tables:</h4>
            <div className="tables-grid">
              {Object.entries(dbStatus.tables).map(([tableName, tableInfo]) => (
                <div key={tableName} className="table-item">
                  <span className="table-name">{tableName}</span>
                  <span className={`table-status ${tableInfo.exists ? 'exists' : 'missing'}`}>
                    {tableInfo.exists ? 
                      `✅ ${tableInfo.row_count} rows` : 
                      '❌ Missing'
                    }
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}
      </div>

      {/* Azure Info */}
      <div className="health-section">
        <h3>Azure Configuration</h3>
        <div className="health-grid">
          <div className="health-item">
            <span className="label">Web App:</span>
            <span className="value">{healthStatus?.azure_web_app?.name || 'Unknown'}</span>
          </div>
          
          <div className="health-item">
            <span className="label">Resource Group:</span>
            <span className="value">{healthStatus?.azure_web_app?.resource_group || 'Unknown'}</span>
          </div>
          
          <div className="health-item">
            <span className="label">Region:</span>
            <span className="value">{healthStatus?.azure_web_app?.region || 'Unknown'}</span>
          </div>
          
          <div className="health-item">
            <span className="label">Database Server:</span>
            <span className="value">{healthStatus?.database?.server || 'Unknown'}</span>
          </div>
        </div>
      </div>
    </div>
  );
}

export default HealthStatus;