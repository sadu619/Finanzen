import React, { useState, useEffect } from 'react';
import './App.css';
import apiService from './services/apiService';

// Components
import HealthStatus from './components/HealthStatus';
import Dashboard from './components/Dashboard';

function App() {
  const [healthStatus, setHealthStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [currentView, setCurrentView] = useState('dashboard');

  useEffect(() => {
    loadInitialData();
  }, []);

  const loadInitialData = async () => {
    try {
      const health = await apiService.getHealth();
      setHealthStatus(health);
    } catch (error) {
      console.error('Failed to load initial data:', error);
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="app-loading">
        <h2>Loading SAP Dashboard...</h2>
        <div className="spinner"></div>
      </div>
    );
  }

  return (
    <div className="App">
      <header className="app-header">
        <h1>🏢 SAP Integration Dashboard</h1>
        <nav className="app-nav">
          <button 
            onClick={() => setCurrentView('dashboard')}
            className={currentView === 'dashboard' ? 'active' : ''}
          >
            Dashboard
          </button>
          <button 
            onClick={() => setCurrentView('health')}
            className={currentView === 'health' ? 'active' : ''}
          >
            System Health
          </button>
        </nav>
      </header>

      <main className="app-main">
        {currentView === 'dashboard' && (
          <Dashboard healthStatus={healthStatus} />
        )}
        
        {currentView === 'health' && (
          <HealthStatus healthStatus={healthStatus} onRefresh={loadInitialData} />
        )}
      </main>

      <footer className="app-footer">
        <p>SAP Integration v4.2.0 | Azure Static Web Apps</p>
      </footer>
    </div>
  );
}

export default App;