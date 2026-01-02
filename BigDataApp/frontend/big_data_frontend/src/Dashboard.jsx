import React, { useState, useEffect } from 'react';
import { LineChart, Line, PieChart, Pie, Cell, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { Client } from '@stomp/stompjs';
import './dashboardStyles.css';

const API_URL = 'http://localhost:8080/api/fraud';
const WS_URL = 'ws://localhost:8080/ws';

const FraudDashboard = () => {
  const [kpis, setKpis] = useState({
    totalTransactions: 0,
    fraudDetected: 0,
    fraudRate: 0, 
    averageFraudScore: 0
  });
  const [timeSeries, setTimeSeries] = useState([]);
  const [recentAlerts, setRecentAlerts] = useState([]);
  const [highRiskUsers, setHighRiskUsers] = useState([]);
  const [metrics, setMetrics] = useState({
    processingTimeMs: 0,
    messagesProcessed: 0,
    status: 'HEALTHY'
  });
  const [locationData, setLocationData] = useState([]);
  const [wsConnected, setWsConnected] = useState(false);

  useEffect(() => {
    console.log('Connecting to WebSocket:', WS_URL);
    
    const client = new Client({
      brokerURL: WS_URL,
      reconnectDelay: 5000,
      heartbeatIncoming: 10000,
      heartbeatOutgoing: 10000,
      debug: (str) => console.log('[STOMP]', str),
      onConnect: () => {
        console.log('✅ Connected to WebSocket!');
        setWsConnected(true);
        
        client.subscribe('/topic/fraud-alerts', (message) => {
          const alert = JSON.parse(message.body);
          console.log('🚨 Received fraud alert via WebSocket:', alert);
          
          setRecentAlerts(prev => {
            const updatedAlerts = [alert, ...prev.slice(0, 19)];
            
            const locationCounts = updatedAlerts.reduce((acc, a) => {
              const location = a.location || 'Unknown';
              acc[location] = (acc[location] || 0) + 1;
              return acc;
            }, {});
            
            const total = updatedAlerts.length;
            const locationArray = Object.entries(locationCounts)
              .map(([name, value]) => ({
                name,
                value,
                percentage: total > 0 ? ((value / total) * 100).toFixed(1) : 0
              }))
              .sort((a, b) => b.value - a.value)
              .slice(0, 5);
            
            setLocationData(locationArray);
            
            return updatedAlerts;
          });
        });

        client.subscribe('/topic/metrics', (message) => {
          const newMetrics = JSON.parse(message.body);
          console.log('Received metrics update:', newMetrics);
          setKpis({
            totalTransactions: newMetrics.totalTransactions,
            fraudDetected: newMetrics.fraudDetected,
            fraudRate: newMetrics.fraudRate,
            averageFraudScore: newMetrics.averageFraudScore
          });
        });

        client.subscribe('/topic/timeseries', (message) => {
          const newTimeSeries = JSON.parse(message.body);
          console.log('Received time series update');
          const formattedData = newTimeSeries.map(item => ({
            time: new Date(item.timestamp).toLocaleTimeString('en-US', { 
              hour: '2-digit',
              minute:'2-digit'
            }),
            total: item.totalTransactions,
            fraudCount: item.fraudCount
          }));
          setTimeSeries(formattedData);
        });

        client.subscribe('/topic/high-risk-users', (message) => {
          const newHighRiskUsers = JSON.parse(message.body);
          console.log('Received high risk users update');
          setHighRiskUsers(newHighRiskUsers.slice(0, 5));
        });

        client.subscribe('/topic/system-health', (message) => {
          const health = JSON.parse(message.body);
          console.log('🩺 System health update:', health);

          setMetrics({
            processingTimeMs: health.processingTimeMs,
            status: "HEALTHY",
            messagesProcessed: health.messagesProcessed
          });
        });
      },
      onDisconnect: () => {
        console.log('Disconnected');
        setWsConnected(false);
      },
      onStompError: (frame) => {
        console.error('STOMP Error:', frame);
        setWsConnected(false);
      }
    });

    client.activate();

    return () => {
      if (client) {
        client.deactivate();
      }
    };
  }, []);

  useEffect(() => {
    const fetchInitialData = async () => {
      await fetchKPIs();
      await fetchTimeSeries();
      await fetchRecentAlerts();
      await fetchHighRiskUsers();
    };
    
    fetchInitialData();
  }, []);

  const fetchKPIs = async () => {
    try {
      const response = await fetch(`${API_URL}/kpis?hours=24`);
      if (!response.ok) throw new Error('Failed to fetch KPIs');
      const data = await response.json();
      setKpis(data);
    } catch (error) {
      console.error('Error fetching KPIs:', error);
    }
  };

  const fetchTimeSeries = async () => {
    try {
      const response = await fetch(`${API_URL}/timeseries?hours=12`);
      if (!response.ok) throw new Error('Failed to fetch time series');
      const data = await response.json();
      const formattedData = data.map(item => ({
        time: new Date(item.timestamp).toLocaleTimeString('en-US', { 
          hour: '2-digit',
          minute:'2-digit'
        }),
        total: item.totalTransactions,
        fraudCount: item.fraudCount
      }));
      setTimeSeries(formattedData);
    } catch (error) {
      console.error('Error fetching time series:', error);
    }
  };

  const fetchRecentAlerts = async () => {
    try {
      const response = await fetch(`${API_URL}/alerts/fraud`);
      if (!response.ok) throw new Error('Failed to fetch alerts');
      const data = await response.json();
      setRecentAlerts(data);
      
      const locationCounts = data.reduce((acc, alert) => {
        const location = alert.location || 'Unknown';
        acc[location] = (acc[location] || 0) + 1;
        return acc;
      }, {});
      
      const total = data.length;
      const locationArray = Object.entries(locationCounts)
        .map(([name, value]) => ({
          name,
          value,
          percentage: total > 0 ? ((value / total) * 100).toFixed(1) : 0
        }))
        .sort((a, b) => b.value - a.value)
        .slice(0, 5);
      
      setLocationData(locationArray);
    } catch (error) {
      console.error('Error fetching recent alerts:', error);
    }
  };

  const fetchHighRiskUsers = async () => {
    try {
      const response = await fetch(`${API_URL}/users/high-risk?hours=24`);
      if (!response.ok) throw new Error('Failed to fetch high risk users');
      const data = await response.json();
      setHighRiskUsers(data.slice(0, 5));
    } catch (error) {
      console.error('Error fetching high risk users:', error);
    }
  };

  const COLORS = [
    '#1e3a8a',
    '#1d4ed8',
    '#2563eb',
    '#60a5fa',
    '#93c5fd'
  ];
  
  return (
    <div className="dashboard-container">
      <div className="connection-status">
        <div className={`status-indicator ${wsConnected ? 'connected' : 'disconnected'}`}></div>
        {wsConnected ? 'Live' : 'Disconnected'}
      </div>

      <h1 className="dashboard-title">
        Real-Time Fraud Detection Dashboard
      </h1>
      
      <div className="kpi-grid">
        <div className="kpi-card">
          <div className="kpi-label">Total Transactions (24h)</div>
          <div className="kpi-value">{kpis.totalTransactions?.toLocaleString()}</div>
        </div>
        
        <div className="kpi-card fraud">
          <div className="kpi-label">Fraud Alerts</div>
          <div className="kpi-value">{kpis.fraudDetected?.toLocaleString()}</div>
        </div>
        
        <div className="kpi-card rate">
          <div className="kpi-label">Fraud Rate</div>
          <div className="kpi-value">{kpis.fraudRate?.toFixed(2)}%</div>
        </div>
        
        <div className="kpi-card score">
          <div className="kpi-label">Avg Fraud Score</div>
          <div className="kpi-value">{kpis.averageFraudScore?.toFixed(3)}</div>
        </div>
      </div>

      <div className="charts-grid">
        <div className="chart-card">
          <h2 className="chart-title">Fraud Alerts Over Time (12h)</h2>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={timeSeries}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
              <XAxis dataKey="time" stroke="#9ca3af" style={{ fontSize: '12px' }} />
              <YAxis stroke="#9ca3af" style={{ fontSize: '12px' }} />
              <Tooltip />
              <Line 
                type="monotone" 
                dataKey="fraudCount" 
                stroke="#ef4444" 
                strokeWidth={2.5}
                name="Fraud Count"
                dot={{ fill: '#ef4444', r: 4 }}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="chart-card">
          <h2 className="chart-title">Fraud Distribution By Location</h2>
          {locationData.length > 0 ? (
            <ResponsiveContainer width="100%" height={280}>
              <PieChart>
                <Pie
                  data={locationData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, percentage }) => `${name} ${percentage}%`}
                  outerRadius={90}
                  dataKey="value"
                >
                  {locationData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip />
              </PieChart>
            </ResponsiveContainer>
          ) : (
            <div className="no-data">No fraud data available</div>
          )}
        </div>
      </div>

      <div className="bottom-grid">
        <div className="alerts-card">
          <h2 className="chart-title">Recent Fraud Alerts (Real-Time)</h2>
          <div className="alerts-table-container">
            {recentAlerts.length > 0 ? (
              <table className="alerts-table">
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Transaction ID</th>
                    <th>User ID</th>
                    <th>Amount</th>
                    <th>Fraud Score</th>
                    <th>Location</th>
                  </tr>
                </thead>
                <tbody>
                  {recentAlerts.slice(0, 20).map((alert, index) => (
                    <tr key={alert.transactionId || index}>
                      <td>
                        {new Date(alert.eventTime).toLocaleTimeString('en-US', { 
                          hour: '2-digit', 
                          minute: '2-digit',
                          second: '2-digit'
                        })}
                      </td>
                      <td className="monospace">{alert.transactionId}</td>
                      <td className="monospace">{alert.userId}</td>
                      <td className="amount">${alert.transactionAmount?.toFixed(2)}</td>
                      <td>
                        <span className={`fraud-score-badge ${alert.fraudScore > 0.9 ? 'high' : 'medium'}`}>
                          {alert.fraudScore?.toFixed(3)}
                        </span>
                      </td>
                      <td>{alert.location}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <div className="no-alerts">
                No fraud alerts detected yet. Waiting for fraudulent transactions...
              </div>
            )}
          </div>
        </div>

        <div className="sidebar">
          <div className="sidebar-card">
            <h2 className="chart-title"> High Risk Users (24h)</h2>
            {highRiskUsers.length > 0 ? (
              highRiskUsers.map((user, index) => (
                <div key={index} className="risk-user-item">
                  <span className="risk-user-id">👤 {user.userId}</span>
                  <span className="risk-user-badge">{user.fraudCount} Alerts</span>
                </div>
              ))
            ) : (
              <div className="no-risk-users">No high-risk users detected</div>
            )}
          </div>

          <div className="sidebar-card">
            <h2 className="chart-title">System Health</h2>
            <div className="system-health">
              <div className="health-metric">
                <span className="health-label">Processing Time</span>
                <span className={`health-value ${metrics.processingTimeMs > 100 ? 'warning' : 'success'}`}>
                  {metrics.processingTimeMs}ms
                </span>
              </div>
              <div className="health-metric">
                <span className="health-label">Messages Processed</span>
                <span className="health-value success">
                  {metrics.messagesProcessed?.toLocaleString()}
                </span>
              </div>
              <div className="health-metric">
                <span className="health-label">WebSocket Status</span>
                <span className={`health-value ${wsConnected ? 'success' : 'error'}`}>
                  {wsConnected ? 'Connected' : ' Disconnected'}
                </span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default FraudDashboard;