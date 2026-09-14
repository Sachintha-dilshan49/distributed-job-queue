'use client';

import { useEffect, useState } from 'react';

export default function Dashboard() {
  const [stats, setStats] = useState({});
  const [workers, setWorkers] = useState([]);

  const fetchData = async () => {
    const [s, w] = await Promise.all([
      fetch('http://localhost:8080/admin/stats').then(r => r.json()),
      fetch('http://localhost:8080/admin/workers').then(r => r.json()),
    ]);
    setStats(s);
    setWorkers(w);
  };

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 3000);
    return () => clearInterval(interval);
  }, []);

  const kill = async (claimedBy) => {
    await fetch(`http://localhost:8080/admin/workers/${encodeURIComponent(claimedBy)}/kill`, {
      method: 'POST',
    });
    fetchData();
  };

  return (
    <main style={{ padding: 32, fontFamily: 'monospace' }}>
      <h1>Job Queue Dashboard</h1>

      <h2>Stats</h2>
      <table border="1" cellPadding="8">
        <thead><tr><th>State</th><th>Count</th></tr></thead>
        <tbody>
          {Object.entries(stats).map(([state, count]) => (
            <tr key={state}><td>{state}</td><td>{count}</td></tr>
          ))}
        </tbody>
      </table>

      <h2>Active Workers</h2>
      {workers.length === 0 ? <p>No active workers</p> : (
        <table border="1" cellPadding="8">
          <thead><tr><th>Worker</th><th>Jobs</th><th>Action</th></tr></thead>
          <tbody>
            {workers.map(w => (
              <tr key={w.claimed_by}>
                <td>{w.claimed_by}</td>
                <td>{w.job_count}</td>
                <td><button onClick={() => kill(w.claimed_by)}>Kill</button></td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  );
}