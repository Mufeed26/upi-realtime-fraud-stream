# Real-Time UPI Fraud & Anomaly Stream Pipeline

A stateful, real-time streaming ETL and anomaly detection pipeline built in **Python** and **SQLite**. It simulates live UPI payment events, maintains in-memory sliding user windows (`collections.deque`) to detect fraud patterns in milliseconds, and routes events into isolated SQL sinks (`clean_transactions` vs. `fraud_alerts`).

## Stream Architecture

```text
+------------------------+
| Live UPI Event Stream  | (Producer: 2.5 events/sec)
+-----------+------------+
            |
            v
+------------------------+
| Stateful Rule Engine   | (Sliding Window Memory per UPI ID)
| - Impossible Travel    |
| - Late-Night Spikes    |
| - Velocity Bursts      |
+-----+------------+-----+
      |            |
   [Clean]      [Flagged]
      |            |
      v            v
+-----------+ +---------------+
|Clean Sink | |Fraud Alert DLQ| (SQLite Dual-Sink Tables)
+-----+-----+ +-------+-------+
      |               |
      +-------+-------+
              |
              v
     +-----------------+
     | SQL Audit Layer | (audit_report.py)
     +-----------------+