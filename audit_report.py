import sqlite3

conn = sqlite3.connect("upi_stream.db")
cursor = conn.cursor()

print("\n=== REAL-TIME STREAM AUDIT SUMMARY ===")
summary = cursor.execute("""
    SELECT 
        (SELECT COUNT(*) FROM clean_transactions) AS clean_txns,
        (SELECT COALESCE(ROUND(SUM(amount_inr), 2), 0) FROM clean_transactions) AS clean_volume,
        (SELECT COUNT(*) FROM fraud_alerts) AS blocked_txns,
        (SELECT COALESCE(ROUND(SUM(amount_inr), 2), 0) FROM fraud_alerts) AS blocked_volume
""").fetchone()

print(f"Clean Transactions   : {summary[0]} (Total: ₹{summary[1]:,.2f})")
print(f"Blocked Fraud Alerts : {summary[2]} (Prevented Loss: ₹{summary[3]:,.2f})")

print("\n=== TOP FLAGGED UPI ACCOUNTS ===")
top_accounts = cursor.execute("""
    SELECT 
        upi_id,
        COUNT(*) AS alert_count,
        ROUND(SUM(amount_inr), 2) AS total_flagged_inr
    FROM fraud_alerts
    GROUP BY upi_id
    ORDER BY total_flagged_inr DESC;
""").fetchall()

for row in top_accounts:
    print(f"UPI ID: {row[0]:<16} | Alerts: {row[1]:<3} | Flagged Amount: ₹{row[2]:,.2f}")

conn.close()