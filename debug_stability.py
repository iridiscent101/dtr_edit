import psycopg2, os
from datetime import datetime, timedelta
import zoneinfo

tz = zoneinfo.ZoneInfo("Asia/Singapore")
conn = psycopg2.connect(os.getenv('DATABASE_URL') or 'dbname=nvsu_test user=postgres password=admin host=localhost')
cur = conn.cursor()

thirty_days_ago = int((datetime.now() - timedelta(days=30)).timestamp())
user_id = 46
today_str = datetime.now(tz=tz).strftime('%Y-%m-%d')

cur.execute('SELECT time_in, time_out FROM time_logs WHERE user_id = %s AND time_in > %s AND time_out IS NOT NULL', (user_id, thirty_days_ago))
logs = cur.fetchall()

daily_last_out = {}
for r in logs:
    dt_in = datetime.fromtimestamp(r[0], tz=tz)
    dt_out = datetime.fromtimestamp(r[1], tz=tz)
    date_str = dt_in.strftime('%Y-%m-%d')
    if date_str == today_str:
        continue
    out_mins = dt_out.hour * 60 + dt_out.minute
    if date_str not in daily_last_out or out_mins > daily_last_out[date_str]:
        daily_last_out[date_str] = out_mins

retention_days = len(daily_last_out)
retention_count = sum(1 for mins in daily_last_out.values() if mins >= 17 * 60)
retention = (retention_count / retention_days * 100) if retention_days > 0 else 0

print(f"=== Nics Peri — Retention (per-day fix) ===")
print(f"Days analyzed (excl. today): {retention_days}")
print(f"Days with last clock-out >= 5 PM: {retention_count}")
print(f"Retention Score: {round(retention)}%")

cur.close()
conn.close()
