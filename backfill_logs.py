import os
import psycopg2
import random
from datetime import datetime, timedelta
import zoneinfo

# --- DATABASE CONFIG (Matches app.py) ---
def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    pg_host = os.environ.get("PGHOST", "localhost")
    is_remote = pg_host not in ("localhost", "127.0.0.1", "::1")
    ssl_mode = os.environ.get("PGSSLMODE", "require") if is_remote else "disable"
    try:
        if db_url:
            return psycopg2.connect(db_url, sslmode="require")
        return psycopg2.connect(
            dbname=os.environ.get("PGDATABASE", "nvsu_test"),
            user=os.environ.get("PGUSER", "postgres"),
            password=os.environ.get("PGPASSWORD", "admin"),
            host=pg_host,
            sslmode=ssl_mode,
            port=os.environ.get("PGPORT", "5432")
        )
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

# Timezone
TIMEZONE = "Asia/Singapore"
tz = zoneinfo.ZoneInfo(TIMEZONE)

def backfill():
    conn = get_db_connection()
    if not conn:
        return
    cur = conn.cursor()

    # Clear existing logs if user wants (optional safety check)
    # cur.execute("DELETE FROM time_logs")
    # conn.commit()

    # Get all users
    cur.execute("SELECT id, name FROM users")
    users = cur.fetchall()

    if not users:
        print("No users found in database.")
        return

    print(f"Generating backfill logs for {len(users)} users from Jan 1 to March 30, 2026...")

    start_date = datetime(2026, 1, 1, tzinfo=tz)
    end_date = datetime(2026, 3, 30, tzinfo=tz)

    logs_created = 0

    for user_id, user_name in users:
        current_date = start_date
        while current_date <= end_date:
            # Skip weekends
            if current_date.weekday() < 5: 
                # 1. MORNING SHIFT TIME-IN (around 8:00 AM)
                # Randomize: 7:50 AM to 8:15 AM (mostly on time)
                m_hour = 7 if random.random() > 0.2 else 8
                if m_hour == 7:
                    m_min = random.randint(50, 59)
                else:
                    m_min = random.randint(0, 20) # Occasional late after 8:15
                
                m_dt_in = current_date.replace(hour=m_hour, minute=m_min, second=random.randint(0,59), microsecond=0)
                m_epoch_in = m_dt_in.timestamp()
                m_late = (m_hour == 8 and m_min > 15)
                m_log_date = current_date.strftime('%A, %B %d, %Y')

                # Note: We provide a default time_out so the analytics work properly (Shift Completion/Integrity)
                # Morning shift ends at 12 PM
                m_dt_out = current_date.replace(hour=12, minute=0, second=0)
                m_epoch_out = m_dt_out.timestamp()
                m_hours = round((m_epoch_out - m_epoch_in) / 3600.0, 2)

                cur.execute(
                    "INSERT INTO time_logs (user_id, time_in, time_out, is_late, rendered_hours, log_date) VALUES (%s, %s, %s, %s, %s, %s)",
                    (user_id, m_epoch_in, m_epoch_out, m_late, m_hours, m_log_date)
                )

                # 2. AFTERNOON SHIFT TIME-IN (around 1:00 PM)
                # Randomize: 12:50 PM to 1:20 PM
                a_hour = 12 if random.random() > 0.2 else 13
                if a_hour == 12:
                    a_min = random.randint(50, 59)
                else:
                    a_min = random.randint(0, 25)
                
                a_dt_in = current_date.replace(hour=a_hour, minute=a_min, second=random.randint(0,59), microsecond=0)
                a_epoch_in = a_dt_in.timestamp()
                # Afternoon late threshold is 1:15 PM (13:15)
                a_late = (a_hour == 13 and a_min > 15)
                a_log_date = current_date.strftime('%A, %B %d, %Y')

                # Afternoon shift ends at 5 PM
                a_dt_out = current_date.replace(hour=17, minute=0, second=0)
                a_epoch_out = a_dt_out.timestamp()
                a_hours = round((a_epoch_out - a_epoch_in) / 3600.0, 2)

                cur.execute(
                    "INSERT INTO time_logs (user_id, time_in, time_out, is_late, rendered_hours, log_date) VALUES (%s, %s, %s, %s, %s, %s)",
                    (user_id, a_epoch_in, a_epoch_out, a_late, a_hours, m_log_date)
                )
                
                logs_created += 2

            current_date += timedelta(days=1)
        
        print(f"Finished user: {user_name}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"Success! Created {logs_created} logs for the specified range.")

if __name__ == "__main__":
    backfill()
