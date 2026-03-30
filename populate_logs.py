import os
import psycopg2
import time
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
        )
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

# Timezone
TIMEZONE = "Asia/Singapore"
tz = zoneinfo.ZoneInfo(TIMEZONE)

def populate():
    conn = get_db_connection()
    if not conn:
        return
    cur = conn.cursor()

    # Get all users
    cur.execute("SELECT id, name FROM users")
    users = cur.fetchall()

    if not users:
        print("No users found in database.")
        return

    print(f"Generating 100 logs for {len(users)} users...")

    for user_id, user_name in users:
        print(f"Populating logs for {user_name}...")
        
        # Start from Jan 1st 2026 and go until today
        start_date = datetime(2026, 1, 1, tzinfo=tz)
        today = datetime.now(tz)
        
        current_date = start_date
        logs_created = 0

        while current_date <= today:
            # Skip weekends
            if current_date.weekday() < 5: # 0=Mon, ..., 4=Fri
                # Randomize Time In (7:50 AM to 8:30 AM)
                hour_in = 7 if random.random() > 0.3 else 8
                if hour_in == 7:
                    min_in = random.randint(50, 59)
                else:
                    min_in = random.randint(0, 30)

                # Randomize Time Out (5:00 PM to 5:45 PM)
                hour_out = 17
                min_out = random.randint(0, 45)

                # Create datetimes
                dt_in = current_date.replace(hour=hour_in, minute=min_in, second=random.randint(0,59), microsecond=0)
                dt_out = current_date.replace(hour=hour_out, minute=min_out, second=random.randint(0,59), microsecond=0)

                epoch_in = dt_in.timestamp()
                epoch_out = dt_out.timestamp()

                # Lateness logic from app.py
                is_late = hour_in > 8 or (hour_in == 8 and min_in > 15)

                # Rendered hours logic with lunch deduction (12 PM - 1 PM)
                total_duration = epoch_out - epoch_in
                lunch_start = dt_in.replace(hour=12, minute=0, second=0)
                lunch_end = dt_in.replace(hour=13, minute=0, second=0)

                if dt_in <= lunch_start and dt_out >= lunch_end:
                    total_duration -= 3600 # 1 hour lunch
                
                rendered_hours = round(max(0, total_duration) / 3600.0, 2)

                # Insert log
                log_date = current_date.strftime('%A, %B %d, %Y')
                cur.execute(
                    "INSERT INTO time_logs (user_id, time_in, time_out, is_late, rendered_hours, log_date) VALUES (%s, %s, %s, %s, %s, %s)",
                    (user_id, epoch_in, epoch_out, is_late, rendered_hours, log_date)
                )
                logs_created += 1

            # Move to next day
            current_date += timedelta(days=1)

        conn.commit()
    
    cur.close()
    conn.close()
    print("Done! All users have been populated with logs from Jan 1st to today.")

if __name__ == "__main__":
    populate()
