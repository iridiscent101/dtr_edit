import psycopg2
import os
import random
import time
from datetime import datetime, timedelta

def get_db_connection():
    db_url = os.getenv('DATABASE_URL')
    if db_url:
        return psycopg2.connect(db_url)
    return psycopg2.connect(
        dbname=os.environ.get("PGDATABASE", "nvsu_test"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "admin"),
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432")
    )

def seed_data():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        print("Seeding users...")
        users = []
        for i in range(1, 6):
            name = f"User {i}"
            email = f"user{i}@nvsu.edu.ph"
            password_hash = "12345678"  # Note: In production, use bcrypt/argon2
            rfid = f"RFID_TAG_{i:04d}"
            
            cur.execute("""
                INSERT INTO users (name, email, password_hash, rfid_tag, role)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name
                RETURNING id;
            """, (name, email, password_hash, rfid, 'user'))
            
            user_id = cur.fetchone()[0]
            users.append(user_id)

        print("Seeding time logs...")
        # Start from 30 days ago to today
        base_date = datetime.now() - timedelta(days=30)

        for user_id in users:
            for day_offset in range(30):
                current_day = base_date + timedelta(days=day_offset)
                
                # Logic: Log in between 8:00 AM and 8:30 AM
                time_in_dt = current_day.replace(hour=8, minute=0, second=0) + \
                             timedelta(minutes=random.randint(0, 30))
                
                # Logic: Log out between 4:30 PM and 5:30 PM
                time_out_dt = current_day.replace(hour=16, minute=30, second=0) + \
                              timedelta(minutes=random.randint(0, 60))

                time_in = int(time_in_dt.timestamp())
                time_out = int(time_out_dt.timestamp())
                
                # Calculate rendered hours with lunch break deduction
                # Standard lunch break: 12:00 PM - 1:00 PM (1 hour)
                duration_seconds = time_out - time_in
                
                # Only deduct lunch if work period covers the entire lunch period (12 PM - 1 PM)
                # i.e., time in is at or before 12 PM AND time out is at or after 1 PM
                if time_in_dt < time_out_dt:
                    lunch_start = time_in_dt.replace(hour=12, minute=0, second=0, microsecond=0)
                    lunch_end = time_in_dt.replace(hour=13, minute=0, second=0, microsecond=0)
                    if time_in_dt <= lunch_start and time_out_dt >= lunch_end:
                        duration_seconds = max(0, duration_seconds - 3600)
                
                rendered_hours = round(duration_seconds / 3600, 2)
                
                # Late logic (if after 8:00 AM)
                is_late = time_in_dt.hour >= 8 and time_in_dt.minute > 0

                cur.execute("""
                    INSERT INTO time_logs (user_id, time_in, time_out, is_late, rendered_hours)
                    VALUES (%s, %s, %s, %s, %s);
                """, (user_id, time_in, time_out, is_late, rendered_hours))

        conn.commit()
        print(f"Successfully inserted {len(users)} users and {len(users) * 30} logs.")

    except Exception as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    seed_data()