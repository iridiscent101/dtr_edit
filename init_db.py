import os
import psycopg2
from werkzeug.security import generate_password_hash

def get_db_connection():
    db_url = os.getenv('DATABASE_URL')
    if db_url:
        return psycopg2.connect(db_url)
    else:
        return psycopg2.connect(
            dbname=os.environ.get("PGDATABASE", "nvsu_test"),
            user=os.environ.get("PGUSER", "postgres"),
            password=os.environ.get("PGPASSWORD", "admin"),
            host=os.environ.get("PGHOST", "localhost"),
            port=os.environ.get("PGPORT", "5432")
        )

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Create users table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            rfid_tag TEXT UNIQUE NOT NULL,
            role TEXT NOT NULL DEFAULT 'user'
        );
    """)
    
    # Create time_logs table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS time_logs (
            id SERIAL PRIMARY KEY,
            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            time_in BIGINT NOT NULL,
            time_out BIGINT,
            is_late BOOLEAN DEFAULT FALSE,
            rendered_hours NUMERIC(10, 2) DEFAULT 0.0
        );
    """)
    
    # Check if admin exists
    admin_email = os.environ.get('ADMIN_EMAIL', 'admin@nvsu.edu.ph')
    cur.execute("SELECT * FROM users WHERE email = %s", (admin_email,))
    admin = cur.fetchone()
    
    if not admin:
        admin_user = os.environ.get('ADMIN_USER', 'Admin')
        admin_password = os.environ.get('ADMIN_PASSWORD', 'admin')
        # We need a unique RFID for admin too, or allow it to be null if app handles it.
        # Looking at app.py, qr_scan uses rfid_tag. Let's give admin a dummy one.
        admin_rfid = os.environ.get('ADMIN_RFID', 'ADMIN_TEMP_TAG')
        
        cur.execute("""
            INSERT INTO users (name, email, password_hash, rfid_tag, role)
            VALUES (%s, %s, %s, %s, %s)
        """, (admin_user, admin_email, generate_password_hash(admin_password), admin_rfid, 'admin'))
        print(f"Admin user '{admin_user}' created.")
    else:
        print("Admin user already exists.")
        
    conn.commit()
    cur.close()
    conn.close()
    print("Database initialization completed.")

if __name__ == '__main__':
    init_db()
