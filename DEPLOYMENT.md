# NVSU DTR System Deployment Guide

This guide explains how to deploy the NVSU DTR (Daily Time Record) system using Neon for the database and Render for hosting.

## Prerequisites

1. A Neon account (you already have one)
2. A Render account (you already have one)
3. This codebase

## Deployment Steps

### 1. Set up Neon PostgreSQL Database

1. Log in to your Neon account
2. Create a new project or use an existing one
3. Note down the following connection details from your Neon dashboard:
   - Host (`PGHOST`)
   - Database name (`PGDATABASE`)
   - User (`PGUSER`)
   - Password (`PGPASSWORD`)
   - Connection string (for reference)

### 2. Configure Environment Variables in Render

When deploying to Render, you'll need to set the following environment variables in your Render dashboard:

```
PGHOST=your_neon_host
PGDATABASE=your_neon_database_name
PGUSER=your_neon_user
PGPASSWORD=your_neon_password
PGPORT=5432
SECRET_KEY=your_secret_key_here
TIMEZONE=Asia/Singapore
# Shift timing configuration (optional)
AM_SHIFT_HOUR=8
AM_SHIFT_MINUTE=0
AM_LATE_THRESHOLD_MINUTES=15
PM_SHIFT_HOUR=13
PM_SHIFT_MINUTE=0
PM_LATE_THRESHOLD_MINUTES=15
PYTHON_VERSION='3.9.15'
PGSSLMODE='require'
PGCHANNELBINDING='require'
```

### 3. Deploy to Render

1. Fork this repository to your GitHub account (if not already done)
2. Log in to your Render account
3. Click "New" and select "Web Service"
4. Connect your GitHub account and select your repository
5. Configure the service:
   - Name: `nvsu-dtr-system` (or any name you prefer)
   - Runtime: Python 3
   - Build command: `pip install -r requirements.txt`
   - Start command: `gunicorn --bind 0.0.0.0:$PORT app:app`
6. Set the environment variables as described in step 2
7. Click "Create Web Service"

### 4. Initialize the Database

After deployment, you'll need to initialize the database with the required tables:

1. In your Render dashboard, go to your service
2. Go to the "Shell" tab
3. Run the following command:
   ```
   python init_db.py
   ```

This will create the necessary tables and a default admin user:
- Email: `admin@nvsu.edu.ph`
- Password: `admin` (change this after first login)

### 5. Access Your Application

Once deployment is complete, your application will be accessible at the URL provided by Render (usually `https://your-app-name.onrender.com`).

## Notes

- The application uses environment variables for database configuration, making it easy to deploy to different environments
- Make sure to change the default admin password after your first login
- For the SECRET_KEY, generate a secure random string (you can use `python -c "import os; print(os.urandom(24).hex())"` to generate one)
- For the TIMEZONE, use a valid timezone name from the IANA timezone database (e.g., "Asia/Singapore", "America/New_York", etc.). Defaults to "Asia/Singapore" if not set.
- Render automatically handles SSL certificates for your application
- The application will automatically scale based on traffic