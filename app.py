from flask import (
    Flask,
    render_template,
    request,
    jsonify,
    redirect,
    url_for,
    session,
    flash,
)
from werkzeug.security import generate_password_hash, check_password_hash
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import time
from functools import wraps
import os
import logging
from datetime import timezone
import zoneinfo
import qrcode
from io import BytesIO
import base64
import json
import statistics
import math


# Helper function for 12-hour time format (works on both Windows and Unix)
def format_time_12h(dt):
    hour = dt.hour
    if hour == 0:
        return f"12:{dt.strftime('%M')} AM"
    elif hour < 12:
        return f"{hour}:{dt.strftime('%M')} AM"
    elif hour == 12:
        return f"12:{dt.strftime('%M')} PM"
    else:
        return f"{hour-12}:{dt.strftime('%M')} PM"


app = Flask(__name__, static_folder="images", static_url_path="/images")

# Set up logging first so logger is available everywhere below
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

_secret = os.environ.get("SECRET_KEY")
if not _secret:
    logger.warning(
        "SECRET_KEY not set — using a random key. All sessions will be lost on restart. Set SECRET_KEY in production."
    )
    _secret = os.urandom(24)
app.secret_key = _secret

# Permanent session lifetime (30 days) - used when "Remember Me" is checked
app.permanent_session_lifetime = timedelta(days=30)

# In-memory store for pending QR login tokens {token: {"user_id": ..., "validated": bool}}
# In production, use Redis or a DB table with TTL instead.
import secrets as _secrets

qr_login_store = {}

# Timezone configuration
TIMEZONE = os.environ.get("TIMEZONE", "Asia/Singapore")
try:
    tz = zoneinfo.ZoneInfo(TIMEZONE)
except Exception:
    tz = zoneinfo.ZoneInfo("Asia/Singapore")


# Jinja2 template filter for epoch time
@app.template_filter("format_epoch")
def format_epoch(epoch_time):
    utc_dt = datetime.fromtimestamp(epoch_time, tz=timezone.utc)
    local_dt = utc_dt.astimezone(tz)
    return local_dt.strftime("%Y-%m-%d %I:%M %p")


# --- Database Connection ---
def get_db_connection():
    db_url = os.getenv("DATABASE_URL")
    pg_host = os.environ.get("PGHOST", "localhost")
    # Only use SSL for remote hosts (Neon/production).
    # Local PostgreSQL does not support SSL.
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
            port=os.environ.get("PGPORT", "5432"),
            sslmode=ssl_mode,
        )
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection failed: {e}")
        raise


from contextlib import contextmanager


@contextmanager
def db_cursor():
    """Context manager that yields a RealDictCursor and auto-closes conn/cur.
    Usage:
        with db_cursor() as cur:
            cur.execute(...)
    Rolls back on exception, always closes. Raises DatabaseError → 503 via error handler.
    """
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        yield cur, conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()


@app.errorhandler(psycopg2.OperationalError)
def handle_db_error(e):
    logger.error(f"Unhandled DB error: {e}")
    if request.is_json or request.path.startswith("/api/"):
        return (
            jsonify(
                {
                    "success": False,
                    "message": "Database unavailable. Please try again later.",
                }
            ),
            503,
        )
    flash("Database unavailable. Please try again later.", "error")
    return redirect(url_for("login"))


# --- Utility: Time Range Epochs ---
def get_time_range_epochs(filter_type, specific_date=None):
    if specific_date:
        try:
            naive_dt = datetime.strptime(specific_date, "%Y-%m-%d")
            start_dt = naive_dt.replace(tzinfo=tz)
            end_dt = start_dt + timedelta(days=1)
            return int(start_dt.timestamp()), int(end_dt.timestamp())
        except ValueError:
            return None, None

    now = datetime.now(tz)
    if filter_type == "today":
        start_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = start_dt + timedelta(days=1)
    elif filter_type == "week":
        start_dt = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end_dt = start_dt + timedelta(days=7)
    elif filter_type == "month":
        start_dt = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        if now.month == 12:
            end_dt = now.replace(
                year=now.year + 1,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
        else:
            end_dt = now.replace(
                month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0
            )
    elif filter_type == "year":
        start_dt = now.replace(
            month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        )
        end_dt = now.replace(
            year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        )
    else:
        return None, None

    return int(start_dt.timestamp()), int(end_dt.timestamp())


# --- Utility: Avg Clock Times ---
def get_avg_clock_times(cur, base_where, params):
    """Run the avg clock-in/out query and return formatted strings."""
    avg_sql = f"""
        SELECT
            AVG(EXTRACT(HOUR FROM TO_TIMESTAMP(time_in) AT TIME ZONE 'Asia/Singapore') * 3600 +
                EXTRACT(MINUTE FROM TO_TIMESTAMP(time_in) AT TIME ZONE 'Asia/Singapore') * 60 +
                EXTRACT(SECOND FROM TO_TIMESTAMP(time_in) AT TIME ZONE 'Asia/Singapore')) as avg_time_in,
            AVG(EXTRACT(HOUR FROM TO_TIMESTAMP(time_out) AT TIME ZONE 'Asia/Singapore') * 3600 +
                EXTRACT(MINUTE FROM TO_TIMESTAMP(time_out) AT TIME ZONE 'Asia/Singapore') * 60 +
                EXTRACT(SECOND FROM TO_TIMESTAMP(time_out) AT TIME ZONE 'Asia/Singapore')) as avg_time_out
        FROM time_logs
        WHERE time_out IS NOT NULL {base_where}
    """
    cur.execute(avg_sql, tuple(params))
    result = cur.fetchone()
    avg_clock_in = "--:--"
    avg_clock_out = "--:--"
    if result and result["avg_time_in"]:
        avg_dt = datetime.now(tz).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(seconds=int(result["avg_time_in"]))
        avg_clock_in = format_time_12h(avg_dt)
    if result and result["avg_time_out"]:
        avg_dt = datetime.now(tz).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(seconds=int(result["avg_time_out"]))
        avg_clock_out = format_time_12h(avg_dt)
    return avg_clock_in, avg_clock_out


def validate_password(password):
    """Returns (ok, error_message). Enforces minimum 8 chars server-side."""
    if not password or len(password.strip()) < 8:
        return False, "Password must be at least 8 characters."
    return True, None


def parse_filter_params():
    """Extract and resolve date filter query params into (start_ep, end_ep, meta).
    Eliminates the 25-line block that was copy-pasted across 4 routes.
    Returns a dict with keys: start_ep, end_ep, filter_time, specific_date,
    date_from, date_to, status_filter.
    """
    filter_time = request.args.get("filter_time", "all").strip() or "all"
    specific_date = request.args.get("specific_date", "").strip() or None
    date_from = request.args.get("date_from", "").strip() or None
    date_to = request.args.get("date_to", "").strip() or None
    status_filter = request.args.get("status_filter", "all").strip() or "all"

    if date_from and date_to:
        try:
            start_dt = datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=tz)
            end_dt = datetime.strptime(date_to, "%Y-%m-%d").replace(
                tzinfo=tz
            ) + timedelta(days=1)
            start_ep, end_ep = int(start_dt.timestamp()), int(end_dt.timestamp())
        except ValueError:
            start_ep, end_ep = None, None
    else:
        start_ep, end_ep = get_time_range_epochs(
            filter_time if not specific_date else None, specific_date
        )

    return {
        "start_ep": start_ep,
        "end_ep": end_ep,
        "filter_time": filter_time,
        "specific_date": specific_date,
        "date_from": date_from,
        "date_to": date_to,
        "status_filter": status_filter,
    }


# --- Auth Decorators ---
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("login"))
        return f(*args, **kwargs)

    return decorated_function


def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "user_id" not in session or session.get("user_role") != "admin":
            return redirect(url_for("user_dashboard"))
        return f(*args, **kwargs)

    return decorated_function


# =============================================================================
# AUTH ROUTES
# =============================================================================


@app.route("/")
def index():
    if "user_id" in session:
        if session.get("user_role") == "admin":
            return redirect(url_for("admin_dashboard"))
        return redirect(url_for("user_dashboard"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "")
        remember_me = request.form.get("remember_me")
        try:
            with db_cursor() as (cur, _):
                cur.execute("SELECT * FROM users WHERE email = %s", (email,))
                user = cur.fetchone()
        except Exception as e:
            logger.error(f"Login DB error: {e}")
            flash("System error. Please try again.", "error")
            return render_template("login.html")
        if user and check_password_hash(user["password_hash"], password):
            session["user_id"] = user["id"]
            session["user_name"] = user["name"]
            session["user_role"] = user["role"]
            session["user_email"] = user["email"]
            # Keep user logged in for 30 days if "Remember Me" is checked
            if remember_me:
                session.permanent = True
            if user["role"] == "admin":
                return redirect(url_for("admin_dashboard"))
            return redirect(url_for("user_dashboard"))
        else:
            flash("Invalid email or password", "error")
    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# =============================================================================
# USER ROUTES
# =============================================================================


@app.route("/dashboard")
@login_required
def user_dashboard():
    user_id = session["user_id"]
    f = parse_filter_params()
    start_ep, end_ep = f["start_ep"], f["end_ep"]
    filter_time, specific_date = f["filter_time"], f["specific_date"]
    date_from, date_to = f["date_from"], f["date_to"]
    status_filter = f["status_filter"]

    page = max(1, int(request.args.get("page", 1) or 1))
    per_page = 30

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if start_ep and end_ep:
        cur.execute(
            "SELECT * FROM time_logs WHERE user_id = %s AND time_in >= %s AND time_in < %s ORDER BY time_in DESC LIMIT 500",
            (user_id, start_ep, end_ep),
        )
    else:
        cur.execute(
            "SELECT * FROM time_logs WHERE user_id = %s ORDER BY time_in DESC LIMIT 500",
            (user_id,),
        )
    all_logs = cur.fetchall()

    if status_filter == "late":
        all_logs = [l for l in all_logs if l.get("is_late")]
    elif status_filter == "ontime":
        all_logs = [l for l in all_logs if not l.get("is_late") and l.get("time_out")]

    total_logs = len(all_logs)
    total_pages = max(1, (total_logs + per_page - 1) // per_page)
    page = min(page, total_pages)
    logs = all_logs[(page - 1) * per_page : page * per_page]

    if start_ep and end_ep:
        cur.execute(
            "SELECT COUNT(*) FILTER (WHERE is_late = TRUE) as total_lates, SUM(rendered_hours) as total_hours FROM time_logs WHERE user_id = %s AND time_in >= %s AND time_in < %s",
            (user_id, start_ep, end_ep),
        )
    else:
        cur.execute(
            "SELECT COUNT(*) FILTER (WHERE is_late = TRUE) as total_lates, SUM(rendered_hours) as total_hours FROM time_logs WHERE user_id = %s",
            (user_id,),
        )
    stats = cur.fetchone()

    avg_where = "AND user_id = %s"
    avg_params = [user_id]
    if start_ep and end_ep:
        avg_where += " AND time_in >= %s AND time_in < %s"
        avg_params += [start_ep, end_ep]
    avg_clock_in, avg_clock_out = get_avg_clock_times(cur, avg_where, avg_params)

    logs_data = []
    for log in logs:
        log_copy = dict(log)
        if log_copy.get("rendered_hours") is not None:
            log_copy["rendered_hours"] = float(log_copy["rendered_hours"])
        logs_data.append(log_copy)

    cur.close()
    conn.close()

    return render_template(
        "dashboard.html",
        logs=logs,
        logs_data=logs_data,
        total_lates=stats["total_lates"] or 0,
        total_hours=round(stats["total_hours"] or 0.0, 2),
        current_time_filter=(
            filter_time if not specific_date and not date_from else "specific"
        ),
        current_specific_date=specific_date,
        current_date_from=date_from,
        current_date_to=date_to,
        current_status_filter=status_filter,
        avg_clock_in=avg_clock_in,
        avg_clock_out=avg_clock_out,
        page=page,
        total_pages=total_pages,
        total_logs=total_logs,
    )
    
@app.route("/user/radar")
@login_required
def user_radar():
    """
    User Route: Renders the Behavioral Radar Chart page.
    Correctly handles BIGINT timestamps and missing 'department' columns.
    """
    user_id = session["user_id"]
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # 1. Fetch User Info (Handle potential missing department column)
    try:
        cur.execute("SELECT name, department FROM users WHERE id = %s", (user_id,))
        user_info = cur.fetchone()
    except Exception:
        conn.rollback() # Reset transaction if 'department' column doesn't exist
        cur.execute("SELECT name FROM users WHERE id = %s", (user_id,))
        user_info = cur.fetchone()
        user_info['department'] = "General" # Fallback value
    
    # 2. Fetch last 30 days of logs 
    # Calculate the epoch timestamp for 30 days ago to match the BIGINT column type
    thirty_days_ago_dt = datetime.now() - timedelta(days=30)
    thirty_days_ago_epoch = int(thirty_days_ago_dt.timestamp())
    
    # If your timestamps are in milliseconds (13 digits), multiply by 1000
    # This query avoids the BigInt vs Timestamp operator error
    cur.execute("""
        SELECT time_in, time_out, is_late 
        FROM time_logs 
        WHERE user_id = %s AND time_in > %s
        ORDER BY time_in ASC
    """, (user_id, thirty_days_ago_epoch))
    logs = cur.fetchall()
    
    total_logs = len(logs)
    if total_logs == 0:
        # Default scores for empty profiles
        scores = {m: 50 for m in ["Punctuality", "Shift Completion", "Reliability", "Stability", "Integrity", "Retention"]}
    else:
        # Helper to convert BIGINT epoch to Python datetime
        def to_dt(epoch_val):
            if epoch_val is None: return None
            # Standard DTRs use seconds (10 digits), JS/Web often uses ms (13 digits)
            return datetime.fromtimestamp(epoch_val if epoch_val < 1e11 else epoch_val/1000)

        # 1. Punctuality (% of logs not marked late)
        punctual_count = len([l for l in logs if not l['is_late']])
        punctuality = (punctual_count / total_logs) * 100
        
        # 2. Shift Completion (% of logs where user worked >= 8 hours)
        completed_shifts = 0
        for l in logs:
            if l['time_out'] and l['time_in']:
                duration_seconds = l['time_out'] - l['time_in']
                # Adjust threshold if timestamps are in milliseconds
                threshold = 28800 if l['time_in'] < 1e11 else 28800000 
                if duration_seconds >= threshold:
                    completed_shifts += 1
        shift_completion = (completed_shifts / total_logs) * 100
        
        # 3. Reliability (Ratio of logs vs expected 22 work days)
        reliability = min((total_logs / 22) * 100, 100)
        
        # 4. Stability (Consistency of arrival times - Inverse of Std Deviation)
        clock_in_minutes = []
        for l in logs:
            dt = to_dt(l['time_in'])
            clock_in_minutes.append(dt.hour * 60 + dt.minute)
        
        if len(clock_in_minutes) > 1:
            std_dev = statistics.stdev(clock_in_minutes)
            stability = max(100 - (std_dev * 2), 0)
        else:
            stability = 100
            
        # 5. Integrity (% of logs with both In and Out timestamps)
        integrity = (len([l for l in logs if l['time_out']]) / total_logs) * 100
        
        # 6. Retention (% of clock-outs after 5:00 PM)
        retention_count = 0
        for l in logs:
            dt_out = to_dt(l['time_out'])
            if dt_out and dt_out.hour >= 17:
                retention_count += 1
        retention = (retention_count / total_logs) * 100

        scores = {
            "Punctuality": round(punctuality),
            "Shift Completion": round(shift_completion),
            "Reliability": round(reliability),
            "Stability": round(stability),
            "Integrity": round(integrity),
            "Retention": round(retention)
        }

    cur.close()
    conn.close()
    
    # Prepare data for React/Recharts
    metrics_json = [{"metric": k, "me": v, "avg": 75} for k, v in scores.items()]
    
    return render_template("user_radar.html", 
                           user=user_info, 
                           metrics=metrics_json, 
                           total_logs=total_logs)


@app.route("/dashboard/analytics")
@login_required
def user_analytics():
    user_id = session["user_id"]
    f = parse_filter_params()
    start_ep, end_ep = f["start_ep"], f["end_ep"]
    filter_time, specific_date = f["filter_time"], f["specific_date"]
    date_from, date_to = f["date_from"], f["date_to"]

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if start_ep and end_ep:
        cur.execute(
            "SELECT * FROM time_logs WHERE user_id = %s AND time_in >= %s AND time_in < %s ORDER BY time_in DESC LIMIT 500",
            (user_id, start_ep, end_ep),
        )
    else:
        cur.execute(
            "SELECT * FROM time_logs WHERE user_id = %s ORDER BY time_in DESC LIMIT 500",
            (user_id,),
        )
    logs = cur.fetchall()

    if start_ep and end_ep:
        cur.execute(
            "SELECT COUNT(*) FILTER (WHERE is_late = TRUE) as total_lates, SUM(rendered_hours) as total_hours FROM time_logs WHERE user_id = %s AND time_in >= %s AND time_in < %s",
            (user_id, start_ep, end_ep),
        )
    else:
        cur.execute(
            "SELECT COUNT(*) FILTER (WHERE is_late = TRUE) as total_lates, SUM(rendered_hours) as total_hours FROM time_logs WHERE user_id = %s",
            (user_id,),
        )
    stats = cur.fetchone()

    avg_where = "AND user_id = %s"
    avg_params = [user_id]
    if start_ep and end_ep:
        avg_where += " AND time_in >= %s AND time_in < %s"
        avg_params += [start_ep, end_ep]
    avg_clock_in, avg_clock_out = get_avg_clock_times(cur, avg_where, avg_params)

    logs_data = []
    for log in logs:
        log_copy = dict(log)
        if log_copy.get("rendered_hours") is not None:
            log_copy["rendered_hours"] = float(log_copy["rendered_hours"])
        logs_data.append(log_copy)

    cur.close()
    conn.close()

    return render_template(
        "analytics_user.html",
        logs=logs,
        logs_data=logs_data,
        total_lates=stats["total_lates"] or 0 if stats else 0,
        total_hours=round(stats["total_hours"] or 0.0, 2) if stats else 0.0,
        avg_clock_in=avg_clock_in,
        avg_clock_out=avg_clock_out,
    )


@app.route("/profile", methods=["GET", "POST"])
@login_required
def profile():
    user_id = session["user_id"]
    try:
        with db_cursor() as (cur, conn):
            if request.method == "POST":
                new_password = request.form.get("new_password", "").strip()
                if len(new_password) < 8:
                    flash("Password must be at least 8 characters.", "error")
                else:
                    cur.execute(
                        "UPDATE users SET password_hash = %s WHERE id = %s",
                        (generate_password_hash(new_password), user_id),
                    )
                    flash("Password updated successfully!", "success")
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            user = cur.fetchone()
    except Exception as e:
        logger.error(f"Profile error: {e}")
        flash("An error occurred. Please try again.", "error")
        return redirect(url_for("user_dashboard"))
    return render_template("profile.html", user=user)


@app.route("/api/dashboard-tap", methods=["POST"])
@login_required
def dashboard_tap_rfid():
    user_id = session["user_id"]
    try:
        with db_cursor() as (cur, _):
            cur.execute("SELECT rfid_tag FROM users WHERE id = %s", (user_id,))
            user = cur.fetchone()
    except Exception as e:
        logger.error(f"Dashboard tap error: {e}")
        return jsonify({"success": False, "message": "Database error"}), 503
    if user:
        return jsonify({"success": True, "rfid": user["rfid_tag"]})
    return jsonify({"success": False, "message": "User not found"}), 404


# =============================================================================
# KIOSK ROUTES
# =============================================================================


@app.route("/login/qr")
def qr_login():
    """Desktop page that shows a QR code for mobile login."""
    token = _secrets.token_urlsafe(32)
    qr_login_store[token] = {
        "user_id": None,
        "validated": False,
        "created_at": datetime.now(tz),
    }
    scan_url = url_for("mobile_qr_confirm", token=token, _external=True)
    # Generate QR code for the scan URL
    qr = qrcode.QRCode(version=1, box_size=10, border=4)
    qr.add_data(scan_url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buffer = BytesIO()
    img.save(buffer, format="PNG")
    qr_code_b64 = base64.b64encode(buffer.getvalue()).decode()
    return render_template(
        "qr_login.html", token=token, qr_code=qr_code_b64, scan_url=scan_url
    )


@app.route("/login/qr/mobile/<token>")
@login_required
def mobile_qr_confirm(token):
    """Mobile page where a logged-in user confirms the desktop login."""
    if token not in qr_login_store:
        flash("Invalid or expired QR token.", "error")
        return redirect(url_for("login"))
    return render_template("mobile_qr_scanner.html", token=token)


@app.route("/api/qr-validate", methods=["POST"])
@login_required
def qr_validate():
    """Mobile user confirms the desktop login token."""
    data = request.json or {}
    token = data.get("token", "").strip()
    if not token or token not in qr_login_store:
        return jsonify({"success": False, "message": "Invalid or expired token"}), 400
    entry = qr_login_store[token]
    # Expire tokens older than 5 minutes
    if (datetime.now(tz) - entry["created_at"]).total_seconds() > 300:
        del qr_login_store[token]
        return jsonify({"success": False, "message": "Token expired"}), 400
    entry["user_id"] = session["user_id"]
    entry["validated"] = True
    return jsonify({"success": True})


@app.route("/api/qr-check")
def qr_check():
    """Desktop polls this to know when mobile has confirmed the token."""
    token = request.args.get("token", "").strip()
    if not token or token not in qr_login_store:
        return jsonify({"success": False, "message": "Invalid token"}), 400
    entry = qr_login_store[token]
    if (datetime.now(tz) - entry["created_at"]).total_seconds() > 300:
        del qr_login_store[token]
        return jsonify({"success": False, "message": "Token expired"}), 400
    if not entry["validated"]:
        return Response(status=202)  # Still waiting
    # Mark consumed and log the user in
    user_id = entry["user_id"]
    del qr_login_store[token]
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
    user = cur.fetchone()
    cur.close()
    conn.close()
    if not user:
        return jsonify({"success": False, "message": "User not found"}), 404
    session["user_id"] = user["id"]
    session["user_name"] = user["name"]
    session["user_role"] = user["role"]
    session["user_email"] = user["email"]
    redirect_url = (
        url_for("admin_dashboard")
        if user["role"] == "admin"
        else url_for("user_dashboard")
    )
    return jsonify({"success": True, "redirect": redirect_url})


@app.route("/kiosk")
@login_required
def tap_interface():
    return render_template("index.html")


def _process_tap_logic(rfid_tag):
    """Shared tap logic used by both RFID and QR scan endpoints."""
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT * FROM users WHERE rfid_tag = %s", (rfid_tag,))
        user = cur.fetchone()

        if not user:
            return jsonify({"success": False, "message": "Unregistered ID"}), 404

        user_id = user["id"]
        current_dt = datetime.now(tz)
        current_epoch = int(current_dt.timestamp())
        hour = current_dt.hour
        minute = current_dt.minute

        logger.info(
            f"User {user['name']} tapped at epoch {current_epoch} ({current_dt})"
        )

        cur.execute(
            "SELECT * FROM time_logs WHERE user_id = %s AND time_out IS NULL",
            (user_id,),
        )
        active_log = cur.fetchone()

        if active_log:
            time_in_epoch = active_log["time_in"]
            in_dt = datetime.fromtimestamp(time_in_epoch, tz)
            if in_dt.hour < 8:
                effective_in_epoch = int(
                    in_dt.replace(hour=8, minute=0, second=0, microsecond=0).timestamp()
                )
            else:
                effective_in_epoch = time_in_epoch

            if current_dt.hour >= 17:
                effective_out_epoch = int(
                    current_dt.replace(
                        hour=17, minute=0, second=0, microsecond=0
                    ).timestamp()
                )
            else:
                effective_out_epoch = current_epoch

            # Calculate rendered hours with lunch break deduction
            # Standard lunch break: 12:00 PM - 1:00 PM (1 hour)
            total_duration = max(0, effective_out_epoch - effective_in_epoch)
            
            # Check if the work period crosses lunch time (12:00 - 13:00)
            in_dt = datetime.fromtimestamp(effective_in_epoch, tz)
            out_dt = datetime.fromtimestamp(effective_out_epoch, tz)
            
            # Lunch hours: 12:00 PM to 1:00 PM
            lunch_start = in_dt.replace(hour=12, minute=0, second=0, microsecond=0)
            lunch_end = in_dt.replace(hour=13, minute=0, second=0, microsecond=0)
            
            # Only deduct lunch if work period covers the entire lunch period (12 PM - 1 PM)
            # i.e., time in is at or before 12 PM AND time out is at or after 1 PM
            if in_dt <= lunch_start and out_dt >= lunch_end:
                lunch_duration = 3600  # 1 hour in seconds
                total_duration = max(0, total_duration - lunch_duration)
                logger.info(f"Lunch deducted: in_dt={in_dt}, out_dt={out_dt}, lunch_start={lunch_start}, lunch_end={lunch_end}, total_duration={total_duration}")
            
            rendered_hours = round(total_duration / 3600.0, 2)
            cur.execute(
                "UPDATE time_logs SET time_out = %s, rendered_hours = %s WHERE id = %s",
                (current_epoch, rendered_hours, active_log["id"]),
            )
            conn.commit()
            return jsonify(
                {
                    "success": True,
                    "action": "timeout",
                    "user": user["name"],
                    "message": f"Timed out at {current_dt.strftime('%I:%M %p')}<br>Rendered: {rendered_hours} hrs",
                }
            )
        else:
            is_late = hour > 8 or (hour == 8 and minute > 15)
            cur.execute(
                "INSERT INTO time_logs (user_id, time_in, is_late) VALUES (%s, %s, %s)",
                (user_id, current_epoch, is_late),
            )
            conn.commit()
            status_msg = "Late!" if is_late else "On Time!"
            return jsonify(
                {
                    "success": True,
                    "action": "timein",
                    "user": user["name"],
                    "message": f"Timed in at {current_dt.strftime('%I:%M %p')}<br>Status: {status_msg}",
                }
            )

    except Exception as e:
        conn.rollback()
        logger.error(f"Tap error: {e}")
        return (
            jsonify({"success": False, "message": "System error. Please try again."}),
            500,
        )
    finally:
        cur.close()
        conn.close()


@app.route("/api/tap", methods=["POST"])
def process_tap():
    data = request.json
    if not data:
        return jsonify({"success": False, "message": "Invalid request"}), 400
    rfid_tag = data.get("rfid_tag", "").strip()
    if not rfid_tag:
        return jsonify({"success": False, "message": "Invalid RFID tag"}), 400
    return _process_tap_logic(rfid_tag)


@app.route("/api/qr-scan", methods=["POST"])
def qr_scan():
    data = request.json
    if not data or "qr_data" not in data:
        return jsonify({"success": False, "message": "Invalid data format"}), 400
    qr_data = data.get("qr_data", "").strip()
    if not qr_data:
        return jsonify({"success": False, "message": "Invalid QR data"}), 400
    # Call the shared tap logic directly instead of mutating request.json (which is immutable)
    return _process_tap_logic(qr_data)


# =============================================================================
# ADMIN ROUTES
# =============================================================================


@app.route("/admin")
@admin_required
def admin_dashboard():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT id, name FROM users ORDER BY name")
    all_users = cur.fetchall()

    f = parse_filter_params()
    start_ep, end_ep = f["start_ep"], f["end_ep"]
    filter_time, specific_date = f["filter_time"], f["specific_date"]
    date_from, date_to = f["date_from"], f["date_to"]
    status_filter = f["status_filter"]
    filter_user_id = request.args.get("user_id", "").strip() or None
    page = max(1, int(request.args.get("page", 1) or 1))
    per_page = 30

    if filter_user_id and not filter_user_id.isdigit():
        filter_user_id = None

    query = "SELECT time_logs.*, users.name, users.rfid_tag FROM time_logs JOIN users ON time_logs.user_id = users.id WHERE 1=1"
    params = []
    if filter_user_id:
        query += " AND users.id = %s"
        params.append(int(filter_user_id))
    if start_ep and end_ep:
        query += " AND time_in >= %s AND time_in < %s"
        params.extend([start_ep, end_ep])
    query += " ORDER BY time_in DESC LIMIT 500"
    cur.execute(query, tuple(params))
    all_logs = cur.fetchall()

    if status_filter == "late":
        all_logs = [l for l in all_logs if l.get("is_late")]
    elif status_filter == "ontime":
        all_logs = [l for l in all_logs if not l.get("is_late") and l.get("time_out")]

    # Server-side pagination
    total_logs = len(all_logs)
    total_pages = max(1, (total_logs + per_page - 1) // per_page)
    page = min(page, total_pages)
    logs = all_logs[(page - 1) * per_page : page * per_page]

    stats_query = "SELECT COUNT(*) FILTER (WHERE is_late = TRUE) as total_lates, SUM(rendered_hours) as total_hours FROM time_logs WHERE 1=1"
    stats_params = []
    if filter_user_id:
        stats_query += " AND user_id = %s"
        stats_params.append(int(filter_user_id))
    if start_ep and end_ep:
        stats_query += " AND time_in >= %s AND time_in < %s"
        stats_params.extend([start_ep, end_ep])
    cur.execute(stats_query, tuple(stats_params))
    stats = cur.fetchone()
    total_lates = stats["total_lates"] or 0 if stats else 0
    total_hours = round(stats["total_hours"] or 0.0, 2) if stats else 0.0

    cur.execute("SELECT COUNT(*) as active_count FROM time_logs WHERE time_out IS NULL")
    active_result = cur.fetchone()
    active_staff = active_result["active_count"] if active_result else 0

    avg_where = ""
    avg_params = []
    if filter_user_id:
        avg_where += " AND user_id = %s"
        avg_params.append(int(filter_user_id))
    if start_ep and end_ep:
        avg_where += " AND time_in >= %s AND time_in < %s"
        avg_params.extend([start_ep, end_ep])
    avg_clock_in, avg_clock_out = get_avg_clock_times(cur, avg_where, avg_params)

    logs_data = []
    for log in all_logs:
        log_copy = dict(log)
        if log_copy.get("rendered_hours") is not None:
            log_copy["rendered_hours"] = float(log_copy["rendered_hours"])
        logs_data.append(log_copy)

    cur.close()
    conn.close()

    return render_template(
        "admin.html",
        logs=logs,
        logs_data=logs_data,
        all_users=all_users,
        current_filter=filter_user_id,
        current_time_filter=(
            filter_time if not specific_date and not date_from else "specific"
        ),
        current_specific_date=specific_date,
        total_lates=total_lates,
        total_hours=total_hours,
        active_staff=active_staff,
        avg_clock_in=avg_clock_in,
        avg_clock_out=avg_clock_out,
        current_date_from=date_from,
        current_date_to=date_to,
        current_status_filter=status_filter,
        page=page,
        total_pages=total_pages,
        total_logs=total_logs,
    )


@app.route("/admin/analytics")
@admin_required
def admin_analytics():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT id, name FROM users ORDER BY name")
    all_users = cur.fetchall()

    filter_user_id = request.args.get("user_id", "").strip() or None
    f = parse_filter_params()
    start_ep, end_ep = f["start_ep"], f["end_ep"]
    filter_time, specific_date = f["filter_time"], f["specific_date"]
    date_from, date_to = f["date_from"], f["date_to"]

    if filter_user_id and not filter_user_id.isdigit():
        filter_user_id = None

    query = "SELECT time_logs.*, users.name, users.rfid_tag FROM time_logs JOIN users ON time_logs.user_id = users.id WHERE 1=1"
    params = []
    if filter_user_id:
        query += " AND users.id = %s"
        params.append(int(filter_user_id))
    if start_ep and end_ep:
        query += " AND time_in >= %s AND time_in < %s"
        params.extend([start_ep, end_ep])
    query += " ORDER BY time_in DESC LIMIT 500"
    cur.execute(query, tuple(params))
    logs = cur.fetchall()

    stats_query = "SELECT COUNT(*) FILTER (WHERE is_late = TRUE) as total_lates, SUM(rendered_hours) as total_hours FROM time_logs WHERE 1=1"
    stats_params = []
    if filter_user_id:
        stats_query += " AND user_id = %s"
        stats_params.append(int(filter_user_id))
    if start_ep and end_ep:
        stats_query += " AND time_in >= %s AND time_in < %s"
        stats_params.extend([start_ep, end_ep])
    cur.execute(stats_query, tuple(stats_params))
    stats = cur.fetchone()
    total_lates = stats["total_lates"] or 0 if stats else 0
    total_hours = round(stats["total_hours"] or 0.0, 2) if stats else 0.0

    cur.execute("SELECT COUNT(*) as active_count FROM time_logs WHERE time_out IS NULL")
    active_result = cur.fetchone()
    active_staff = active_result["active_count"] if active_result else 0

    avg_where = ""
    avg_params = []
    if filter_user_id:
        avg_where += " AND user_id = %s"
        avg_params.append(int(filter_user_id))
    if start_ep and end_ep:
        avg_where += " AND time_in >= %s AND time_in < %s"
        avg_params.extend([start_ep, end_ep])
    avg_clock_in, avg_clock_out = get_avg_clock_times(cur, avg_where, avg_params)

    logs_data = []
    for log in logs:
        log_copy = dict(log)
        if log_copy.get("rendered_hours") is not None:
            log_copy["rendered_hours"] = float(log_copy["rendered_hours"])
        logs_data.append(log_copy)

    cur.close()
    conn.close()

    return render_template(
        "analytics_admin.html",
        logs=logs,
        logs_data=logs_data,
        all_users=all_users,
        active_staff=active_staff,
        total_lates=total_lates,
        total_hours=total_hours,
        avg_clock_in=avg_clock_in,
        avg_clock_out=avg_clock_out,
    )


@app.route("/admin/qr-codes")
@admin_required
def admin_qr_codes():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT id, name, rfid_tag FROM users ORDER BY name")
    users = cur.fetchall()
    cur.close()
    conn.close()

    users_with_qr = []
    for user in users:
        qr = qrcode.QRCode(version=1, box_size=10, border=5)
        qr.add_data(user["rfid_tag"])
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buffer = BytesIO()
        img.save(buffer, format="PNG")
        img_str = base64.b64encode(buffer.getvalue()).decode()
        users_with_qr.append(
            {
                "id": user["id"],
                "name": user["name"],
                "rfid_tag": user["rfid_tag"],
                "qr_code": img_str,
            }
        )

    return render_template("admin_qr_codes.html", users=users_with_qr)


@app.route("/admin/users", methods=["GET", "POST"])
@admin_required
def manage_users():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    if request.method == "POST":
        name = request.form["name"]
        email = request.form["email"]
        password = request.form.get("password", "")
        rfid_tag = request.form["rfid_tag"]
        role = request.form["role"]
        ok, err = validate_password(password)
        if not ok:
            flash(err, "error")
        else:
            try:
                cur.execute(
                    "INSERT INTO users (name, email, password_hash, rfid_tag, role) VALUES (%s, %s, %s, %s, %s)",
                    (name, email, generate_password_hash(password), rfid_tag, role),
                )
                conn.commit()
                flash("User created successfully.", "success")
            except Exception:
                conn.rollback()
                flash(
                    "Error creating user. Email or RFID may already be in use.", "error"
                )
    cur.execute("SELECT * FROM users ORDER BY name")
    users = cur.fetchall()
    cur.close()
    conn.close()
    return render_template("admin_users.html", users=users)


@app.route("/admin/users/edit/<int:id>", methods=["POST"])
@admin_required
def edit_user(id):
    conn = get_db_connection()
    cur = conn.cursor()
    name = request.form["name"]
    email = request.form["email"]
    rfid = request.form["rfid_tag"]
    role = request.form["role"]
    pw = request.form.get("new_password", "").strip()
    try:
        if pw:
            ok, err = validate_password(pw)
            if not ok:
                flash(err, "error")
                return redirect(url_for("manage_users"))
            cur.execute(
                "UPDATE users SET name=%s, email=%s, rfid_tag=%s, role=%s, password_hash=%s WHERE id=%s",
                (name, email, rfid, role, generate_password_hash(pw), id),
            )
        else:
            cur.execute(
                "UPDATE users SET name=%s, email=%s, rfid_tag=%s, role=%s WHERE id=%s",
                (name, email, rfid, role, id),
            )
        conn.commit()
        flash("User updated.", "success")
    except Exception:
        conn.rollback()
        flash("Update failed.", "error")
    finally:
        cur.close()
        conn.close()
    return redirect(url_for("manage_users"))


@app.route("/admin/users/delete/<int:id>", methods=["POST"])
@admin_required
def delete_user(id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM users WHERE id = %s AND id != %s", (id, session["user_id"])
    )
    conn.commit()
    cur.close()
    conn.close()
    flash("User deleted.", "success")
    return redirect(url_for("manage_users"))


@app.route("/admin/logs/clear", methods=["POST"])
@admin_required
def clear_logs():
    filter_user_id = request.args.get("user_id")
    conn = get_db_connection()
    cur = conn.cursor()
    if filter_user_id:
        cur.execute("DELETE FROM time_logs WHERE user_id = %s", (filter_user_id,))
    else:
        cur.execute("TRUNCATE TABLE time_logs RESTART IDENTITY CASCADE")
    conn.commit()
    cur.close()
    conn.close()
    return redirect(url_for("admin_dashboard"))


# =============================================================================
# API ROUTES
# =============================================================================


@app.route("/api/chart-data")
@admin_required
def chart_data():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT time_in FROM time_logs ORDER BY time_in DESC LIMIT 1000")
    records = cur.fetchall()
    cur.close()
    conn.close()
    counts = {}
    for r in records:
        d = time.strftime("%Y-%m-%d", time.localtime(r["time_in"]))
        counts[d] = counts.get(d, 0) + 1
    sorted_dates = sorted(counts.keys())[-7:]
    return jsonify({"labels": sorted_dates, "data": [counts[d] for d in sorted_dates]})


@app.route("/api/active-staff-data")
@admin_required
def active_staff_data():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "SELECT user_id, time_in FROM time_logs ORDER BY time_in DESC LIMIT 2000"
    )
    records = cur.fetchall()
    cur.close()
    conn.close()

    active_by_date = {}
    for r in records:
        utc_dt = datetime.fromtimestamp(r["time_in"], tz=timezone.utc)
        local_dt = utc_dt.astimezone(tz)
        date_str = local_dt.strftime("%Y-%m-%d")
        if date_str not in active_by_date:
            active_by_date[date_str] = set()
        active_by_date[date_str].add(r["user_id"])

    sorted_dates = sorted(active_by_date.keys())[-7:]
    return jsonify(
        {"labels": sorted_dates, "data": [len(active_by_date[d]) for d in sorted_dates]}
    )


@app.route("/api/admin/recent-activity")
@admin_required
def admin_recent_activity():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT time_logs.id, users.name, time_logs.time_in, time_logs.time_out
        FROM time_logs JOIN users ON time_logs.user_id = users.id
        ORDER BY time_logs.id DESC LIMIT 5
    """
    )
    logs = cur.fetchall()
    cur.close()
    conn.close()

    activity = []
    for log in logs:
        action = "Time Out" if log["time_out"] else "Time In"
        timestamp = log["time_out"] if log["time_out"] else log["time_in"]
        activity.append(
            {
                "id": log["id"],
                "name": log["name"],
                "action": action,
                "timestamp": timestamp,
            }
        )
    return jsonify(activity)


@app.route("/api/user/recent-activity")
@login_required
def user_recent_activity():
    user_id = session["user_id"]
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "SELECT id, time_in, time_out FROM time_logs WHERE user_id = %s ORDER BY id DESC LIMIT 5",
        (user_id,),
    )
    logs = cur.fetchall()
    cur.close()
    conn.close()

    activity = []
    for log in logs:
        action = "Time Out" if log["time_out"] else "Time In"
        timestamp = log["time_out"] if log["time_out"] else log["time_in"]
        activity.append({"id": log["id"], "action": action, "timestamp": timestamp})
    return jsonify(activity)


@app.route("/api/analytics", methods=["POST"])
def api_analytics():
    data = None
    if request.is_json:
        data = request.get_json(silent=True)
    if not data:
        try:
            data = json.loads(request.get_data(as_text=True) or "{}")
        except Exception:
            data = {}

    event = (data or {}).get("event") or (data or {}).get("event_type")
    metadata = (data or {}).get("metadata", {})

    if not event:
        logger.warning("Analytics endpoint called without event payload")
        return jsonify({"success": False, "message": "event is required"}), 200

    logger.info(
        "Analytics event: %s; metadata: %s; user_id: %s",
        event,
        metadata,
        session.get("user_id"),
    )
    return jsonify({"success": True, "event": event})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
