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
def format_epoch(epoch_time, fmt="%Y-%m-%d %I:%M %p"):
    if epoch_time is None:
        return "--"
    utc_dt = datetime.fromtimestamp(epoch_time, tz=timezone.utc)
    local_dt = utc_dt.astimezone(tz)
    return local_dt.strftime(fmt)


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


def get_routine_markers(cur, base_where, params):
    """Return Median (P50) for AM-In, AM-Out, PM-In, PM-Out as formatted strings."""
    avg_sql = f"""
        WITH daily_markers AS (
            SELECT
                user_id,
                MIN(CASE WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(time_in) AT TIME ZONE 'Asia/Singapore') < 12 THEN
                    EXTRACT(HOUR FROM TO_TIMESTAMP(time_in) AT TIME ZONE 'Asia/Singapore') * 3600 +
                    EXTRACT(MINUTE FROM TO_TIMESTAMP(time_in) AT TIME ZONE 'Asia/Singapore') * 60 +
                    EXTRACT(SECOND FROM TO_TIMESTAMP(time_in) AT TIME ZONE 'Asia/Singapore') END) as am_in_s,
                MAX(CASE WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(time_out) AT TIME ZONE 'Asia/Singapore') <= 13 THEN
                    EXTRACT(HOUR FROM TO_TIMESTAMP(time_out) AT TIME ZONE 'Asia/Singapore') * 3600 +
                    EXTRACT(MINUTE FROM TO_TIMESTAMP(time_out) AT TIME ZONE 'Asia/Singapore') * 60 +
                    EXTRACT(SECOND FROM TO_TIMESTAMP(time_out) AT TIME ZONE 'Asia/Singapore') END) as am_out_s,
                MIN(CASE WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(time_in) AT TIME ZONE 'Asia/Singapore') >= 12 THEN
                    EXTRACT(HOUR FROM TO_TIMESTAMP(time_in) AT TIME ZONE 'Asia/Singapore') * 3600 +
                    EXTRACT(MINUTE FROM TO_TIMESTAMP(time_in) AT TIME ZONE 'Asia/Singapore') * 60 +
                    EXTRACT(SECOND FROM TO_TIMESTAMP(time_in) AT TIME ZONE 'Asia/Singapore') END) as pm_in_s,
                MAX(CASE WHEN EXTRACT(HOUR FROM TO_TIMESTAMP(time_out) AT TIME ZONE 'Asia/Singapore') > 13 THEN
                    EXTRACT(HOUR FROM TO_TIMESTAMP(time_out) AT TIME ZONE 'Asia/Singapore') * 3600 +
                    EXTRACT(MINUTE FROM TO_TIMESTAMP(time_out) AT TIME ZONE 'Asia/Singapore') * 60 +
                    EXTRACT(SECOND FROM TO_TIMESTAMP(time_out) AT TIME ZONE 'Asia/Singapore') END) as pm_out_s
            FROM time_logs
            WHERE 1=1 {base_where}
            GROUP BY user_id, (TO_TIMESTAMP(time_in) AT TIME ZONE 'Asia/Singapore')::date
        )
        SELECT 
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY am_in_s) as am_in,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY am_out_s) as am_out,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pm_in_s) as pm_in,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pm_out_s) as pm_out
        FROM daily_markers
    """
    cur.execute(avg_sql, tuple(params))
    r = cur.fetchone()
    
    def fmt(secs):
        if secs is None: return None
        dt = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=int(secs))
        return format_time_12h(dt)

    return {
        "am_in": fmt(r["am_in"]),
        "am_out": fmt(r["am_out"]),
        "pm_in": fmt(r["pm_in"]),
        "pm_out": fmt(r["pm_out"])
    }


def validate_password(password):
    """Returns (ok, error_message). Enforces minimum 8 chars server-side."""
    if not password or len(password.strip()) < 8:
        return False, "Password must be at least 8 characters."
    return True, None


def validate_user_name(name):
    """Returns (ok, error_message). Basic name check."""
    if not name or len(name.strip()) < 3:
        return False, "Full Name must be at least 3 characters."
    return True, None


def validate_user_email(email):
    """Returns (ok, error_message). Basic email check."""
    if not email or "@" not in email or "." not in email:
        return False, "Please enter a valid email address."
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
    per_page = max(10, int(request.args.get("per_page", 20) or 20))

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # --- Unified SQL Query construction ---
    query = "SELECT * FROM time_logs WHERE user_id = %s"
    params = [user_id]
    
    if start_ep and end_ep:
        query += " AND time_in >= %s AND time_in < %s"
        params.extend([start_ep, end_ep])
    
    if status_filter == "late":
        query += " AND is_late = TRUE"
    elif status_filter == "ontime":
        query += " AND is_late = FALSE"
    
    # Get total count for pagination before applying LIMIT
    cur.execute(f"SELECT COUNT(*) FROM ({query}) as count_query", tuple(params))
    total_logs = cur.fetchone()["count"]
    
    total_pages = max(1, (total_logs + per_page - 1) // per_page)
    page = min(page, total_pages)
    offset = (page - 1) * per_page
    
    query += " ORDER BY time_in DESC LIMIT %s OFFSET %s"
    params.extend([per_page, offset])
    
    cur.execute(query, tuple(params))
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

    # Determine the label for the cards based on current filters
    avg_label = "(Today)"
    if start_ep and end_ep:
        if filter_time == "today": avg_label = "(Today)"
        elif filter_time == "week": avg_label = "(This Week)"
        elif filter_time == "month": avg_label = "(This Month)"
        elif filter_time == "year": avg_label = "(This Year)"
        elif specific_date: avg_label = f"({specific_date})"
        elif date_from and date_to:
            if date_from == date_to: avg_label = f"({date_from})"
            else: avg_label = f"({date_from} - {date_to})"
        else: avg_label = "(Filtered)"
    else:
        avg_label = "(All Time)"

    # Get the routine timeline markers (Median Arrival/Departure/Lunch)
    routine = get_routine_markers(cur, " AND user_id = %s" + (" AND time_in >= %s AND time_in < %s" if start_ep and end_ep else ""), [user_id] + ([start_ep, end_ep] if start_ep and end_ep else []))

    # Prepare logs_data for template (if needed)
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
        routine=routine,
        avg_label=avg_label,
        page=page,
        total_pages=total_pages,
        total_logs=total_logs,
        per_page=per_page,
    )
    
@app.route("/user/radar")
@login_required
def user_radar():
    """
    Redirect to analytics page. Radar chart is now integrated in My Analytics.
    """
    return redirect(url_for("user_analytics"))


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

    routine = get_routine_markers(cur, avg_where, avg_params)

    # Determine the label for the cards dynamically
    avg_label = "(Today)"
    if start_ep and end_ep:
        if filter_time == "today": avg_label = "(Today)"
        elif filter_time == "week": avg_label = "(This Week)"
        elif filter_time == "month": avg_label = "(This Month)"
        elif filter_time == "year": avg_label = "(This Year)"
        elif specific_date: avg_label = f"({specific_date})"
        elif date_from and date_to:
            if date_from == date_to: avg_label = f"({date_from})"
            else: avg_label = f"({date_from} - {date_to})"
        else: avg_label = "(Filtered)"
    else:
        avg_label = "(All Time)"

    logs_data = []
    for log in logs:
        log_copy = dict(log)
        if log_copy.get("rendered_hours") is not None:
            log_copy["rendered_hours"] = float(log_copy["rendered_hours"])
        logs_data.append(log_copy)

    # Calculate metrics for Radar tab (last 30 days)
    # Calculate stats for the current user (last 30 days)
    thirty_days_ago_dt = datetime.now() - timedelta(days=30)
    thirty_days_ago_epoch = int(thirty_days_ago_dt.timestamp())
    
    def calculate_user_metrics(logs_list):
        if not logs_list:
            return {m: 0 for m in ["Punctuality", "Shift Completion", "Reliability", "Stability", "Integrity", "Retention"]}
        
        total = len(logs_list)
        finished_logs = [l for l in logs_list if l['time_out']]
        total_finished = len(finished_logs)
        
        punctual = len([l for l in logs_list if not l['is_late']])
        punctuality = (punctual / total) * 100
        
        # Shift Completion: Check for at least 4 hours (standard session)
        completed = 0
        for l in finished_logs:
            if l['time_in']:
                duration = l['time_out'] - l['time_in']
                if duration >= 14400: completed += 1 # 4-hour threshold
        shift_completion = (completed / total_finished * 100) if total_finished > 0 else 0
        
        # Reliability: Attendance frequency over 22 work days
        reliability = min((total / 22) * 100, 100)
        
        # Consistent clock-in stability (use earliest daily clock-in, excluding today)
        today_str = datetime.now(tz=tz).strftime('%Y-%m-%d')
        daily_first_clock_ins = {}
        for l in logs_list:
            dt = datetime.fromtimestamp(l['time_in'], tz=tz)
            date_str = dt.strftime('%Y-%m-%d')
            if date_str == today_str:
                continue  # Skip today — session may still be in progress
            mins = dt.hour * 60 + dt.minute
            if date_str not in daily_first_clock_ins or mins < daily_first_clock_ins[date_str]:
                daily_first_clock_ins[date_str] = mins
        
        clock_in_mins = list(daily_first_clock_ins.values())
        
        if len(clock_in_mins) > 1:
            try:
                std_dev = statistics.stdev(clock_in_mins)
                stability = max(100 - (std_dev * 2), 0) # 100% score for 0 variance
            except:
                stability = 0
        else:
            stability = 0
            
        integrity = (total_finished / total) * 100
        
        # Retention: did the user's LAST clock-out of each day happen at or after 5 PM?
        # Per-day check avoids penalizing split-shift workers for their mid-day clock-outs.
        today_str_ret = datetime.now(tz=tz).strftime('%Y-%m-%d')
        daily_last_out = {}
        for l in finished_logs:
            dt_in = datetime.fromtimestamp(l['time_in'], tz=tz)
            dt_out = datetime.fromtimestamp(l['time_out'], tz=tz)
            date_str = dt_in.strftime('%Y-%m-%d')
            if date_str == today_str_ret:
                continue  # Skip today — may still be in progress
            out_mins = dt_out.hour * 60 + dt_out.minute
            if date_str not in daily_last_out or out_mins > daily_last_out[date_str]:
                daily_last_out[date_str] = out_mins

        retention_days = len(daily_last_out)
        retention_count = sum(1 for mins in daily_last_out.values() if mins >= 17 * 60)
        retention = (retention_count / retention_days * 100) if retention_days > 0 else 0

        return {
            "Punctuality": round(punctuality),
            "Shift Completion": round(shift_completion),
            "Reliability": round(reliability),
            "Stability": round(stability),
            "Integrity": round(integrity),
            "Retention": round(retention)
        }

    # Get user department
    cur.execute("SELECT department FROM users WHERE id = %s", (user_id,))
    u_row = cur.fetchone()
    user_dept = u_row['department'] if u_row else 'General'

    # 1. My Metrics (Last 30 days)
    cur.execute("SELECT time_in, time_out, is_late FROM time_logs WHERE user_id = %s AND time_in > %s", (user_id, thirty_days_ago_epoch))
    my_radar_logs = cur.fetchall()
    my_scores = calculate_user_metrics(my_radar_logs)
    total_logs_count = len(my_radar_logs)

    # 2. Dept Average Metrics (Last 30 days)
    cur.execute("""
        SELECT tl.user_id, tl.time_in, tl.time_out, tl.is_late 
        FROM time_logs tl
        JOIN users u ON tl.user_id = u.id
        WHERE u.department = %s AND tl.time_in > %s
    """, (user_dept, thirty_days_ago_epoch))
    dept_radar_logs = cur.fetchall()
    
    user_map = {}
    for l in dept_radar_logs:
        uid = l['user_id']
        if uid not in user_map: user_map[uid] = []
        user_map[uid].append(l)
    
    all_user_metrics = [calculate_user_metrics(u_logs) for u_logs in user_map.values()]
    avg_scores = {m: 0 for m in ["Punctuality", "Shift Completion", "Reliability", "Stability", "Integrity", "Retention"]}
    
    if all_user_metrics:
        for m in avg_scores.keys():
            avg_scores[m] = round(sum(u[m] for u in all_user_metrics) / len(all_user_metrics))

    metrics_json = [
        {"metric": m, "me": my_scores[m], "avg": avg_scores[m]} 
        for m in ["Punctuality", "Shift Completion", "Reliability", "Stability", "Integrity", "Retention"]
    ]

    # Additional KPI: Longest Shift
    cur.execute("SELECT MAX(rendered_hours) FROM time_logs WHERE user_id = %s" + 
                (" AND time_in >= %s AND time_in < %s" if start_ep and end_ep else ""), 
                tuple([user_id] + ([start_ep, end_ep] if start_ep and end_ep else [])))
    m_row = cur.fetchone()
    max_shift = round(float(m_row['max'] or 0), 1) if m_row else 0

    cur.close()
    conn.close()

    return render_template(
        "analytics_user.html",
        logs=logs,
        logs_data=logs_data,
        total_lates=stats["total_lates"] or 0 if stats else 0,
        total_hours=round(stats["total_hours"] or 0.0, 2) if stats else 0.0,
        routine=routine,
        avg_label=avg_label,
        max_shift=max_shift,
        metrics=metrics_json,
        total_logs=total_logs_count,
        department=user_dept
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
            # Shift detection logic:
            # Morning shift: Late after 8:15 AM
            # Afternoon shift (12 PM onwards): Late after 1:15 PM (13:15)
            if hour < 12:
                is_late = hour > 8 or (hour == 8 and minute > 15)
            else:
                is_late = hour > 13 or (hour == 13 and minute > 15)
            log_date = current_dt.strftime('%A, %B %nd, %Y').replace(' 0', ' ')
            # More standard header: "Monday, March 30, 2026"
            log_date = current_dt.strftime('%A, %B %d, %Y')
            
            cur.execute(
                "INSERT INTO time_logs (user_id, time_in, is_late, log_date) VALUES (%s, %s, %s, %s)",
                (user_id, current_epoch, is_late, log_date),
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
# --- Admin Logs API (JSON) ---
@app.route("/api/admin/logs")
@admin_required
def api_admin_logs():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # 1. Reuse the robust filtering logic from the main dashboard
    f = parse_filter_params()
    start_ep, end_ep = f["start_ep"], f["end_ep"]
    status_filter = f["status_filter"]
    filter_user_id = request.args.get("user_id", "").strip() or None
    search_query = request.args.get("search", "").strip()
    page = max(1, int(request.args.get("page", 1) or 1))
    per_page = max(1, int(request.args.get("per_page", 20) or 20))

    if filter_user_id and not filter_user_id.isdigit():
        filter_user_id = None

    # Base query for logs
    query = "SELECT time_logs.*, users.name, users.rfid_tag FROM time_logs JOIN users ON time_logs.user_id = users.id WHERE 1=1"
    params = []
    if filter_user_id:
        query += " AND users.id = %s"
        params.append(int(filter_user_id))
    if search_query:
        query += " AND (users.name ILIKE %s OR users.department ILIKE %s OR users.rfid_tag ILIKE %s OR users.email ILIKE %s)"
        like_term = f"%{search_query}%"
        params.extend([like_term] * 4)
    if start_ep and end_ep:
        query += " AND time_in >= %s AND time_in < %s"
        params.extend([start_ep, end_ep])
    
    if status_filter == "late":
        query += " AND is_late = TRUE"
    elif status_filter == "ontime":
        query += " AND is_late = FALSE"

    # Get total count for pagination
    cur.execute(f"SELECT COUNT(*) FROM ({query}) as count_query", tuple(params))
    total_logs = cur.fetchone()["count"]
    total_pages = max(1, (total_logs + per_page - 1) // per_page)
    page = min(page, total_pages)
    offset = (page - 1) * per_page

    query += " ORDER BY time_in DESC LIMIT %s OFFSET %s"
    params.extend([per_page, offset])
    
    cur.execute(query, tuple(params))
    logs = cur.fetchall()

    # 2. Get Statistics (for real-time update of summary cards)
    stats_query = "SELECT COUNT(*) FILTER (WHERE is_late = TRUE) as total_lates, SUM(rendered_hours) as total_hours FROM time_logs WHERE 1=1"
    stats_params = []
    if filter_user_id:
        stats_query += " AND user_id = %s"
        stats_params.append(int(filter_user_id))
    if start_ep and end_ep:
        stats_query += " AND time_in >= %s AND time_in < %s"
        stats_params.extend([start_ep, end_ep])
    
    # Respect status filter in stats too
    if status_filter == "late":
        stats_query += " AND is_late = TRUE"
    elif status_filter == "ontime":
        stats_query += " AND is_late = FALSE"

    cur.execute(stats_query, tuple(stats_params))
    stats = cur.fetchone()
    
    total_lates = stats["total_lates"] or 0
    total_hours = round(stats["total_hours"] or 0.0, 2)

    # 3. Add Average Clock Times to the API
    avg_where = ""
    avg_params = []
    if filter_user_id:
        avg_where += " AND user_id = %s"
        avg_params.append(int(filter_user_id))
    if start_ep and end_ep:
        avg_where += " AND time_in >= %s AND time_in < %s"
        avg_params.extend([start_ep, end_ep])
    
    # If no filter, defaults to today's global average as seen on dashboard
    if not avg_where:
        s_td, e_td = get_time_range_epochs("today")
        avg_clock_in, avg_clock_out = get_avg_clock_times(cur, " AND time_in >= %s AND time_in < %s", [s_td, e_td])
    else:
        avg_clock_in, avg_clock_out = get_avg_clock_times(cur, avg_where, avg_params)

    # 4. Format logs for easy JS consumption
    formatted_logs = []
    for log in logs:
        dt_in = datetime.fromtimestamp(log["time_in"], tz=timezone.utc).astimezone(tz)
        dt_out = None
        if log["time_out"]:
            dt_out = datetime.fromtimestamp(log["time_out"], tz=timezone.utc).astimezone(tz)
            
        formatted_logs.append({
            "id": log["id"],
            "name": log["name"],
            "rfid_tag": log["rfid_tag"],
            "date": dt_in.strftime("%b %d, %Y"),
            "day": dt_in.strftime("%A"),
            "time_in_label": format_time_12h(dt_in),
            "time_in_raw": log["time_in"],
            "time_out_label": format_time_12h(dt_out) if dt_out else None,
            "time_out_raw": log["time_out"],
            "is_late": log["is_late"],
            "rendered_hours": float(log["rendered_hours"]) if log["rendered_hours"] is not None else None
        })

    cur.close()
    conn.close()

    return jsonify({
        "success": True,
        "logs": formatted_logs,
        "pagination": {
            "total_logs": total_logs,
            "total_pages": total_pages,
            "page": page,
            "per_page": per_page,
            "start_entry": ((page - 1) * per_page) + 1 if total_logs > 0 else 0,
            "end_entry": min(page * per_page, total_logs)
        },
        "stats": {
            "total_lates": total_lates,
            "total_hours": total_hours,
            "avg_clock_in": avg_clock_in,
            "avg_clock_out": avg_clock_out
        }
    })


# --- User Logs API (JSON) ---
@app.route("/api/user/logs")
@login_required
def api_user_logs():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    user_id = session.get("user_id")
    f = parse_filter_params()
    start_ep, end_ep = f["start_ep"], f["end_ep"]
    status_filter = f["status_filter"]
    page = max(1, int(request.args.get("page", 1) or 1))
    per_page = max(1, int(request.args.get("per_page", 20) or 20))

    # Base query for personal logs
    query = "SELECT * FROM time_logs WHERE user_id = %s"
    params = [user_id]
    if start_ep and end_ep:
        query += " AND time_in >= %s AND time_in < %s"
        params.extend([start_ep, end_ep])
    
    if status_filter == "late":
        query += " AND is_late = TRUE"
    elif status_filter == "ontime":
        query += " AND is_late = FALSE"
    
    # Get total count
    cur.execute(f"SELECT COUNT(*) FROM ({query}) as count_query", tuple(params))
    total_logs = cur.fetchone()["count"]
    total_pages = max(1, (total_logs + per_page - 1) // per_page)
    page = min(page, total_pages)
    offset = (page - 1) * per_page

    query += " ORDER BY time_in DESC LIMIT %s OFFSET %s"
    params.extend([per_page, offset])

    cur.execute(query, tuple(params))
    logs = cur.fetchall()

    cur.execute("SELECT COUNT(*) FILTER (WHERE is_late = TRUE) as total_lates, SUM(rendered_hours) as total_hours FROM time_logs WHERE user_id = %s" + 
                (" AND time_in >= %s AND time_in < %s" if start_ep and end_ep else ""), 
                tuple([user_id] + ([start_ep, end_ep] if start_ep and end_ep else [])))
    stats = cur.fetchone()
    
    # Calculate filtered averages for the cards
    avg_where = "AND user_id = %s"
    avg_params = [user_id]
    
    # Determine the label for the cards dynamically
    avg_label = "(Today)"
    f_time = f["filter_time"]
    d_from = f["date_from"]
    d_to = f["date_to"]
    s_date = f["specific_date"]

    if start_ep and end_ep:
        avg_where += " AND time_in >= %s AND time_in < %s"
        avg_params += [start_ep, end_ep]
        if f_time == "today": avg_label = "(Today)"
        elif f_time == "week": avg_label = "(This Week)"
        elif f_time == "month": avg_label = "(This Month)"
        elif f_time == "year": avg_label = "(This Year)"
        elif s_date: avg_label = f"({s_date})"
        elif d_from and d_to:
            if d_from == d_to: avg_label = f"({d_from})"
            else: avg_label = f"({d_from} - {d_to})"
        else: avg_label = "(Filtered)"
    else:
        avg_label = "(All Time)"

    routine = get_routine_markers(cur, avg_where, avg_params)

    formatted_logs = []
    for log in logs:
        dt_in = datetime.fromtimestamp(log["time_in"], tz=timezone.utc).astimezone(tz)
        dt_out = None
        if log["time_out"]:
            dt_out = datetime.fromtimestamp(log["time_out"], tz=timezone.utc).astimezone(tz)
            
        formatted_logs.append({
            "id": log["id"],
            "date": dt_in.strftime("%b %d, %Y"),
            "day": dt_in.strftime("%A"),
            "time_in_label": format_time_12h(dt_in),
            "time_in_raw": log["time_in"],
            "time_out_label": format_time_12h(dt_out) if dt_out else None,
            "time_out_raw": log["time_out"],
            "is_late": log["is_late"],
            "rendered_hours": float(log["rendered_hours"]) if log["rendered_hours"] is not None else None
        })

    cur.close()
    conn.close()

    return jsonify({
        "success": True,
        "logs": formatted_logs,
        "pagination": {
            "total_logs": total_logs,
            "total_pages": total_pages,
            "page": page,
            "per_page": per_page,
            "start_entry": ((page - 1) * per_page) + 1 if total_logs > 0 else 0,
            "end_entry": min(page * per_page, total_logs)
        },
        "stats": {
            "total_lates": stats["total_lates"] or 0,
            "total_hours": round(stats["total_hours"] or 0.0, 2),
            "routine": routine,
            "avg_label": avg_label
        }
    })


# =============================================================================
# ADMIN ROUTES
# =============================================================================


@app.route("/admin")
@admin_required
def admin_dashboard():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT id, name, department FROM users ORDER BY name")
    all_users = cur.fetchall()

    f = parse_filter_params()
    start_ep, end_ep = f["start_ep"], f["end_ep"]
    filter_time, specific_date = f["filter_time"], f["specific_date"]
    date_from, date_to = f["date_from"], f["date_to"]
    status_filter = f["status_filter"]
    filter_user_id = request.args.get("user_id", "").strip() or None
    search_query = request.args.get("search", "").strip()
    page = max(1, int(request.args.get("page", 1) or 1))
    per_page = max(10, int(request.args.get("per_page", 20) or 20))

    if filter_user_id and not filter_user_id.isdigit():
        filter_user_id = None

    current_user_name = None
    if filter_user_id:
        cur.execute("SELECT name FROM users WHERE id = %s", (int(filter_user_id),))
        u_row = cur.fetchone()
        if u_row:
            current_user_name = u_row["name"]

    query = "SELECT time_logs.*, users.name, users.rfid_tag, users.department FROM time_logs JOIN users ON time_logs.user_id = users.id WHERE 1=1"
    params = []
    if filter_user_id:
        query += " AND users.id = %s"
        params.append(int(filter_user_id))
    if search_query:
        query += " AND (users.name ILIKE %s OR users.department ILIKE %s OR users.rfid_tag ILIKE %s OR users.email ILIKE %s)"
        like_term = f"%{search_query}%"
        params.extend([like_term]*4)
    if start_ep and end_ep:
        query += " AND time_in >= %s AND time_in < %s"
        params.extend([start_ep, end_ep])
    
    if status_filter == "late":
        query += " AND is_late = TRUE"
    elif status_filter == "ontime":
        query += " AND is_late = FALSE"
    
    # Get total count for pagination
    cur.execute(f"SELECT COUNT(*) FROM ({query}) as count_query", tuple(params))
    total_count_row = cur.fetchone()
    total_logs = total_count_row["count"] if total_count_row else 0
    total_pages = max(1, (total_logs + per_page - 1) // per_page)
    page = min(page, total_pages)
    offset = (page - 1) * per_page

    query += " ORDER BY time_in DESC LIMIT %s OFFSET %s"
    params.extend([per_page, offset])
    
    cur.execute(query, tuple(params))
    logs = cur.fetchall()

    stats_query = "SELECT COUNT(*) FILTER (WHERE is_late = TRUE) as total_lates, SUM(rendered_hours) as total_hours FROM time_logs WHERE 1=1"
    stats_params = []
    if filter_user_id:
        stats_query += " AND user_id = %s"
        stats_params.append(int(filter_user_id))
    if search_query:
        stats_query += " AND user_id IN (SELECT id FROM users WHERE name ILIKE %s OR department ILIKE %s OR rfid_tag ILIKE %s OR email ILIKE %s)"
        like_term = f"%{search_query}%"
        stats_params.extend([like_term]*4)
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
    if search_query:
        avg_where += " AND user_id IN (SELECT id FROM users WHERE name ILIKE %s OR department ILIKE %s OR rfid_tag ILIKE %s OR email ILIKE %s)"
        like_term = f"%{search_query}%"
        avg_params.extend([like_term]*4)
    if start_ep and end_ep:
        avg_where += " AND time_in >= %s AND time_in < %s"
        avg_params.extend([start_ep, end_ep])
    if not avg_where:
        s_td, e_td = get_time_range_epochs("today")
        avg_clock_in, avg_clock_out = get_avg_clock_times(cur, " AND time_in >= %s AND time_in < %s", [s_td, e_td])
    else:
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
        "admin.html",
        logs=logs,
        logs_data=logs_data,
        all_users=all_users,
        current_filter=filter_user_id,
        current_user_name=current_user_name,
        current_search=search_query,
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
        per_page=per_page,
    )


@app.route("/admin/analytics")
@admin_required
def admin_analytics():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT id, name, department FROM users ORDER BY name")
    all_users = cur.fetchall()

    dept_membership = {}
    for user in all_users:
        dept = user["department"] or "General"
        dept_membership[dept] = dept_membership.get(dept, 0) + 1

    filter_user_id = request.args.get("user_id", "").strip() or None
    f = parse_filter_params()
    start_ep, end_ep = f["start_ep"], f["end_ep"]
    filter_time, specific_date = f["filter_time"], f["specific_date"]
    date_from, date_to = f["date_from"], f["date_to"]

    if filter_user_id and not filter_user_id.isdigit():
        filter_user_id = None

    query = "SELECT time_logs.*, users.name, users.rfid_tag, users.department FROM time_logs JOIN users ON time_logs.user_id = users.id WHERE 1=1"
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

    today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    cur.execute("SELECT COUNT(DISTINCT user_id) as present_today_count FROM time_logs WHERE time_in >= %s", (today_start,))
    present_today = cur.fetchone()["present_today_count"] or 0
    absent_today = len(all_users) - present_today

    cur.execute("SELECT users.department, COUNT(*) as active_count FROM time_logs JOIN users ON time_logs.user_id = users.id WHERE time_out IS NULL AND time_in >= %s GROUP BY users.department", (today_start,))
    active_rows = cur.fetchall()
    dept_active = {row["department"] or "General": row["active_count"] for row in active_rows}
    active_staff = sum(dept_active.values())

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
        absent_today=absent_today,
        dept_membership=dept_membership,
        dept_active=dept_active,
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
    cur.execute("SELECT * FROM users ORDER BY name")
    users = cur.fetchall()

    # Get unique departments for auto-suggestion
    cur.execute("SELECT DISTINCT department FROM users WHERE department IS NOT NULL AND department != '' ORDER BY department")
    departments = [row["department"] for row in cur.fetchall()]

    if request.method == "POST":
        name = request.form.get("name", "").strip()
        email = request.form.get("email", "").strip()
        password = request.form.get("password", "")
        rfid_tag = request.form.get("rfid_tag", "").strip()
        role = request.form["role"]
        department = request.form.get("department", "General").strip() or "General"

        # Validation
        v_name_ok, v_name_err = validate_user_name(name)
        v_email_ok, v_email_err = validate_user_email(email)
        v_pass_ok, v_pass_err = validate_password(password)

        if not v_name_ok:
            flash(v_name_err, "error")
        elif not v_email_ok:
            flash(v_email_err, "error")
        elif not v_pass_ok:
            flash(v_pass_err, "error")
        elif not rfid_tag:
            flash("RFID Tag / ID Number is required.", "error")
        else:
            try:
                cur.execute(
                    "INSERT INTO users (name, email, password_hash, rfid_tag, role, department) VALUES (%s, %s, %s, %s, %s, %s)",
                    (name, email, generate_password_hash(password), rfid_tag, role, department),
                )
                conn.commit()
                flash("User created successfully.", "success")
                return redirect(url_for("manage_users"))
            except Exception:
                conn.rollback()
                flash("Error creating user. Email or RFID may already be in use.", "error")

    cur.close()
    conn.close()
    return render_template("admin_users.html", users=users, departments=departments)


@app.route("/admin/users/edit/<int:id>", methods=["POST"])
@admin_required
def edit_user(id):
    conn = get_db_connection()
    cur = conn.cursor()
    name = request.form["name"].strip()
    email = request.form["email"].strip()
    rfid = request.form["rfid_tag"].strip()
    role = request.form["role"]
    department = request.form.get("department", "General").strip() or "General"
    pw = request.form.get("new_password", "").strip()

    # Validation
    v_name_ok, v_name_err = validate_user_name(name)
    v_email_ok, v_email_err = validate_user_email(email)

    if not v_name_ok:
        flash(v_name_err, "error")
        return redirect(url_for("manage_users"))
    if not v_email_ok:
        flash(v_email_err, "error")
        return redirect(url_for("manage_users"))

    try:
        if pw:
            v_pass_ok, v_pass_err = validate_password(pw)
            if not v_pass_ok:
                flash(v_pass_err, "error")
                return redirect(url_for("manage_users"))
            cur.execute(
                "UPDATE users SET name=%s, email=%s, rfid_tag=%s, role=%s, password_hash=%s, department=%s WHERE id=%s",
                (name, email, rfid, role, generate_password_hash(pw), department, id),
            )
        else:
            cur.execute(
                "UPDATE users SET name=%s, email=%s, rfid_tag=%s, role=%s, department=%s WHERE id=%s",
                (name, email, rfid, role, department, id),
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


@app.route("/admin/users/clear-logs/<int:id>", methods=["POST"])
@admin_required
def clear_user_logs(id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM time_logs WHERE user_id = %s", (id,))
    conn.commit()
    cur.close()
    conn.close()
    flash("Successfully cleared all logs for this user.", "success")
    return redirect(url_for("manage_users"))


@app.route("/admin/users/bulk-clear-logs", methods=["POST"])
@admin_required
def bulk_clear_logs():
    # Expecting a list of IDs from form data
    user_ids = request.form.getlist("selected_users[]")
    if not user_ids:
        flash("No users selected.", "error")
        return redirect(url_for("manage_users"))

    conn = get_db_connection()
    cur = conn.cursor()
    # Safely delete logs for all selected IDs
    cur.execute("DELETE FROM time_logs WHERE user_id = ANY(%s::int[])", (user_ids,))
    conn.commit()
    count = cur.rowcount
    cur.close()
    conn.close()

    flash(f"Successfully cleared logs for {len(user_ids)} selected users ({count} total records).", "success")
    return redirect(url_for("manage_users"))


@app.route("/admin/users/bulk-delete", methods=["POST"])
@admin_required
def bulk_delete_users():
    user_ids = request.form.getlist("selected_users[]")
    if not user_ids:
        flash("No users selected to delete.", "error")
        return redirect(url_for("manage_users"))

    # Convert strings to ints and filter out current user
    try:
        user_ids = [int(uid) for uid in user_ids if int(uid) != session.get('user_id')]
    except ValueError:
        flash("Invalid user selection.", "error")
        return redirect(url_for("manage_users"))

    if not user_ids:
        flash("Cannot delete your own account via bulk actions.", "error")
        return redirect(url_for("manage_users"))

    conn = get_db_connection()
    cur = conn.cursor()
    try:
        # Delete logs first to satisfy foreign key constraints if not cascading
        cur.execute("DELETE FROM time_logs WHERE user_id = ANY(%s::int[])", (user_ids,))
        cur.execute("DELETE FROM users WHERE id = ANY(%s::int[])", (user_ids,))
        conn.commit()
        flash(f"Successfully deleted {len(user_ids)} selected accounts.", "success")
    except Exception as e:
        conn.rollback()
        flash(f"Bulk deletion failed: {str(e)}", "error")
    finally:
        cur.close()
        conn.close()

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
