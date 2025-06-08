"""
Flask dashboard for monitoring collectors.
"""

import os
import secrets
from datetime import datetime, timedelta, timezone
from functools import wraps
from flask import Flask, render_template, jsonify, request, session, redirect, url_for, send_file
from flow_analysis.monitoring.collector_monitor import CollectorMonitor
from flow_analysis.config.env_config import DB_CONFIG
import psycopg2
import io
import csv

app = Flask(__name__)
# Generate a secure secret key if not set
app.secret_key = os.getenv('DASHBOARD_SECRET_KEY', secrets.token_hex(32))
# Set secure session settings
app.config.update(
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    PERMANENT_SESSION_LIFETIME=timedelta(hours=1)
)

monitor = CollectorMonitor(DB_CONFIG)

# Get dashboard password from environment variable
DASHBOARD_PASSWORD = os.getenv('DASHBOARD_PASSWORD')
if not DASHBOARD_PASSWORD:
    raise ValueError("DASHBOARD_PASSWORD environment variable must be set")

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('authenticated'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        if request.form.get('password') == DASHBOARD_PASSWORD:
            session['authenticated'] = True
            return redirect(url_for('index'))
        return render_template('login.html', error='Invalid password')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.pop('authenticated', None)
    return redirect(url_for('login'))

@app.route('/')
@login_required
def index():
    health = monitor.get_collector_health()
    return render_template('index.html', health=health)

@app.route('/api/status')
@login_required
def status():
    return jsonify(monitor.get_collector_health())

@app.route('/api/history')
@login_required
def history():
    """Get historical status information for all collectors."""
    hours = request.args.get('hours', default=24, type=int)
    history_data = {}
    
    for collector_type in monitor.collectors:
        history_data[collector_type] = monitor.get_collector_history(collector_type, hours)
    
    return jsonify(history_data)

@app.route('/api/logs')
@login_required
def logs():
    """Get recent logs for all collectors."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 
                        timestamp,
                        collector_name,
                        level,
                        message,
                        task_type,
                        details,
                        is_heartbeat,
                        status,
                        error_details
                    FROM trading.collector_logs
                    WHERE timestamp > NOW() - INTERVAL '1 hour'
                    ORDER BY timestamp DESC
                    LIMIT 100
                """)
                logs = cur.fetchall()
                
                # Convert to list of dicts
                return jsonify([{
                    'timestamp': log[0].isoformat(),
                    'collector': log[1],
                    'level': log[2],
                    'message': log[3],
                    'task_type': log[4],
                    'details': log[5],
                    'is_heartbeat': log[6],
                    'status': log[7],
                    'error_details': log[8]
                } for log in logs])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data_freshness')
@login_required
def data_freshness():
    """Return data freshness and completeness info for each collector."""
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                result = {}
                # News Collector
                cur.execute("SELECT MAX(created_at) FROM trading.news_headlines;")
                news_last = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM trading.news_headlines WHERE created_at > NOW() - INTERVAL '1 hour';")
                news_count = cur.fetchone()[0]
                # Placeholder for expected items (could be dynamic)
                news_expected = 100
                news_completeness = int((news_count / news_expected) * 100) if news_expected else 0
                news_status = 'up_to_date'
                if news_last:
                    delta = (datetime.now(timezone.utc) - news_last.astimezone(timezone.utc)).total_seconds()
                    if delta >= 3600:
                        news_status = 'stale'
                else:
                    news_status = 'stale'
                result['news'] = {
                    'last_data_timestamp': news_last.isoformat() if news_last else None,
                    'items_collected': news_count,
                    'expected_items': news_expected,
                    'completeness': news_completeness,
                    'status': news_status
                }
                # Darkpool Collector
                cur.execute("SELECT MAX(executed_at) FROM trading.darkpool_trades;")
                dp_last = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM trading.darkpool_trades WHERE executed_at > NOW() - INTERVAL '1 hour';")
                dp_count = cur.fetchone()[0]
                dp_expected = 50
                dp_completeness = int((dp_count / dp_expected) * 100) if dp_expected else 0
                dp_status = 'up_to_date'
                if dp_last:
                    delta = (datetime.now(timezone.utc) - dp_last.astimezone(timezone.utc)).total_seconds()
                    if delta >= 3600:
                        dp_status = 'stale'
                else:
                    dp_status = 'stale'
                result['darkpool'] = {
                    'last_data_timestamp': dp_last.isoformat() if dp_last else None,
                    'items_collected': dp_count,
                    'expected_items': dp_expected,
                    'completeness': dp_completeness,
                    'status': dp_status
                }
                return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export')
@login_required
def export_data():
    """Export data for selected collectors and time range as CSV."""
    collectors = request.args.get('collectors', 'news,darkpool').split(',')
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    output = io.StringIO()
    writer = csv.writer(output)
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            for collector in collectors:
                if collector == 'news':
                    query = "SELECT * FROM trading.news_headlines"
                    params = []
                    if start_time and end_time:
                        query += " WHERE created_at BETWEEN %s AND %s"
                        params = [start_time, end_time]
                    elif start_time:
                        query += " WHERE created_at >= %s"
                        params = [start_time]
                    elif end_time:
                        query += " WHERE created_at <= %s"
                        params = [end_time]
                    cur.execute(query, params)
                    rows = cur.fetchall()
                    colnames = [desc[0] for desc in cur.description]
                    writer.writerow([f'news_headlines: {len(rows)} rows'])
                    writer.writerow(colnames)
                    writer.writerows(rows)
                    writer.writerow([])
                elif collector == 'darkpool':
                    query = "SELECT * FROM trading.darkpool_trades"
                    params = []
                    if start_time and end_time:
                        query += " WHERE executed_at BETWEEN %s AND %s"
                        params = [start_time, end_time]
                    elif start_time:
                        query += " WHERE executed_at >= %s"
                        params = [start_time]
                    elif end_time:
                        query += " WHERE executed_at <= %s"
                        params = [end_time]
                    cur.execute(query, params)
                    rows = cur.fetchall()
                    colnames = [desc[0] for desc in cur.description]
                    writer.writerow([f'darkpool_trades: {len(rows)} rows'])
                    writer.writerow(colnames)
                    writer.writerows(rows)
                    writer.writerow([])
    output.seek(0)
    return send_file(
        io.BytesIO(output.getvalue().encode()),
        mimetype='text/csv',
        as_attachment=True,
        download_name='collector_export.csv'
    )

@app.route('/api/collection_counts')
@login_required
def collection_counts():
    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                # Query to get hourly collection counts for the last 24 hours
                query = """
                SELECT 
                    date_trunc('hour', created_at) as hour,
                    COUNT(*) as count
                FROM trading.news_headlines
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                GROUP BY hour
                ORDER BY hour DESC
                """
                cur.execute(query)
                results = cur.fetchall()
                return jsonify(results)
    except Exception as e:
        print(f"Error in collection_counts: {e}")
        return jsonify([])  # Return empty list on error

# Temporary route-printing snippet
print("Registered routes:", [rule.rule for rule in app.url_map.iter_rules()])

if __name__ == "__main__":
    app.run(debug=True) 