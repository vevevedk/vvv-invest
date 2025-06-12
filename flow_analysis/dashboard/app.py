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
import subprocess
import pytz
from collectors.utils.market_utils import get_market_status

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
    health = monitor.get_collector_health()
    cest = pytz.timezone('Europe/Copenhagen')
    for collector, status in health.get('collectors', {}).items():
        last_update = status.get('last_update')
        if last_update:
            if isinstance(last_update, str):
                try:
                    dt = datetime.fromisoformat(last_update)
                except Exception:
                    continue
            else:
                dt = last_update
            if dt.tzinfo is None:
                dt = pytz.UTC.localize(dt)
            dt_cest = dt.astimezone(cest)
            health['collectors'][collector]['last_update'] = dt_cest.isoformat()
            health['collectors'][collector]['timezone'] = 'Europe/Copenhagen (CEST)'
    return jsonify(health)

@app.route('/api/history')
@login_required
def history():
    """Get historical status information for all collectors."""
    hours = request.args.get('hours', default=24, type=int)
    history_data = {}
    cest = pytz.timezone('Europe/Copenhagen')
    for collector_type in monitor.collectors:
        raw_history = monitor.get_collector_history(collector_type, hours)
        history_data[collector_type] = [
            {**h, 'timestamp': (h['timestamp'].astimezone(cest).isoformat() if h.get('timestamp') else None), 'timezone': 'Europe/Copenhagen (CEST)'}
            for h in raw_history
        ]
    return jsonify(history_data)

@app.route('/api/logs')
@login_required
def logs():
    """Get recent logs for all collectors with filtering options."""
    try:
        collector = request.args.get('collector')
        level = request.args.get('level')
        hours = request.args.get('hours', default=1, type=int)
        cest = pytz.timezone('Europe/Copenhagen')
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                query = """
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
                    WHERE timestamp > NOW() - INTERVAL '%s hours'
                """
                params = [hours]
                if collector:
                    query += " AND collector_name = %s"
                    params.append(collector)
                if level:
                    query += " AND level = %s"
                    params.append(level)
                query += " ORDER BY timestamp DESC LIMIT 100"
                cur.execute(query, params)
                logs = cur.fetchall()
                return jsonify([{
                    'timestamp': (log[0].astimezone(cest).isoformat() if log[0] else None),
                    'collector': log[1],
                    'level': log[2],
                    'message': log[3],
                    'task_type': log[4],
                    'details': log[5],
                    'is_heartbeat': log[6],
                    'status': log[7],
                    'error_details': log[8],
                    'timezone': 'Europe/Copenhagen (CEST)'
                } for log in logs])
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data_freshness')
@login_required
def data_freshness():
    """Return data freshness and completeness info for each collector."""
    try:
        cest = pytz.timezone('Europe/Copenhagen')
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                result = {}
                # News Collector
                cur.execute("SELECT MAX(created_at) FROM trading.news_headlines;")
                news_last = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM trading.news_headlines WHERE created_at > NOW() - INTERVAL '1 hour';")
                news_count = cur.fetchone()[0]
                news_expected = 100
                news_completeness = int((news_count / news_expected) * 100) if news_expected else 0
                news_status = 'up_to_date'
                if news_last:
                    delta = (datetime.now(timezone.utc) - news_last.astimezone(timezone.utc)).total_seconds()
                    if delta >= 3600:
                        news_status = 'stale'
                    # Convert to CEST and use isoformat
                    if news_last.tzinfo is None:
                        news_last = pytz.UTC.localize(news_last)
                    news_last_cest = news_last.astimezone(cest)
                    news_last_iso = news_last_cest.isoformat()
                else:
                    news_status = 'stale'
                    news_last_iso = None
                result['news'] = {
                    'last_data_timestamp': news_last_iso,
                    'items_collected': news_count,
                    'expected_items': news_expected,
                    'completeness': news_completeness,
                    'status': news_status,
                    'timezone': 'Europe/Copenhagen (CEST)'
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
                    if dp_last.tzinfo is None:
                        dp_last = pytz.UTC.localize(dp_last)
                    dp_last_cest = dp_last.astimezone(cest)
                    dp_last_iso = dp_last_cest.isoformat()
                else:
                    dp_status = 'stale'
                    dp_last_iso = None
                result['darkpool'] = {
                    'last_data_timestamp': dp_last_iso,
                    'items_collected': dp_count,
                    'expected_items': dp_expected,
                    'completeness': dp_completeness,
                    'status': dp_status,
                    'timezone': 'Europe/Copenhagen (CEST)'
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
    cest = pytz.timezone('Europe/Copenhagen')
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
                    writer.writerow([f'Timezone: Europe/Copenhagen (CEST)'])
                    writer.writerow(colnames)
                    dt_indexes = [i for i, desc in enumerate(cur.description) if desc.type_code in (1114, 1184)]
                    for row in rows:
                        row = list(row)
                        for i in dt_indexes:
                            if row[i]:
                                if row[i].tzinfo is None:
                                    row[i] = pytz.UTC.localize(row[i])
                                row[i] = row[i].astimezone(cest).isoformat()
                        writer.writerow(row)
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
                    writer.writerow([f'Timezone: Europe/Copenhagen (CEST)'])
                    writer.writerow(colnames)
                    dt_indexes = [i for i, desc in enumerate(cur.description) if desc.type_code in (1114, 1184)]
                    for row in rows:
                        row = list(row)
                        for i in dt_indexes:
                            if row[i]:
                                if row[i].tzinfo is None:
                                    row[i] = pytz.UTC.localize(row[i])
                                row[i] = row[i].astimezone(cest).isoformat()
                        writer.writerow(row)
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
        cest = pytz.timezone('Europe/Copenhagen')
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                query = """
                SELECT
                    collector_name,
                    date_trunc('hour', timestamp) as hour,
                    COUNT(*) as count
                FROM trading.collector_logs
                WHERE status = 'collected'
                  AND timestamp >= NOW() - INTERVAL '24 hours'
                GROUP BY collector_name, hour
                ORDER BY hour DESC
                """
                cur.execute(query)
                results = cur.fetchall()
                return jsonify([
                    {
                        'collector': row[0],
                        'hour': (row[1].astimezone(cest).isoformat() if row[1] else None),
                        'count': row[2],
                        'timezone': 'Europe/Copenhagen (CEST)'
                    }
                    for row in results
                ])
    except Exception as e:
        print(f"Error in collection_counts: {e}")
        return jsonify([])

@app.route('/api/backfill', methods=['POST'])
@login_required
def trigger_backfill():
    """Trigger a backfill for a specific collector."""
    try:
        collector = request.json.get('collector')
        hours = request.json.get('hours', 24)
        
        if not collector:
            return jsonify({'error': 'Collector name is required'}), 400
            
        if collector not in ['news', 'darkpool']:
            return jsonify({'error': 'Invalid collector name'}), 400
            
        # Log the backfill request
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trading.collector_logs 
                    (collector_name, level, message, task_type, status)
                    VALUES (%s, 'INFO', %s, 'manual_backfill', 'started')
                """, (collector, f'Manual backfill triggered for last {hours} hours'))
        
        # Trigger the backfill process
        if collector == 'news':
            subprocess.Popen([
                '/opt/darkpool_collector/venv/bin/python',
                '-m', 'collectors.news.newscollector',
                '--backfill',
                '--hours', str(hours)
            ], env=dict(os.environ, PYTHONPATH='/opt/darkpool_collector'))
        else:  # darkpool
            subprocess.Popen([
                '/opt/darkpool_collector/venv/bin/python',
                '-m', 'collectors.darkpool.darkpool_collector_backfill',
                '--hours', str(hours)
            ], env=dict(os.environ, PYTHONPATH='/opt/darkpool_collector'))
            
        return jsonify({'message': f'Backfill triggered for {collector}'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/restart', methods=['POST'])
@login_required
def restart_collector():
    """Restart a specific collector service."""
    try:
        collector = request.json.get('collector')
        
        if not collector:
            return jsonify({'error': 'Collector name is required'}), 400
            
        if collector not in ['news', 'darkpool']:
            return jsonify({'error': 'Invalid collector name'}), 400
            
        # Log the restart request
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO trading.collector_logs 
                    (collector_name, level, message, task_type, status)
                    VALUES (%s, 'INFO', 'Manual restart triggered', 'restart', 'started')
                """, (collector,))
        
        # Restart the appropriate service
        if collector == 'news':
            subprocess.run(['systemctl', 'restart', 'news-collector-worker.service'])
            subprocess.run(['systemctl', 'restart', 'news-collector-beat.service'])
        else:  # darkpool
            subprocess.run(['systemctl', 'restart', 'darkpool-collector-worker.service'])
            subprocess.run(['systemctl', 'restart', 'darkpool-collector-beat.service'])
            
        return jsonify({'message': f'Restart triggered for {collector}'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/market_status')
@login_required
def market_status():
    return jsonify(get_market_status())

# Temporary route-printing snippet
print("Registered routes:", [rule.rule for rule in app.url_map.iter_rules()])

if __name__ == "__main__":
    app.run(debug=True) 