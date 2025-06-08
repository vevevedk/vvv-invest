"""
Flask dashboard for monitoring collectors.
"""

import os
import secrets
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, render_template, jsonify, request, session, redirect, url_for
from flow_analysis.monitoring.collector_monitor import CollectorMonitor
from flow_analysis.config.env_config import DB_CONFIG
import psycopg2

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
                news_status = 'up_to_date' if news_last and (datetime.utcnow() - news_last).total_seconds() < 3600 else 'stale'
                result['news'] = {
                    'last_data_timestamp': news_last.isoformat() if news_last else None,
                    'items_collected': news_count,
                    'expected_items': news_expected,
                    'completeness': news_completeness,
                    'status': news_status
                }
                # Darkpool Collector
                cur.execute("SELECT MAX(timestamp) FROM trading.darkpool_trades;")
                dp_last = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM trading.darkpool_trades WHERE timestamp > NOW() - INTERVAL '1 hour';")
                dp_count = cur.fetchone()[0]
                dp_expected = 50
                dp_completeness = int((dp_count / dp_expected) * 100) if dp_expected else 0
                dp_status = 'up_to_date' if dp_last and (datetime.utcnow() - dp_last).total_seconds() < 3600 else 'stale'
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

if __name__ == '__main__':
    # Only allow local connections in production
    app.run(host='127.0.0.1', port=5000, debug=False) 