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
    # Get historical data for the last 24 hours
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)
    
    # This is a placeholder - implement actual historical data collection
    history_data = {
        'news': {
            'timestamps': [],
            'status': []
        },
        'darkpool': {
            'timestamps': [],
            'status': []
        }
    }
    
    return jsonify(history_data)

if __name__ == '__main__':
    # Only allow local connections in production
    app.run(host='127.0.0.1', port=5000, debug=False) 