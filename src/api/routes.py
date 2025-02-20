from flask import Flask, request, jsonify, render_template_string, send_from_directory, make_response, render_template, session, redirect, url_for
from flask_cors import CORS
from datetime import datetime, timedelta
import sys
import os
import pandas as pd
import random
from sqlalchemy.sql import func
import json
import pytz  # Add this import at the top
from functools import wraps

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.data.db_manager import DatabaseManager, ReferralData, AllowedAccount
from src.scraper.scheduler import ScraperScheduler

app = Flask(__name__, static_folder='../../frontend/build', static_url_path='/')
app.secret_key = os.environ.get('SECRET_KEY', 'your-secret-key-here')
app.permanent_session_lifetime = timedelta(days=30)

# Define allowed origins for CORS
ALLOWED_ORIGINS = [
    'http://localhost:8000',
    'http://localhost:5001',
    'https://referral-tracker-8dea3f9d92b7.herokuapp.com',
    'https://referral-tracker-production.up.railway.app'
]

# Configure CORS with multiple origins
CORS(app, resources={
    r"/api/*": {  # Only allows routes starting with /api/
        "origins": ALLOWED_ORIGINS,
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type"],
        "supports_credentials": True
    },
    r"/admin/*": {  # Add this block
        "origins": ALLOWED_ORIGINS,
        "methods": ["GET", "POST", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type"],
        "supports_credentials": True
    },
    r"/login": {"origins": ALLOWED_ORIGINS},
    r"/logout": {"origins": ALLOWED_ORIGINS}
})
db = DatabaseManager()

# Login required decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# Login route
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        password = request.form.get('password')
        if password == os.environ.get('APP_PASSWORD', 'default-password'):
            session['logged_in'] = True
            session.permanent = True
            return redirect('/data/referral_viewer.html')
        return redirect(url_for('login', error=True))
    error = request.args.get('error', False)
    return '''
        <!DOCTYPE html>
        <html>
        <head>
            <title>Referral Tracker - Login</title>
            <style>
                body {
                    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
                    background-color: #f5f5f5;
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    margin: 0;
                }
                .login-container {
                    background: white;
                    padding: 2rem;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
                    width: 100%;
                    max-width: 320px;
                }
                h1 {
                    color: #333;
                    text-align: center;
                    margin-bottom: 1.5rem;
                    font-size: 1.5rem;
                }
                .form-group {
                    margin-bottom: 1rem;
                }
                input[type="password"] {
                    width: 100%;
                    padding: 0.75rem;
                    border: 1px solid #ddd;
                    border-radius: 4px;
                    font-size: 1rem;
                    box-sizing: border-box;
                }
                input[type="submit"] {
                    width: 100%;
                    padding: 0.75rem;
                    background-color: #007bff;
                    color: white;
                    border: none;
                    border-radius: 4px;
                    font-size: 1rem;
                    cursor: pointer;
                    transition: background-color 0.2s;
                }
                input[type="submit"]:hover {
                    background-color: #0056b3;
                }
                .error {
                    color: #dc3545;
                    text-align: center;
                    margin-bottom: 1rem;
                    display: ''' + ('block' if error else 'none') + ''';
                }
            </style>
        </head>
        <body>
            <div class="login-container">
                <h1>Referral Tracker</h1>
                <div class="error">Invalid password. Please try again.</div>
                <form method="post">
                    <div class="form-group">
                        <input type="password" name="password" placeholder="Enter password" required>
                    </div>
                    <input type="submit" value="Login">
                </form>
            </div>
        </body>
        </html>
    '''

# Root route
@app.route('/')
def index():
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    return redirect('/data/referral_viewer.html')

def safe_int_convert(value):
    """Safely convert a value to integer, handling empty strings, commas and percentage signs"""
    if not value:  # Handle empty strings
        return 0
    try:
        # Remove commas and % signs, then convert to int
        cleaned = str(value).replace(',', '').replace('%', '')
        return int(float(cleaned))
    except (ValueError, TypeError):
        return 0

def interpolate_missing_days(data, start_date, end_date):
    """
    !!! OPTIMIZED - DO NOT MODIFY !!!
    This function has been tested and optimized for proper date handling and interpolation.
    Last verified: December 2024
    
    Fills in missing days with linear interpolation between known points.
    Uses a date_map for efficient lookup of existing records to prevent duplicates.
    """
    if not data:
        return []
        
    filled_data = []
    current_date = start_date
    
    # Create a map of existing dates for faster lookup
    date_map = {r.date.strftime('%Y-%m-%d'): r for r in data}
    
    while current_date <= end_date:
        date_key = current_date.strftime('%Y-%m-%d')
        
        if date_key in date_map:
            # Use actual data if we have it
            filled_data.append(date_map[date_key])
        else:
            # Find surrounding known data points
            prev_record = next((r for r in reversed(data) if r.date <= current_date), None)
            next_record = next((r for r in data if r.date > current_date), None)
            
            if prev_record and next_record:
                # Interpolate between known points
                total_days = (next_record.date - prev_record.date).days
                days_from_prev = (current_date - prev_record.date).days
                
                # Add protection against zero division
                if total_days == 0:
                    # If dates are the same, just use the previous record's values
                    filled_data.append(prev_record)
                else:
                    progress = days_from_prev / total_days
                    
                    interpolated = ReferralData(
                        date=current_date,
                        account_name=prev_record.account_name,
                        recommending_me=[],
                        my_recommendations=[]
                    )
                    
                    # Interpolate recommending_me
                    for rec in prev_record.recommending_me:
                        prev_val = safe_int_convert(rec['subscribers'])
                        next_val = next((safe_int_convert(r['subscribers']) 
                            for r in next_record.recommending_me if r['creator'] == rec['creator']), prev_val)
                        interpolated_val = int(prev_val + (next_val - prev_val) * progress)
                        
                        interpolated.recommending_me.append({
                            'creator': rec['creator'],
                            'subscribers': str(interpolated_val),
                            'conversion_rate': rec.get('conversion_rate', 0)
                        })
                    
                    # Interpolate my_recommendations
                    for rec in prev_record.my_recommendations:
                        prev_val = safe_int_convert(rec['subscribers'])
                        next_val = next((safe_int_convert(r['subscribers']) 
                            for r in next_record.my_recommendations if r['creator'] == rec['creator']), prev_val)
                        interpolated_val = int(prev_val + (next_val - prev_val) * progress)
                        
                        interpolated.my_recommendations.append({
                            'creator': rec['creator'],
                            'subscribers': str(interpolated_val),
                            'conversion_rate': rec.get('conversion_rate', 0)
                        })
                    
                    filled_data.append(interpolated)
            elif prev_record:
                filled_data.append(prev_record)
                
        current_date += timedelta(days=1)
    
    return filled_data

@app.route('/api/partnership-metrics')
@login_required
def get_partnership_metrics():
    try:
        print("\n\n=== PARTNERSHIP METRICS DEBUG ===")
        
        start_date_str = request.args.get('start')
        end_date_str = request.args.get('end')
        account = request.args.get('account')

        print(f"Request for account: {account}")

        # Always return demo data for Demo Client
        if account == "Demo Client":
            print("Generating fresh demo data...")
            demo_data = db.generate_demo_data()
            
            # Get the most recent demo record
            latest_record = demo_data[0]  # Most recent due to date ordering
            earliest_record = demo_data[-1]  # Earliest record
            
            print(f"Processing demo data with {len(demo_data)} records")
            
            # Process metrics similar to real data
            metrics = []
            all_partners = set(
                [rec['creator'] for rec in latest_record.recommending_me] + 
                [rec['creator'] for rec in latest_record.my_recommendations]
            )
            
            for partner in all_partners:
                # Get latest values
                latest_received = next((int(rec['subscribers']) 
                    for rec in latest_record.recommending_me if rec['creator'] == partner), 0)
                latest_sent = next((int(rec['subscribers']) 
                    for rec in latest_record.my_recommendations if rec['creator'] == partner), 0)
                
                # Get earliest values
                earliest_received = next((int(rec['subscribers']) 
                    for rec in earliest_record.recommending_me if rec['creator'] == partner), 0)
                earliest_sent = next((int(rec['subscribers']) 
                    for rec in earliest_record.my_recommendations if rec['creator'] == partner), 0)
                
                # Calculate changes
                period_received = latest_received - earliest_received
                period_sent = latest_sent - earliest_sent
                
                metrics.append({
                    'partner': partner,
                    'period_received': period_received,
                    'period_sent': period_sent,
                    'period_balance': period_received - period_sent,
                    'all_time_balance': latest_received - latest_sent,
                    'account': 'Demo Client'
                })
            
            # Sort by period balance (most negative first)
            metrics.sort(key=lambda x: x['period_balance'])
            print(f"Returning {len(metrics)} demo metrics")
            return jsonify(metrics)

        # Continue with existing code for real clients...
        if not start_date_str:
            start_date_str = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not end_date_str:
            end_date_str = datetime.now().strftime('%Y-%m-%d')

        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        
        session = db.Session()
        try:
            # Base query
            query = session.query(ReferralData)\
                .filter(ReferralData.date <= end_date)\
                .filter(ReferralData.date >= start_date)\
                .order_by(ReferralData.date)

            # Add account filter only if specific account requested
            if account and account != 'all':
                query = query.filter(ReferralData.account_name == account)
                
            records = query.all()
                
            if not records:
                return jsonify([])
            
            # Group records by account
            account_records = {}
            for record in records:
                if record.account_name not in account_records:
                    account_records[record.account_name] = []
                account_records[record.account_name].append(record)
            
            results = []
            all_partners = set()
            partner_metrics = {}
            
            # Process each account's records
            for acc_records in account_records.values():
                if len(acc_records) <= 1:
                    continue
                    
                # Find first non-zero record for each partner's sent and received
                partner_baselines = {}
                
                for record in acc_records:
                    for rec in record.recommending_me:
                        partner = rec['creator']
                        if partner not in partner_baselines:
                            partner_baselines[partner] = {'received': None, 'sent': None}
                        if partner_baselines[partner]['received'] is None and safe_int_convert(rec['subscribers']) > 0:
                            partner_baselines[partner]['received'] = {
                                'value': safe_int_convert(rec['subscribers']),
                                'date': record.date
                            }
                            
                    for rec in record.my_recommendations:
                        partner = rec['creator']
                        if partner not in partner_baselines:
                            partner_baselines[partner] = {'received': None, 'sent': None}
                        if partner_baselines[partner]['sent'] is None and safe_int_convert(rec['subscribers']) > 0:
                            partner_baselines[partner]['sent'] = {
                                'value': safe_int_convert(rec['subscribers']),
                                'date': record.date
                            }

                # Now process the changes using separate baselines
                period_start = acc_records[0]
                period_end = acc_records[-1]
                
                for partner in partner_baselines:
                    key = partner if account == 'all' else f"{partner}_{period_start.account_name}"
                    
                    if key not in partner_metrics:
                        partner_metrics[key] = {
                            'partner': partner,
                            'account': period_start.account_name if account != 'all' else 'All Clients',
                            'period_received': 0,
                            'period_sent': 0,
                            'latest_received': 0,
                            'latest_sent': 0,
                            'baseline_received': partner_baselines[partner]['received'],
                            'baseline_sent': partner_baselines[partner]['sent']
                        }
                    
                    # Calculate period end values
                    period_end_received = next((safe_int_convert(rec['subscribers']) 
                        for rec in period_end.recommending_me if rec['creator'] == partner), 0)
                    period_end_sent = next((safe_int_convert(rec['subscribers']) 
                        for rec in period_end.my_recommendations if rec['creator'] == partner), 0)
                    
                    # Calculate changes using appropriate baselines
                    baseline = partner_baselines[partner]
                    if baseline['received'] and baseline['received']['date'] <= period_end.date:
                        partner_metrics[key]['period_received'] = period_end_received - baseline['received']['value']
                    if baseline['sent'] and baseline['sent']['date'] <= period_end.date:
                        partner_metrics[key]['period_sent'] = period_end_sent - baseline['sent']['value']
                    
                    # Store latest values
                    partner_metrics[key]['latest_received'] = period_end_received
                    partner_metrics[key]['latest_sent'] = period_end_sent

            # Convert metrics to results
            for metrics in partner_metrics.values():
                metrics['period_balance'] = metrics['period_received'] - metrics['period_sent']
                metrics['all_time_balance'] = metrics['latest_received'] - metrics['latest_sent']
                results.append(metrics)
            
            # Sort by period balance, most negative first
            results.sort(key=lambda x: x['period_balance'])
            
            return jsonify(results)
            
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error in partnership metrics: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/earliest-date')
@login_required
def get_earliest_date():
    earliest_date = db.get_earliest_data_date()
    return jsonify({'earliest_date': earliest_date.isoformat() if earliest_date else None})

@app.route('/api/largest-imbalances')
@login_required
def get_largest_imbalances():
    try:
        # Get date parameters
        start_date = request.args.get('start')
        end_date = request.args.get('end')
        account = request.args.get('account')  # Add this line to get the account parameter
        
        session = db.Session()
        try:
            # Initialize stats dictionaries
            period_stats = {}
            latest_stats = {}
            
            # Get latest record for all-time stats
            latest_record = session.query(ReferralData)\
                .order_by(ReferralData.date.desc())\
                .first()
            
            if latest_record:
                for rec in latest_record.recommending_me:
                    creator = rec['creator']
                    if creator not in latest_stats:
                        latest_stats[creator] = {'received': 0, 'sent': 0}
                    latest_stats[creator]['received'] += safe_int_convert(rec['subscribers'])
                
                for rec in latest_record.my_recommendations:
                    creator = rec['creator']
                    if creator not in latest_stats:
                        latest_stats[creator] = {'received': 0, 'sent': 0}
                    latest_stats[creator]['sent'] += safe_int_convert(rec['subscribers'])
            
            # Get period data
            if start_date and end_date:
                start = datetime.strptime(start_date, '%Y-%m-%d')
                end = datetime.strptime(end_date, '%Y-%m-%d')
                
                # Add account filter if specified
                query = session.query(ReferralData)\
                    .filter(ReferralData.date.between(start, end))
                
                if account and account != 'all':
                    query = query.filter(ReferralData.account_name == account)
                
                period_records = query.all()
                
                # Process all records in period
                for record in period_records:
                    for rec in record.recommending_me:
                        creator = rec['creator']
                        if creator not in period_stats:
                            period_stats[creator] = {'received': 0, 'sent': 0}
                        period_stats[creator]['received'] += safe_int_convert(rec['subscribers'])
                    
                    for rec in record.my_recommendations:
                        creator = rec['creator']
                        if creator not in period_stats:
                            period_stats[creator] = {'received': 0, 'sent': 0}
                        period_stats[creator]['sent'] += safe_int_convert(rec['subscribers'])

            # Combine results
            results = []
            all_partners = set(list(period_stats.keys()) + list(latest_stats.keys()))
            
            for partner in all_partners:
                period_received = period_stats.get(partner, {'received': 0})['received']
                period_sent = period_stats.get(partner, {'sent': 0})['sent']
                period_balance = period_received - period_sent
                
                latest_received = latest_stats.get(partner, {'received': 0})['received']
                latest_sent = latest_stats.get(partner, {'sent': 0})['sent']
                all_time_balance = latest_received - latest_sent
                
                results.append({
                    'partner': partner,
                    'account': account if account and account != 'all' else 'All Clients',
                    'period_received': period_received,
                    'period_sent': period_sent,
                    'period_balance': period_balance,
                    'all_time_balance': all_time_balance
                })
            
            # Sort by period_balance ascending (worst performing first)
            results.sort(key=lambda x: x['period_balance'])
            
            return jsonify(results)
            
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error processing request: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/debug/db')
@login_required
def debug_db():
    session = db.Session()
    try:
        records = session.query(ReferralData).all()
        print("\n=== Database Contents ===")
        print(f"Total records: {len(records)}")
        for record in records[:5]:  # Show first 5 records
            print(f"\nAccount: {record.account_name}")
            print(f"Date: {record.date}")
            print(f"Num recommending me: {len(record.recommending_me)}")
            print(f"Num recommendations: {len(record.my_recommendations)}")
        return jsonify({
            'total_records': len(records),
            'accounts': list(set(r.account_name for r in records)),
            'dates': [r.date.isoformat() for r in records[:5]]
        })
    finally:
        session.close()

@app.route('/api/trends/<account_name>')
@login_required
def get_trends(account_name):
    try:
        partner = request.args.get('partner')
        days = request.args.get('days', default=30, type=int)
        end_date = request.args.get('end_date')
        
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            end_date = datetime.now()
            
        start_date = end_date - timedelta(days=days)
        
        # Handle Demo Client request
        if account_name == "Demo Client":
            demo_data = db.generate_demo_data()
            
            trend_data = {
                'trends': {
                    'dates': [],
                    'received': [],
                    'sent': [],
                    'balance': []
                },
                'growth': {
                    'subscriber_growth': 25.5,  # Demo growth metrics
                    'conversion_growth': 15.2,
                    'earnings_growth': 30.1
                },
                'period': {
                    'start': start_date.strftime('%Y-%m-%d'),
                    'end': end_date.strftime('%Y-%m-%d')
                }
            }
            
            # Process each day's data
            for record in demo_data:
                if partner:
                    # If partner specified, get specific partner trends
                    received = next(
                        (int(rec['subscribers']) for rec in record.recommending_me 
                         if rec['creator'] == partner), 
                        0
                    )
                    sent = next(
                        (int(rec['subscribers']) for rec in record.my_recommendations 
                         if rec['creator'] == partner), 
                        0
                    )
                else:
                    # Otherwise get total trends
                    received = sum(int(rec['subscribers']) for rec in record.recommending_me)
                    sent = sum(int(rec['subscribers']) for rec in record.my_recommendations)
                
                trend_data['trends']['dates'].append(record.date.strftime('%Y-%m-%d'))
                trend_data['trends']['received'].append(received)
                trend_data['trends']['sent'].append(sent)
                trend_data['trends']['balance'].append(received - sent)
                
            return jsonify(trend_data)
            
        # Original code for real clients
        trends = db.get_account_trends(account_name, start_date, end_date)
        growth = db.calculate_growth_metrics(account_name, start_date, end_date)
        
        return jsonify({
            'trends': trends,
            'growth': {
                'subscriber_growth': growth[0] if growth else 0,
                'conversion_growth': growth[1] if growth else 0,
                'earnings_growth': growth[2] if growth else 0
            },
            'period': {
                'start': start_date.strftime('%Y-%m-%d'),
                'end': end_date.strftime('%Y-%m-%d')
            }
        })
            
    except Exception as e:
        print(f"Error getting trends: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/trends/summary')
@login_required
def get_trends_summary():
    try:
        days = request.args.get('days', default=30, type=int)
        min_earnings = request.args.get('min_earnings', default=0, type=float)
        
        summary = db.get_trends_summary(days, min_earnings)
        return jsonify({
            'top_growing': summary['top_growing'],
            'top_declining': summary['top_declining'],
            'period': {
                'days': days,
                'start': (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'),
                'end': datetime.now().strftime('%Y-%m-%d')
            }
        })
    except Exception as e:
        print(f"Error generating trends summary: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/test/import-csv')
@login_required
def import_csv_data():
    """Import existing CSV data into the database"""
    session = db.Session()
    try:
        import pandas as pd
        
        # Read the CSV file
        df = pd.read_csv('src/data/referral_data.csv')
        
        # Convert date strings to datetime objects
        df['date'] = pd.to_datetime(df['date'])
        
        # Group by account_name and date
        records_added = 0
        for account_name in df['account_name'].unique():
            account_df = df[df['account_name'] == account_name]
            
            for date in account_df['date'].unique():
                date_df = account_df[account_df['date'] == date]
                
                # Get recommending_me data
                recommending = date_df[date_df['tab'] == 'recommending_me']
                recommending_list = [
                    {
                        'creator': row['creator'],
                        'subscribers': row['subscribers'],
                        'conversion_rate': row['conversion_rate']
                    }
                    for _, row in recommending.iterrows()
                ]
                
                # Get my_recommendations data
                recommendations = date_df[date_df['tab'] == 'my_recommendations']
                recommendations_list = [
                    {
                        'creator': row['creator'],
                        'subscribers': row['subscribers'],
                        'conversion_rate': row['conversion_rate']
                    }
                    for _, row in recommendations.iterrows()
                ]
                
                # Create record
                record = ReferralData(
                    date=date,
                    account_name=account_name,
                    recommending_me=recommending_list,
                    my_recommendations=recommendations_list
                )
                session.add(record)
                records_added += 1
        
        session.commit()
        return jsonify({
            "message": f"Successfully imported {records_added} records from CSV",
            "records_added": records_added
        })
    
    except Exception as e:
        session.rollback()
        print(f"Error importing data: {str(e)}")
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', 'http://localhost:8000')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization,Cache-Control')
    response.headers.add('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
    response.headers.add('Access-Control-Allow-Credentials', 'true')
    response.headers.add('Access-Control-Max-Age', '86400')
    return response

@app.route('/api/partnership-trends')
@login_required
def get_partnership_trends():
    try:
        account = request.args.get('account')
        partner = request.args.get('partner')
        start = request.args.get('start')
        end = request.args.get('end')
        
        print(f"Processing partnership trends for {partner} with {account}")
        print(f"Date range: {start} to {end}")
        
        if not all([account, partner, start, end]):
            return jsonify({'error': 'Missing required parameters'}), 400
            
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
        
        session = db.Session()
        try:
            records = session.query(ReferralData)\
                .filter(ReferralData.account_name == account)\
                .filter(ReferralData.date.between(start_date, end_date))\
                .order_by(ReferralData.date)\
                .all()

            # Group records by date
            daily_records = {}
            for record in records:
                date_key = record.date.strftime('%Y-%m-%d')
                daily_records[date_key] = record
            
            sorted_records = [daily_records[date] for date in sorted(daily_records.keys())]
            
            dates = []
            received_values = []
            sent_values = []
            conversion_rates = []
            
            # Find baselines
            baseline_received = None
            baseline_sent = None
            baseline_received_date = None
            baseline_sent_date = None
            
            for record in sorted_records:
                # Find first non-zero values for baselines
                for rec in record.recommending_me:
                    if rec['creator'] == partner:
                        received = safe_int_convert(rec.get('subscribers', 0))
                        if received > 0 and baseline_received is None:
                            baseline_received = received
                            baseline_received_date = record.date
                        break
                
                for rec in record.my_recommendations:
                    if rec['creator'] == partner:
                        sent = safe_int_convert(rec.get('subscribers', 0))
                        if sent > 0 and baseline_sent is None:
                            baseline_sent = sent
                            baseline_sent_date = record.date
                        break

            # Process records with both raw and adjusted values
            for record in sorted_records:
                date_str = record.date.strftime('%-m/%-d')
                
                received = next((safe_int_convert(rec['subscribers']) 
                    for rec in record.recommending_me if rec['creator'] == partner), 0)
                sent = next((safe_int_convert(rec['subscribers']) 
                    for rec in record.my_recommendations if rec['creator'] == partner), 0)
                conversion_rate = float(next((rec.get('conversion_rate', 0) 
                    for rec in record.recommending_me if rec['creator'] == partner), 0))
                
                dates.append(date_str)
                received_values.append(received)
                sent_values.append(sent)
                conversion_rates.append(conversion_rate)

            # Calculate current period metrics (using baselines)
            current_received = received_values[-1] - (baseline_received or 0)
            current_sent = sent_values[-1] - (baseline_sent or 0)
            
            return jsonify({
                'historical_data': {
                    'dates': dates,
                    'received': received_values,
                    'sent': sent_values,
                    'conversion_rates': conversion_rates
                },
                'current_period': {
                    'received': current_received,
                    'sent': current_sent,
                    'balance': current_received - current_sent,
                    'conversion_rate': conversion_rates[-1] if conversion_rates else 0
                },
                'baselines': {
                    'received': {
                        'value': baseline_received,
                        'date': baseline_received_date.strftime('%Y-%m-%d') if baseline_received_date else None
                    },
                    'sent': {
                        'value': baseline_sent,
                        'date': baseline_sent_date.strftime('%Y-%m-%d') if baseline_sent_date else None
                    }
                }
            })
            
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error processing partnership trends: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/partnership-metrics', methods=['OPTIONS'])
@login_required
def handle_options():
    response = jsonify({'status': 'ok'})
    return response

@app.route('/admin/database')
@login_required
def database_admin():
    session = db.Session()
    try:
        # Get all records grouped by date and account
        records = session.query(ReferralData)\
            .order_by(ReferralData.date.desc(), ReferralData.account_name)\
            .all()
            
        # Convert timestamps to PT
        timezone = pytz.timezone('America/Los_Angeles')
        for record in records:
            if record.date.tzinfo is None:  # If timestamp is naive
                utc_date = pytz.utc.localize(record.date)
                record.date = utc_date.astimezone(timezone)

        # Create a simple HTML interface
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Database Admin</title>
            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
            <style>
                .action-history {
                    margin-top: 20px;
                    padding: 15px;
                    background: #f8f9fa;
                    border-radius: 8px;
                }
                .btn-primary {
                    background: #0d6efd;
                    border: none;
                }
                .btn-danger {
                    background: #dc3545;
                    border: none;
                }
                .btn-success {
                    background: #198754;
                    border: none;
                }
                .btn-warning {
                    background: #ffc107;
                    color: #000;
                    border: none;
                }
                .btn-secondary {
                    background: #6c757d;
                    border: none;
                }
                .btn-outline {
                    background: transparent;
                    border: 1px solid #dee2e6;
                    color: #6c757d;
                }
                .action-buttons {
                    display: flex;
                    gap: 10px;
                    margin-bottom: 20px;
                }
                .primary-actions {
                    display: flex;
                    gap: 10px;
                }
                .secondary-actions {
                    display: flex;
                    gap: 10px;
                    margin-left: auto;
                }
                .checkbox-column {
                    width: 40px;
                    text-align: center;
                }
                #bulkDeleteBtn {
                    display: none;
                    margin-left: 10px;
                }
                .record-row {
                    cursor: pointer;
                }
                .record-row:hover {
                    background-color: #f8f9fa;
                }
                .checkbox-column {
                    cursor: default;
                }
                .checkbox-column:hover {
                    background-color: inherit !important;
                }
            </style>
        </head>
        <body>
            <div class="container mt-4">
                <h1>Database Management</h1>
                
                <div class="action-buttons">
                    <div class="primary-actions">
                        <button class="btn btn-success" onclick="runScraper()">Run Scraper Now</button>
                        <button id="bulkDeleteBtn" class="btn btn-danger" onclick="bulkDelete()" style="display: none">
                            Delete Selected (<span id="selectedCount">0</span>)
                        </button>
                        <a href="/admin/import-data" class="btn btn-primary">Import Data</a>
                        <!-- Add this new button -->
                        <button class="btn btn-primary" onclick="showAccountManager()">Manage Accounts</button>
                    </div>
                    
                    <div class="secondary-actions">
                        <!-- Maintenance actions -->
                        <button class="btn btn-warning" onclick="cleanupDuplicates()">Clean Duplicates</button>
                        <button class="btn btn-warning" onclick="cleanupInitialData()">Clean Initial Data</button>
                        <button class="btn btn-outline" onclick="reimportCSV()">Reimport from CSV</button>
                        <button class="btn btn-danger" onclick="clearDatabase()">Clear Database</button>
                    </div>
                </div>

                <div class="alert alert-info">
                    <strong>Last Successful Scrape:</strong> <span id="lastScrapeTime">Loading...</span>
                </div>

                <!-- Add this new section -->
                <div id="accountManagerSection" style="display: none;" class="mb-4">
                    <div class="card">
                        <div class="card-body">
                            <h5 class="card-title">Account Management</h5>
                            <p class="text-muted">Select which accounts to include in data collection</p>
                            
                            <div class="mb-3">
                                <div class="form-check">
                                    <input class="form-check-input" type="checkbox" id="selectAllAccounts" onchange="toggleAllAccounts(this)">
                                    <label class="form-check-label" for="selectAllAccounts">
                                        Select All
                                    </label>
                                </div>
                            </div>

                            <div id="accountsList" style="max-height: 400px; overflow-y: auto;">
                                <!-- Accounts will be populated here -->
                            </div>
                            
                            <div class="mt-3">
                                <button class="btn btn-primary" onclick="saveAccountSettings()">Save Changes</button>
                                <button class="btn btn-outline-secondary" onclick="hideAccountManager()">Close</button>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Add action history section -->
                <div class="action-history">
                    <h5>Recent Actions</h5>
                    <div id="actionHistory"></div>
                    <button class="btn btn-outline mt-2" onclick="undoLastAction()" id="undoButton" disabled>
                        Undo Last Action
                    </button>
                </div>

                <table class="table">
                    <thead>
                        <tr>
                            <th class="checkbox-column">
                                <input type="checkbox" id="selectAll" onclick="toggleAllCheckboxes()">
                            </th>
                            <th>Date</th>
                            <th>Account</th>
                            <th>Recommending Me</th>
                            <th>My Recommendations</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>
                        {% for record in records %}
                        <tr class="record-row" data-record-id="{{ record.id }}">
                            <td class="checkbox-column">
                                <input type="checkbox" class="record-checkbox" data-record-id="{{ record.id }}" onclick="updateSelectedCount()">
                            </td>
                            <td>{{ record.date.strftime('%Y-%m-%d %I:%M %p PT') }}</td>
                            <td>{{ record.account_name }}</td>
                            <td>{{ record.recommending_me|length }} entries</td>
                            <td>{{ record.my_recommendations|length }} entries</td>
                            <td>
                                <span class="delete-btn" onclick="deleteRecord('{{ record.id }}')">🗑️</span>
                            </td>
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
            
            <script>
                function deleteRecord(id) {
                    if (confirm('Are you sure you want to delete this record?')) {
                        fetch(`/admin/delete-record/${id}`, {method: 'DELETE'})
                            .then(response => response.json())
                            .then(data => {
                                if (data.success) location.reload();
                                else alert('Error deleting record');
                            });
                    }
                }
                
                function reimportCSV() {
                    if (confirm('This will reimport data from CSV. Continue?')) {
                        fetch('/api/test/import-csv')
                            .then(response => response.json())
                            .then(data => {
                                alert(data.message);
                                location.reload();
                            });
                    }
                }
                
                function clearDatabase() {
                    if (confirm('This will delete ALL records! Are you sure?')) {
                        fetch('/admin/clear-database', {method: 'POST'})
                            .then(response => response.json())
                            .then(data => {
                                alert(data.message);
                                location.reload();
                            });
                    }
                }

                async function runScraper() {
                    if (confirm('Are you sure you want to run the scraper now?')) {
                        try {
                            const response = await fetch('/api/run-scraper', {method: 'POST'});
                            const data = await response.json();
                            if (data.success) {
                                alert('Scraper started successfully!');
                                updateLastScrapeTime();
                            } else {
                                alert('Error: ' + data.error);
                            }
                        } catch (error) {
                            alert('Error starting scraper: ' + error);
                        }
                    }
                }

                async function updateLastScrapeTime() {
                    try {
                        const response = await fetch('/api/last-scrape-time');
                        const data = await response.json();
                        const element = document.getElementById('lastScrapeTime');
                        if (data.last_run) {
                            element.textContent = new Date(data.last_run).toLocaleString();
                        } else {
                            element.textContent = 'No successful scrapes yet';
                        }
                    } catch (error) {
                        console.error('Error fetching last scrape time:', error);
                    }
                }

                async function cleanupDuplicates() {
                    if (confirm('This will remove duplicate entries for the same day. Continue?')) {
                        try {
                            const response = await fetch('/admin/cleanup-duplicates', {
                                method: 'POST'
                            });
                            const data = await response.json();
                            if (data.success) {
                                alert(`Cleaned up ${data.duplicates_removed} duplicate entries`);
                                location.reload();
                            } else {
                                alert('Error: ' + data.error);
                            }
                        } catch (error) {
                            alert('Error cleaning duplicates: ' + error);
                        }
                    }
                }

                // Call this when page loads
                updateLastScrapeTime();

                let actionHistory = [];
                
                function addToHistory(action, details) {
                    actionHistory.push({ action, details, timestamp: new Date() });
                    updateHistoryDisplay();
                }
                
                function updateHistoryDisplay() {
                    const historyDiv = document.getElementById('actionHistory');
                    const undoButton = document.getElementById('undoButton');
                    
                    historyDiv.innerHTML = actionHistory.slice(-5).map(item => `
                        <div class="mb-2">
                            <small class="text-muted">${item.timestamp.toLocaleTimeString()}</small>
                            <br>
                            ${item.action}: ${item.details}
                        </div>
                    `).join('');
                    
                    undoButton.disabled = actionHistory.length === 0;
                }
                
                async function undoLastAction() {
                    if (actionHistory.length === 0) return;
                    
                    const lastAction = actionHistory[actionHistory.length - 1];
                    if (confirm(`Are you sure you want to undo: ${lastAction.action}?`)) {
                        try {
                            const response = await fetch('/admin/undo-action', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({ action: lastAction })
                            });
                            const result = await response.json();
                            if (result.success) {
                                actionHistory.pop();
                                updateHistoryDisplay();
                                alert('Action undone successfully');
                                location.reload();
                            } else {
                                alert('Error undoing action: ' + result.error);
                            }
                        } catch (error) {
                            alert('Error: ' + error);
                        }
                    }
                }
                
                function toggleAllCheckboxes() {
                    const selectAll = document.getElementById('selectAll');
                    const checkboxes = document.getElementsByClassName('record-checkbox');
                    Array.from(checkboxes).forEach(checkbox => {
                        checkbox.checked = selectAll.checked;
                    });
                    updateSelectedCount();
                }
                
                function updateSelectedCount() {
                    const selectedCount = document.getElementsByClassName('record-checkbox')
                        .length;
                    const checkedCount = Array.from(document.getElementsByClassName('record-checkbox'))
                        .filter(cb => cb.checked).length;
                    
                    document.getElementById('selectedCount').textContent = checkedCount;
                    document.getElementById('bulkDeleteBtn').style.display = 
                        checkedCount > 0 ? 'inline-block' : 'none';
                    
                    // Update select all checkbox
                    document.getElementById('selectAll').checked = 
                        checkedCount > 0 && checkedCount === selectedCount;
                }
                
                async function bulkDelete() {
                    const selectedIds = Array.from(document.getElementsByClassName('record-checkbox'))
                        .filter(cb => cb.checked)
                        .map(cb => cb.dataset.recordId);
                    
                    if (selectedIds.length === 0) return;
                    
                    if (confirm(`Are you sure you want to delete ${selectedIds.length} records?`)) {
                        try {
                            const response = await fetch('/admin/bulk-delete', {
                                method: 'POST',
                                headers: {
                                    'Content-Type': 'application/json',
                                },
                                body: JSON.stringify({ record_ids: selectedIds })
                            });
                            
                            const result = await response.json();
                            if (result.success) {
                                addToHistory('Bulk Delete', `Deleted ${result.deleted_count} records`);
                                location.reload();
                            } else {
                                alert('Error: ' + result.error);
                            }
                        } catch (error) {
                            alert('Error: ' + error);
                        }
                    }
                }

                async function cleanupInitialData() {
                    if (confirm('This will remove records where we only have partial data for specific partners. Continue?')) {
                        try {
                            const response = await fetch('/admin/cleanup-initial-data', {
                                method: 'POST'
                            });
                            const data = await response.json();
                            if (data.success) {
                                alert(`Cleaned up ${data.records_removed} records with partial data`);
                                addToHistory('Cleanup Initial Data', `Removed ${data.records_removed} partial records`);
                                location.reload();
                            } else {
                                alert('Error: ' + data.error);
                            }
                        } catch (error) {
                            alert('Error cleaning up initial data: ' + error);
                        }
                    }
                }

                document.querySelectorAll('.record-row').forEach(row => {
                    row.addEventListener('click', () => {
                        const recordId = row.dataset.recordId;
                        window.location.href = `/admin/record/${recordId}`;
                    });
                });

                // Add these new functions
                async function showAccountManager() {
                    try {
                        const response = await fetch('/api/available-accounts');
                        const data = await response.json();
                        
                        const accountsList = document.getElementById('accountsList');
                        accountsList.innerHTML = data.accounts.map(account => {
                            const statuses = [];
                            if (data.available_accounts.includes(account)) {
                                statuses.push('<span class="badge bg-success">Available</span>');
                            }
                            if (data.db_accounts.includes(account)) {
                                statuses.push('<span class="badge bg-info">Has Data</span>');
                            }
                            
                            return `
                                <div class="d-flex align-items-center p-2 border-bottom">
                                    <div class="form-check flex-grow-1">
                                        <input class="form-check-input account-checkbox" 
                                               type="checkbox" 
                                               value="${account}" 
                                               id="account_${account.replace(/\s+/g, '_')}"
                                               ${data.enabled_accounts.includes(account) ? 'checked' : ''}>
                                        <label class="form-check-label" for="account_${account.replace(/\s+/g, '_')}">
                                            ${account}
                                        </label>
                                    </div>
                                    <div class="ms-2">
                                        ${statuses.join(' ')}
                                    </div>
                                </div>
                            `;
                        }).join('');
                        
                        document.getElementById('accountManagerSection').style.display = 'block';
                    } catch (error) {
                        console.error('Error loading accounts:', error);
                        alert('Error loading accounts: ' + error.message);
                    }
                }

                function hideAccountManager() {
                    document.getElementById('accountManagerSection').style.display = 'none';
                }

                function toggleAllAccounts(checkbox) {
                    const accountCheckboxes = document.querySelectorAll('.account-checkbox');
                    accountCheckboxes.forEach(box => box.checked = checkbox.checked);
                }

                async function saveAccountSettings() {
                    try {
                        const selectedAccounts = Array.from(document.querySelectorAll('.account-checkbox:checked'))
                            .map(checkbox => checkbox.value);
                        
                        const response = await fetch('/api/update-enabled-accounts', {
                            method: 'POST',
                            headers: {
                                'Content-Type': 'application/json',
                            },
                            body: JSON.stringify({
                                accounts: selectedAccounts
                            })
                        });
                        
                        const data = await response.json();
                        if (data.success) {
                            alert('Account settings saved successfully!');
                            hideAccountManager();
                        } else {
                            alert('Error saving account settings: ' + data.error);
                        }
                    } catch (error) {
                        alert('Error saving account settings: ' + error);
                    }
                }

                // Prevent row click when clicking checkbox
                document.querySelectorAll('.checkbox-column').forEach(col => {
                    col.addEventListener('click', (e) => e.stopPropagation());
                });
            </script>
        </body>
        </html>
        """
        return render_template_string(html, records=records)
    finally:
        session.close()

@app.route('/admin/delete-record/<int:record_id>', methods=['DELETE'])
@login_required
def delete_record(record_id):
    session = db.Session()
    try:
        record = session.query(ReferralData).get(record_id)
        if record:
            session.delete(record)
            session.commit()
            return jsonify({'success': True})
        return jsonify({'success': False, 'error': 'Record not found'})
    finally:
        session.close()

@app.route('/admin/clear-database', methods=['POST'])
@login_required
def clear_database():
    session = db.Session()
    try:
        count = session.query(ReferralData).delete()
        session.commit()
        return jsonify({
            'success': True,
            'message': f'Deleted {count} records'
        })
    finally:
        session.close()

@app.route('/api/debug/database')
@login_required
def debug_database():
    session = db.Session()
    try:
        records = session.query(ReferralData).all()
        return jsonify({
            'total_records': len(records),
            'accounts': list(set(r.account_name for r in records)),
            'sample_dates': [r.date.strftime('%Y-%m-%d') for r in records[:5]],
            'sample_data': [{
                'account': r.account_name,
                'date': r.date.strftime('%Y-%m-%d'),
                'recommending_me': len(r.recommending_me),
                'my_recommendations': len(r.my_recommendations)
            } for r in records[:5]]
        })
    finally:
        session.close()

@app.route('/api/run-scraper', methods=['POST'])
@login_required
def run_scraper():
    try:
        app.logger.info("Starting scraper run")
        
        # Get config file path
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config')
        config_file = os.path.join(config_path, 'enabled_accounts.json')
        
        # Load current config
        try:
            with open(config_file, 'r') as f:
                config_data = json.load(f)
                if isinstance(config_data, list):
                    enabled_accounts = config_data
                    known_accounts = enabled_accounts.copy()
                else:
                    enabled_accounts = config_data.get('enabled', [])
                    known_accounts = config_data.get('known', [])
        except (FileNotFoundError, json.JSONDecodeError):
            enabled_accounts = []
            known_accounts = []
            
        app.logger.info(f"Enabled accounts: {enabled_accounts}")
        
        # First discover any new accounts from ConvertKit
        try:
            from src.scraper.convertkit_scraper import ConvertKitScraper
            scraper = ConvertKitScraper(headless=True)
            
            if scraper.login():
                available_accounts = [acc['name'] for acc in scraper.get_available_accounts()]
                app.logger.info(f"Found accounts in ConvertKit: {available_accounts}")
                
                # Add any new accounts to known list
                new_accounts = [acc for acc in available_accounts if acc not in known_accounts]
                if new_accounts:
                    app.logger.info(f"Found new accounts: {new_accounts}")
                    known_accounts.extend(new_accounts)
                    
                    # Save updated known accounts list
                    with open(config_file, 'w') as f:
                        json.dump({
                            'enabled': enabled_accounts,
                            'known': known_accounts
                        }, f)
            
            # Clean up browser
            try:
                scraper.driver.quit()
            except:
                pass
                
        except Exception as e:
            app.logger.error(f"Error discovering new accounts: {str(e)}")
            # Continue with scraping even if discovery fails
        
        app.logger.info(f"Processing enabled accounts: {enabled_accounts}")
        
        # Initialize and run scraper with just the enabled accounts
        scheduler = ScraperScheduler(enabled_accounts=enabled_accounts)
        scheduler.run_scraper(force=True)  # Force run for manual trigger
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/last-scrape-time')
@login_required
def get_last_scrape_time():
    try:
        with open('last_run.json', 'r') as f:
            data = json.load(f)
            return jsonify({'last_run': data.get('last_run')})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/admin/cleanup-duplicates', methods=['POST'])
@login_required
def cleanup_duplicates():
    session = db.Session()
    try:
        # Get all records ordered by date and account
        records = session.query(ReferralData)\
            .order_by(ReferralData.date.desc(), ReferralData.account_name)\
            .all()
        
        # Group by date and account
        seen = set()
        duplicates = []
        for record in records:
            # Create a key for each day (strip time) and account
            key = (record.date.strftime('%Y-%m-%d'), record.account_name)
            if key in seen:
                duplicates.append(record)
            else:
                seen.add(key)
        
        # Delete duplicates (keep the latest entry for each day)
        for record in duplicates:
            session.delete(record)
        
        session.commit()
        return jsonify({
            'success': True,
            'duplicates_removed': len(duplicates)
        })
    except Exception as e:
        session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/admin/bulk-delete', methods=['POST'])
@login_required
def bulk_delete_records():
    try:
        data = request.get_json()
        record_ids = data.get('record_ids', [])
        
        session = db.Session()
        try:
            deleted_count = session.query(ReferralData)\
                .filter(ReferralData.id.in_(record_ids))\
                .delete(synchronize_session=False)
            session.commit()
            
            return jsonify({
                'success': True,
                'deleted_count': deleted_count,
                'message': f'Successfully deleted {deleted_count} records'
            })
        finally:
            session.close()
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/daily-changes')
@login_required
def get_daily_changes():
    try:
        # Add cache control headers
        response = make_response()
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        
        account = request.args.get('account')
        partner = request.args.get('partner')
        start_date = datetime.strptime(request.args.get('start'), '%Y-%m-%d')
        end_date = datetime.strptime(request.args.get('end'), '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        
        session = db.Session()
        try:
            records = session.query(ReferralData)\
                .filter(ReferralData.account_name == account)\
                .filter(ReferralData.date >= start_date)\
                .filter(ReferralData.date <= end_date)\
                .order_by(ReferralData.date)\
                .all()
            
            # Get the baseline values (first non-zero values)
            baseline_sent = None
            baseline_received = None
            baseline_sent_date = None
            baseline_received_date = None
            
            for record in records:
                sent = next((safe_int_convert(rec['subscribers']) 
                    for rec in record.my_recommendations if rec['creator'] == partner), 0)
                received = next((safe_int_convert(rec['subscribers']) 
                    for rec in record.recommending_me if rec['creator'] == partner), 0)
                
                if sent > 0 and baseline_sent is None:
                    baseline_sent = sent
                    baseline_sent_date = record.date
                if received > 0 and baseline_received is None:
                    baseline_received = received
                    baseline_received_date = record.date
            
            # Calculate daily changes
            changes = []
            for i in range(1, len(records)):
                prev = records[i-1]
                curr = records[i]
                
                sent_change = 0
                received_change = 0
                
                # Get current and previous values
                curr_sent = next((safe_int_convert(rec['subscribers']) 
                    for rec in curr.my_recommendations if rec['creator'] == partner), 0)
                prev_sent = next((safe_int_convert(rec['subscribers']) 
                    for rec in prev.my_recommendations if rec['creator'] == partner), 0)
                    
                curr_received = next((safe_int_convert(rec['subscribers']) 
                    for rec in curr.recommending_me if rec['creator'] == partner), 0)
                prev_received = next((safe_int_convert(rec['subscribers']) 
                    for rec in prev.recommending_me if rec['creator'] == partner), 0)
                
                # Only calculate change if previous value was non-zero
                # This skips the first appearance of data
                if prev_sent > 0:
                    sent_change = curr_sent - prev_sent
                if prev_received > 0:
                    received_change = curr_received - prev_received
                
                changes.append({
                    'date': curr.date.strftime('%-m/%-d'),
                    'sent': sent_change,
                    'received': received_change
                })
            
            return jsonify({
                'daily_changes': changes
            })
            
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error calculating daily changes: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/admin/cleanup-initial-data', methods=['POST'])
@login_required
def cleanup_initial_data():
    session = db.Session()
    try:
        deleted_count = 0
        
        print("\nChecking Dan Murray-Serter records...")
        # Handle Dan Murray-Serter (before Dec 2)
        records = session.query(ReferralData)\
            .filter(ReferralData.account_name == 'Chris Donnelly')\
            .filter(ReferralData.date < datetime(2024, 12, 2))\
            .all()
            
        print(f"Found {len(records)} records before Dec 2")
        for record in records:
            print(f"Checking record from {record.date}")
            dan_recs = [rec for rec in record.recommending_me if rec['creator'] == 'Dan Murray-Serter']
            if dan_recs:
                print(f"Found Dan Murray-Serter recommendation: {dan_recs}")
                session.delete(record)
                deleted_count += 1
        
        print("\nChecking Benchmark records...")
        # Handle Benchmark (Dec 2 only)
        records = session.query(ReferralData)\
            .filter(ReferralData.account_name == 'Chris Donnelly')\
            .filter(ReferralData.date >= datetime(2024, 12, 2))\
            .filter(ReferralData.date < datetime(2024, 12, 3))\
            .all()
            
        print(f"Found {len(records)} records on Dec 2")
        for record in records:
            print(f"Checking record from {record.date}")
            print("My recommendations:", record.my_recommendations)
            print("Recommending me:", record.recommending_me)
            
            # Delete if we have Benchmark in either list
            if any(rec['creator'] == 'Benchmark' for rec in record.recommending_me) or \
               any(rec['creator'] == 'Benchmark' for rec in record.my_recommendations):
                print("Found Benchmark data - deleting record")
                session.delete(record)
                deleted_count += 1
        
        session.commit()
        print(f"\nTotal records deleted: {deleted_count}")
        
        return jsonify({
            'success': True,
            'records_removed': deleted_count
        })
    except Exception as e:
        session.rollback()
        print(f"Error: {str(e)}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
    finally:
        session.close()

@app.route('/data/<path:filename>')
@login_required
def serve_static(filename):
    # Handle avatar files specifically
    if filename.startswith('avatars/'):
        data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
        return send_from_directory(data_dir, filename)
    
    # Handle other static files
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
    return send_from_directory(data_dir, filename)

@app.route('/api/debug/record/<date>')
@login_required
def debug_record(date):
    session = db.Session()
    try:
        date_obj = datetime.strptime(date, '%Y-%m-%d')
        start = date_obj.replace(hour=0, minute=0, second=0)
        end = date_obj.replace(hour=23, minute=59, second=59)
        
        records = session.query(ReferralData)\
            .filter(ReferralData.date.between(start, end))\
            .all()
            
        return jsonify([{
            'date': r.date.isoformat(),
            'account': r.account_name,
            'recommending_me': r.recommending_me,
            'my_recommendations': r.my_recommendations
        } for r in records])
    finally:
        session.close()

@app.route('/admin/record/<int:record_id>')
@login_required
def record_detail(record_id):
    session = db.Session()
    try:
        record = session.query(ReferralData).get(record_id)
        if not record:
            return "Record not found", 404

        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Record Detail</title>
            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
            <style>
                .data-section { margin-bottom: 2rem; }
                .data-table { width: 100%; margin-top: 1rem; }
                .back-button { margin-bottom: 1rem; }
                .metrics { display: flex; gap: 2rem; margin-bottom: 1rem; }
                .metric-card {
                    background: #f8f9fa;
                    padding: 1rem;
                    border-radius: 8px;
                    flex: 1;
                }
                .record-row {
                    cursor: pointer;
                }
                .record-row:hover {
                    background-color: #f8f9fa;
                }
                .checkbox-column {
                    cursor: default;
                }
                .checkbox-column:hover {
                    background-color: inherit !important;
                }
            </style>
        </head>
        <body>
            <div class="container mt-4">
                <a href="/admin/database" class="btn btn-outline-primary back-button">← Back to Database</a>
                
                <h1>Record Detail</h1>
                
                <div class="metrics">
                    <div class="metric-card">
                        <h5>Account</h5>
                        <p class="mb-0">{{ record.account_name }}</p>
                    </div>
                    <div class="metric-card">
                        <h5>Date</h5>
                        <p class="mb-0">{{ record.date.strftime('%Y-%m-%d %H:%M:%S') }}</p>
                    </div>
                    <div class="metric-card">
                        <h5>Total Recommendations</h5>
                        <p class="mb-0">{{ len(record.recommending_me) + len(record.my_recommendations) }}</p>
                    </div>
                </div>

                <div class="data-section">
                    <h3>Recommending Me ({{ len(record.recommending_me) }})</h3>
                    <table class="table data-table">
                        <thead>
                            <tr>
                                <th>Creator</th>
                                <th>Subscribers</th>
                                <th>Conversion Rate</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for rec in record.recommending_me %}
                            <tr>
                                <td>{{ rec['creator'] }}</td>
                                <td>{{ rec['subscribers'] }}</td>
                                <td>{{ rec.get('conversion_rate', 'N/A') }}</td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>

                <div class="data-section">
                    <h3>My Recommendations ({{ len(record.my_recommendations) }})</h3>
                    <table class="table data-table">
                        <thead>
                            <tr>
                                <th>Creator</th>
                                <th>Subscribers</th>
                                <th>Conversion Rate</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for rec in record.my_recommendations %}
                            <tr>
                                <td>{{ rec['creator'] }}</td>
                                <td>{{ rec['subscribers'] }}</td>
                                <td>{{ rec.get('conversion_rate', 'N/A') }}</td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </div>
        </body>
        </html>
        """
        return render_template_string(html, record=record, len=len)
    finally:
        session.close()

@app.route('/admin/import-data', methods=['GET', 'POST'])
@login_required
def import_data():
    if request.method == 'GET':
        # Get list of unique account names from database
        db = DatabaseManager()
        session = db.Session()
        accounts = session.query(ReferralData.account_name).distinct().all()
        account_list = [account[0] for account in accounts]
        session.close()

        return '''
        <html>
        <head>
            <style>
                body { padding: 20px; font-family: Arial; }
                .container { max-width: 1200px; margin: 0 auto; }
                textarea { width: 100%; margin: 10px 0; font-family: monospace; }
                .filters { margin: 20px 0; display: flex; gap: 20px; align-items: center; }
                .filters select, .filters input { padding: 8px; border-radius: 4px; border: 1px solid #ccc; }
                button { padding: 10px 20px; background: #007bff; color: white; border: none; cursor: pointer; border-radius: 4px; }
                button:hover { background: #0056b3; }
                .preview { margin-top: 20px; }
                label { font-weight: bold; }
            </style>
        </head>
        <body>
            <div class="container">
                <h2>Import Referral Data</h2>
                <form method="POST">
                    <div class="filters">
                        <div>
                            <label for="account_name">Account:</label>
                            <select name="account_name" id="account_name" required>
                                <option value="">Select Account</option>
                                ''' + ''.join([f'<option value="{account}">{account}</option>' for account in account_list]) + '''
                            </select>
                        </div>
                        
                        <div>
                            <label for="date">Date:</label>
                            <input type="date" id="date" name="date" required>
                        </div>
                    </div>
                    
                    <h3>Paste CSV Data</h3>
                    <textarea name="csv_data" rows="20" placeholder="date,account_name,tab,creator,subscribers,conversion_rate" required></textarea>
                    
                    <div style="margin-top: 20px;">
                        <button type="button" onclick="previewData()">Preview Data</button>
                        <button type="submit">Import Selected Data</button>
                    </div>
                    
                    <div class="preview" id="preview"></div>
                </form>
            </div>
            
            <script>
            // Set default date to today
            document.getElementById('date').valueAsDate = new Date();
            
            function previewData() {
                const csvData = document.querySelector('textarea[name="csv_data"]').value;
                const account = document.querySelector('select[name="account_name"]').value;
                const date = document.querySelector('input[name="date"]').value;
                
                // Parse CSV and filter data
                const lines = csvData.trim().split('\\n');
                const headers = lines[0].split(',');
                
                const filteredLines = lines.filter(line => {
                    const values = line.split(',');
                    const lineDate = values[0].split(' ')[0];
                    const lineAccount = values[1];
                    
                    return (!account || lineAccount === account) && 
                           (!date || lineDate === date);
                });
                
                // Show preview
                document.getElementById('preview').innerHTML = `
                    <h3>Preview (${filteredLines.length} rows):</h3>
                    <pre>${filteredLines.join('\\n')}</pre>
                `;
            }
            </script>
        </body>
        </html>
        '''

    if request.method == 'POST':
        try:
            csv_data = request.form['csv_data']
            account_name = request.form.get('account_name')
            date = request.form.get('date')
            
            print("1. Received data:")
            print(f"Account: {account_name}")
            print(f"Date: {date}")
            print("CSV sample:", csv_data[:200])
            
            import pandas as pd
            from io import StringIO
            
            # Read CSV with explicit column names
            df = pd.read_csv(StringIO(csv_data), names=[
                'date',
                'account_name',
                'tab',
                'creator',
                'subscribers',
                'conversion_rate'
            ])
            
            print("\n2. DataFrame head:")
            print(df.head())
            
            # Clean up the data
            df['subscribers'] = df['subscribers'].str.replace(',', '')
            df['conversion_rate'] = df['conversion_rate'].str.rstrip('%')
            
            # Filter by account and date
            if account_name:
                df = df[df['account_name'].str.strip() == account_name.strip()]
            if date:
                df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
                filter_date = pd.to_datetime(date).strftime('%Y-%m-%d')
                df = df[df['date'] == filter_date]
            
            print("\n3. After filtering:")
            print(df.head())
            print(f"Filtered rows: {len(df)}")
            
            if len(df) == 0:
                return 'No matching data found to import'
            
            # Group and format for database
            records = []
            for (date, account), group in df.groupby(['date', 'account_name']):
                recommending_me = []
                my_recommendations = []
                
                for _, row in group.iterrows():
                    entry = {
                        'creator': row['creator'],
                        'subscribers': int(float(row['subscribers'])),
                        'conversion_rate': float(row['conversion_rate'])
                    }
                    if row['tab'] == 'recommending_me':
                        recommending_me.append(entry)
                    else:
                        my_recommendations.append(entry)
                
                records.append({
                    'date': date,
                    'account_name': account,
                    'recommending_me': recommending_me,
                    'my_recommendations': my_recommendations
                })
            
            print("\n4. Final records:")
            print(records)
            
            # Save to database
            db = DatabaseManager()
            session = db.Session()
            
            for record in records:
                data = ReferralData(
                    date=pd.to_datetime(record['date']),
                    account_name=record['account_name'],
                    recommending_me=record['recommending_me'],
                    my_recommendations=record['my_recommendations']
                )
                session.add(data)
            
            session.commit()
            session.close()
            
            return f'Successfully imported {len(records)} records!'
            
        except Exception as e:
            print(f"Error occurred: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return f'Error importing data: {str(e)}'

def process_partnership_records(records, partner):
    if not records:
        return {
            'historical_data': {
                'dates': [],
                'received': [],
                'sent': []
            },
            'daily_changes': []
        }
    
    # Add debug logging
    print(f"Processing {len(records)} records for partner {partner}")
    
    # Standardize data format
    dates = []
    received_values = []
    sent_values = []
    
    for record in records:
        dates.append(record.date.strftime('%-m/%-d'))
        
        # Handle received values
        received = next((r['subscribers'] for r in record.recommending_me 
                       if r['creator'] == partner), 0)
        received_values.append(received)
        
        # Handle sent values
        sent = next((r['subscribers'] for r in record.my_recommendations 
                       if r['creator'] == partner), 0)
        sent_values.append(sent)
        
        # Debug log each data point
        print(f"Date: {dates[-1]}, Received: {received}, Sent: {sent}")
    
    # Calculate daily changes
    daily_changes = []
    for i in range(1, len(records)):
        change = {
            'date': dates[i],
            'received': received_values[i] - received_values[i-1],
            'sent': sent_values[i] - sent_values[i-1]
        }
        daily_changes.append(change)
    
    return {
        'historical_data': {
            'dates': dates,
            'received': received_values,
            'sent': sent_values
        },
        'daily_changes': daily_changes
    }

# Add the new debug endpoint
@app.route('/api/debug/creator-science')
@login_required
def debug_creator_science():
    try:
        session = db.Session()
        try:
            # Get all records with Creator Science data
            records = session.query(ReferralData)\
                .order_by(ReferralData.date)\
                .all()
            
            creator_science_data = []
            for record in records:
                sent = next((rec for rec in record.my_recommendations 
                    if rec['creator'] == 'Creator Science'), None)
                received = next((rec for rec in record.recommending_me 
                    if rec['creator'] == 'Creator Science'), None)
                
                if sent or received:
                    creator_science_data.append({
                        'date': record.date.strftime('%Y-%m-%d'),
                        'account': record.account_name,
                        'sent': sent['subscribers'] if sent else None,
                        'received': received['subscribers'] if received else None
                    })
            
            return jsonify({
                'total_records': len(creator_science_data),
                'data': creator_science_data
            })
            
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error debugging Creator Science data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/fix-nathan-barry-dec6', methods=['POST'])
@login_required
def fix_nathan_barry_dec6():
    try:
        session = db.Session()
        try:
            # Find and delete the problematic record
            dec6_date = datetime(2024, 12, 6)
            record = session.query(ReferralData)\
                .filter(ReferralData.account_name == 'Nathan Barry')\
                .filter(ReferralData.date >= dec6_date.replace(hour=0, minute=0, second=0))\
                .filter(ReferralData.date <= dec6_date.replace(hour=23, minute=59, second=59))\
                .first()
            
            if record:
                session.delete(record)
                session.commit()
                return jsonify({
                    'success': True,
                    'message': 'Successfully deleted incorrect Dec 6 record'
                })
            else:
                return jsonify({
                    'success': False,
                    'message': 'Record not found'
                })
            
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error fixing Dec 6 data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/partnership-recommendations', methods=['GET'])
@login_required
def get_partnership_recommendations():
    try:
        account = request.args.get('account')
        print(f"\n=== Partnership Recommendations Debug ===")
        print(f"Account: {account}")

        # Handle Demo Client case first
        if account == "Demo Client":
            # Generate demo recommendations with realistic but fake data
            recommendations = [
                {
                    'partner': 'Creator Weekly',
                    'monthly_volume': 450,
                    'current_partnerships': ['Digital Academy', 'Tech Insights'],
                    'example_partnership': 'Digital Academy',
                    'volume_match': '450 referrals/month (±25% of your volume)'
                },
                {
                    'partner': 'Startup Guide',
                    'monthly_volume': 380,
                    'current_partnerships': ['Business Academy', 'Marketing School'],
                    'example_partnership': 'Business Academy',
                    'volume_match': '380 referrals/month (±25% of your volume)'
                },
                {
                    'partner': 'Tech Insights',
                    'monthly_volume': 420,
                    'current_partnerships': ['Creator Weekly', 'Growth Weekly'],
                    'example_partnership': 'Creator Weekly',
                    'volume_match': '420 referrals/month (±25% of your volume)'
                },
                {
                    'partner': 'Marketing School',
                    'monthly_volume': 390,
                    'current_partnerships': ['Digital Academy', 'Content Pro'],
                    'example_partnership': 'Digital Academy',
                    'volume_match': '390 referrals/month (±25% of your volume)'
                },
                {
                    'partner': 'Business Academy',
                    'monthly_volume': 410,
                    'current_partnerships': ['Growth Weekly', 'Tech Insights'],
                    'example_partnership': 'Growth Weekly',
                    'volume_match': '410 referrals/month (±25% of your volume)'
                }
            ]
            
            return jsonify({
                'recommendations': recommendations,
                'metrics': {
                    'your_avg_volume': 400,  # Demo average volume
                    'top_partners': ['Digital Academy', 'Creator Weekly', 'Growth Weekly']
                }
            })

        # Continue with existing code for real clients...
        # Calculate date range for last 30 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)

        session = db.Session()
        try:
            # Get all records for the account in the date range
            records = session.query(ReferralData)\
                .filter(ReferralData.account_name == account)\
                .filter(ReferralData.date.between(start_date, end_date))\
                .order_by(ReferralData.date.desc())\
                .all()

            print(f"\nFound {len(records)} records for {account}")
            print(f"Date range: {start_date} to {end_date}")

            # Get the most recent record to find top 3 partners by sent volume
            if records:
                latest_record = records[0]
                earliest_record = records[-1]  # Last record is earliest due to desc order
                
                print(f"\nLatest record date: {latest_record.date}")
                print(f"Earliest record date: {earliest_record.date}")
                
                print("\nLatest record my_recommendations:")
                for rec in latest_record.my_recommendations:
                    print(f"  {rec['creator']}: {rec.get('subscribers', 0)}")
                
                print("\nEarliest record my_recommendations:")
                for rec in earliest_record.my_recommendations:
                    print(f"  {rec['creator']}: {rec.get('subscribers', 0)}")
                
                # Get sent values for each partner by calculating period changes
                partner_sent_values = {}
                print("\n=== DETAILED CALCULATION DEBUG ===")
                print("Getting sent values for each partner...")
                
                for partner in set(rec['creator'] for rec in latest_record.my_recommendations):
                    # Get values from earliest and latest records
                    earliest_sent = next((safe_int_convert(rec.get('subscribers', 0))
                        for rec in earliest_record.my_recommendations if rec['creator'] == partner), 0)
                    latest_sent = next((safe_int_convert(rec.get('subscribers', 0))
                        for rec in latest_record.my_recommendations if rec['creator'] == partner), 0)
                    
                    # Calculate period change
                    period_sent = latest_sent - earliest_sent
                    print(f"\nPartner: {partner}")
                    print(f"  Latest sent: {latest_sent}")
                    print(f"  Earliest sent: {earliest_sent}")
                    print(f"  Period change: {period_sent}")
                    
                    if period_sent > 0:  # Only include positive period changes
                        partner_sent_values[partner] = period_sent
                        print(f"  ✓ Added to calculation (positive change)")
                    else:
                        print(f"  ✗ Excluded from calculation (zero or negative change)")

                print("\nAll collected values before sorting:")
                for partner, value in partner_sent_values.items():
                    print(f"  {partner}: {value}")

                # Sort by sent volume and get top 3
                sorted_partners = sorted(
                    partner_sent_values.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:3]

                print("\nTop 3 partners after sorting:")
                for partner, value in sorted_partners:
                    print(f"  {partner}: {value}")

                # Calculate average from top 3 sent values
                sent_volumes = [sent for _, sent in sorted_partners]
                raw_avg = sum(sent_volumes) / len(sent_volumes) if sent_volumes else 0
                
                print("\nFinal calculation:")
                print(f"Sum of top 3: {sum(sent_volumes)}")
                print(f"Number of values: {len(sent_volumes)}")
                print(f"Raw average: {raw_avg}")
                print(f"Rounded average: {round(raw_avg)}")
                print("=== END DEBUG ===\n")

                # Calculate volume range (±25%)
                min_volume = raw_avg * 0.75
                max_volume = raw_avg * 1.25
                print(f"Looking for partners between {min_volume} and {max_volume} referrals/month")

                # Get all records from other accounts in the date range
                other_records = session.query(ReferralData)\
                    .filter(ReferralData.account_name != account)\
                    .filter(ReferralData.date.between(start_date, end_date))\
                    .order_by(ReferralData.date.desc())\
                    .all()

                # Calculate potential partner volumes
                potential_volumes = {}
                for record in other_records:
                    # Track individual creators - look at what they RECEIVE
                    for rec in record.recommending_me:
                        partner = rec['creator']
                        received = safe_int_convert(rec['subscribers'])
                        if received > 0:  # Only track non-zero volumes
                            potential_volumes[partner] = {
                                'volume': max(potential_volumes.get(partner, {}).get('volume', 0), received),
                                'type': 'creator',
                                'client': record.account_name
                            }
                    
                    # Track client accounts - look at what they SEND
                    sent_volumes = [
                        safe_int_convert(rec['subscribers']) 
                        for rec in record.my_recommendations 
                        if safe_int_convert(rec['subscribers']) > 0  # Only count non-zero recommendations
                    ]
                    
                    if sent_volumes:  # Only include if they have valid sent recommendations
                        total_sent = sum(sent_volumes)
                        if total_sent > 0:
                            potential_volumes[record.account_name] = {
                                'volume': max(potential_volumes.get(record.account_name, {}).get('volume', 0), total_sent),
                                'type': 'client',
                                'active_partnerships': len(sent_volumes)
                            }

                print(f"Potential volumes: {potential_volumes}")

                # Filter for partners within volume range
                matching_partners = {
                    partner: data 
                    for partner, data in potential_volumes.items()
                    if min_volume <= data['volume'] <= max_volume
                }
                print(f"Matching partners before exclusion: {matching_partners}")

                # Exclude current partners and self
                current_partners = {p['creator'] for p in latest_record.my_recommendations} if records else set()
                current_partners.add(account)
                matching_partners = {
                    partner: data 
                    for partner, data in matching_partners.items()
                    if partner not in current_partners
                }
                print(f"Final matching partners: {matching_partners}")

                # Get recommendations
                recommendations = []
                for partner, data in sorted(matching_partners.items(), key=lambda x: x[1]['volume'], reverse=True)[:10]:
                    # Find partner's current partnerships
                    partner_partnerships = set()
                    for record in other_records:
                        if data['type'] == 'creator':
                            if any(rec['creator'] == partner for rec in record.recommending_me):
                                partner_partnerships.add(record.account_name)
                        else:  # client type
                            if record.account_name == partner:
                                partner_partnerships.update(rec['creator'] for rec in record.recommending_me)

                    recommendations.append({
                        'partner': partner,
                        'monthly_volume': data['volume'],
                        'current_partnerships': list(partner_partnerships),
                        'volume_match': f"{data['volume']:,} referrals/month (±25% of your volume)",
                        'example_partnership': next(iter(partner_partnerships)) if partner_partnerships else None,
                        'type': data['type'],
                        'client': data.get('client') if data['type'] == 'creator' else None
                    })

                response_data = {
                    'recommendations': recommendations,
                    'metrics': {
                        'your_avg_volume': round(raw_avg),  # Round the raw average for display
                        'top_partners': [p for p, _ in sorted_partners] if records else []
                    }
                }
                print(f"Response data: {response_data}")
                print("=== End Debug ===\n")

                return jsonify(response_data)

        finally:
            session.close()
            
    except Exception as e:
        print(f"Error generating recommendations: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': 'Internal server error'}), 500

@app.route('/api/demo-data')
@login_required
def get_demo_data():
    """Get demo data for demonstration purposes"""
    try:
        demo_data = db.generate_demo_data()
        
        # Convert to format expected by frontend
        formatted_data = []
        for record in demo_data:
            formatted_data.append({
                'date': record.date.isoformat(),
                'account_name': record.account_name,
                'recommending_me': record.recommending_me,
                'my_recommendations': record.my_recommendations
            })
            
        return jsonify(formatted_data)
    except Exception as e:
        print(f"Error generating demo data: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/trends/demo/<partner_name>')
@login_required
def get_demo_trends(partner_name):
    """Get trend data for a demo partnership"""
    try:
        demo_data = db.generate_demo_data()
        
        trend_data = {
            'dates': [],
            'received': [],
            'sent': [],
            'balance': []
        }
        
        # Process each day's data
        for record in demo_data:
            # Find partner data in receiving list
            received = next(
                (int(rec['subscribers']) for rec in record.recommending_me 
                 if rec['creator'] == partner_name), 
                0
            )
            
            # Find partner data in sending list
            sent = next(
                (int(rec['subscribers']) for rec in record.my_recommendations 
                 if rec['creator'] == partner_name), 
                0
            )
            
            trend_data['dates'].append(record.date.strftime('%Y-%m-%d'))
            trend_data['received'].append(received)
            trend_data['sent'].append(sent)
            trend_data['balance'].append(received - sent)
            
        return jsonify(trend_data)
        
    except Exception as e:
        print(f"Error getting demo trends: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/available-accounts')
@login_required
def get_available_accounts():
    try:
        session = db.Session()
        try:
            # Get all accounts from AllowedAccount table
            allowed_accounts = session.query(AllowedAccount).all()
            
            # Get accounts that have data in ReferralData
            db_accounts = session.query(ReferralData.account_name).distinct().all()
            db_accounts = [account[0] for account in db_accounts]
            
            # Separate into enabled and known accounts
            enabled_accounts = [acc.account_name for acc in allowed_accounts if acc.is_active]
            known_accounts = [acc.account_name for acc in allowed_accounts]
            
            # Add Demo Client if not present
            if 'Demo Client' not in enabled_accounts:
                enabled_accounts.append('Demo Client')
            if 'Demo Client' not in known_accounts:
                known_accounts.append('Demo Client')
                # Also add to database if not exists
                demo_account = session.query(AllowedAccount).filter_by(account_name='Demo Client').first()
                if not demo_account:
                    demo_account = AllowedAccount(account_name='Demo Client', is_active=True)
                    session.add(demo_account)
                    session.commit()
            
            # Return all required data
            return jsonify({
                'success': True,
                'accounts': sorted(list(set(known_accounts))),  # All known accounts
                'enabled_accounts': sorted(list(set(enabled_accounts))),  # Currently enabled accounts
                'db_accounts': sorted(list(set(db_accounts))),  # Accounts with data in DB
                'available_accounts': sorted(list(set(known_accounts)))  # All available accounts
            })
        except Exception as e:
            print(f"Error getting available accounts: {str(e)}")
            return jsonify({
                'success': False,
                'error': str(e)
            })
        finally:
            session.close()
    except Exception as e:
        print(f"Error getting available accounts: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/api/update-enabled-accounts', methods=['POST'])
@login_required
def update_enabled_accounts():
    try:
        data = request.get_json()
        enabled_accounts = data.get('accounts', [])
        
        # Get config file path
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config')
        os.makedirs(config_path, exist_ok=True)
        config_file = os.path.join(config_path, 'enabled_accounts.json')
        
        # Load existing config to get known accounts
        try:
            with open(config_file, 'r') as f:
                config_data = json.load(f)
                if isinstance(config_data, list):
                    # Old format - migrate to new format
                    known_accounts = config_data
                else:
                    known_accounts = config_data.get('known', [])
        except (FileNotFoundError, json.JSONDecodeError):
            known_accounts = []
        
        # Add any newly enabled accounts to known accounts
        known_accounts.extend([acc for acc in enabled_accounts if acc not in known_accounts])
        
        # Save both lists to JSON
        with open(config_file, 'w') as f:
            json.dump({
                'enabled': enabled_accounts,
                'known': known_accounts
            }, f)
            
        # Update database
        session = db.Session()
        try:
            # First, get all known accounts and set them as inactive
            for account_name in known_accounts:
                account = session.query(AllowedAccount).filter_by(account_name=account_name).first()
                if account:
                    account.is_active = False
                else:
                    # Create new account record if it doesn't exist
                    account = AllowedAccount(account_name=account_name, is_active=False)
                    session.add(account)
            
            # Then set enabled accounts as active
            for account_name in enabled_accounts:
                account = session.query(AllowedAccount).filter_by(account_name=account_name).first()
                if account:
                    account.is_active = True
                else:
                    # Create new account record if it doesn't exist
                    account = AllowedAccount(account_name=account_name, is_active=True)
                    session.add(account)
            
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()
            
        return jsonify({'success': True})
    except Exception as e:
        print(f"Error updating enabled accounts: {str(e)}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/bulk_delete', methods=['POST'])
@login_required
def bulk_delete():
    try:
        entries = request.json.get('entries', [])
        if not entries:
            return jsonify({'error': 'No entries selected'}), 400
            
        # Delete entries from database
        with DBManager() as db:
            for entry_id in entries:
                db.delete_entry(entry_id)
                
        return jsonify({'message': f'Successfully deleted {len(entries)} entries'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

if __name__ == '__main__':
    print("Starting Flask server...")
    print("API endpoints:")
    print("  - GET /")
    print("  - GET /api/partnership-metrics")
    print("  - GET /api/earliest-date")
    print("  - GET /api/largest-imbalances")
    print("  - GET /api/trends/<account_name>")
    print("  - GET /api/trends/summary")
    print("  - GET /debug/db")
    print("  - GET /debug/trends")
    print("  - GET /api/test/import-csv")
    print("  - GET /admin/database")
    print("  - DELETE /admin/delete-record/<int:record_id>")
    print("  - POST /admin/clear-database")
    print("  - POST /api/run-scraper")
    print("  - GET /api/last-scrape-time")
    print("  - POST /admin/cleanup-duplicates")
    print("  - POST /admin/bulk-delete")
    print("  - GET /api/available-accounts")
    print("  - POST /api/update-enabled-accounts")
    print("  - POST /bulk_delete")
    app.run(host='0.0.0.0', port=5001, debug=True)
