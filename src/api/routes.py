from flask import Flask, request, jsonify, render_template_string, send_from_directory
from flask_cors import CORS
from datetime import datetime, timedelta
import sys
import os
import pandas as pd
import random
from sqlalchemy.sql import func
import json

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.data.db_manager import DatabaseManager, ReferralData
from src.scraper.scheduler import ScraperScheduler

app = Flask(__name__)
# Get environment variables with defaults
FRONTEND_URL = os.getenv('FRONTEND_URL', 'http://localhost:8000')
ALLOWED_ORIGINS = [
    'http://localhost:8000',  # Local development
    'https://your-heroku-app.herokuapp.com',  # Production URL
    FRONTEND_URL  # Dynamic URL from environment
]

# Configure CORS with multiple origins
CORS(app, resources={
    r"/api/*": {
        "origins": ALLOWED_ORIGINS,
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type"],
        "supports_credentials": True
    }
})
db = DatabaseManager()

def safe_int_convert(value):
    """Safely convert a value to integer, handling empty strings and commas"""
    if not value:  # Handle empty strings
        return 0
    try:
        # Remove commas and convert to int
        return int(str(value).replace(',', ''))
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
def get_partnership_metrics():
    try:
        # Use default date range if not provided
        start_date_str = request.args.get('start')
        end_date_str = request.args.get('end')

        if not start_date_str:
            start_date_str = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        if not end_date_str:
            end_date_str = datetime.now().strftime('%Y-%m-%d')

        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

        account = request.args.get('account')
        
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
            
            # Process metrics
            results = []
            all_partners = set()
            partner_metrics = {}
            
            # Process each account's records
            for acc_records in account_records.values():
                if len(acc_records) <= 1:
                    continue
                    
                period_start = acc_records[0]  # First record in the selected period
                period_end = acc_records[-1]   # Last record in the selected period
                
                print(f"\nProcessing records for {period_start.account_name}")
                print(f"Period start date: {period_start.date}")
                print(f"Period end date: {period_end.date}")
                
                # Get all unique partners from both records
                all_partners = set()
                for record in [period_start, period_end]:
                    all_partners.update(rec['creator'] for rec in record.recommending_me)
                    all_partners.update(rec['creator'] for rec in record.my_recommendations)
                
                for partner in all_partners:
                    key = partner if account == 'all' else f"{partner}_{period_start.account_name}"
                    
                    if key not in partner_metrics:
                        partner_metrics[key] = {
                            'partner': partner,
                            'account': period_start.account_name if account != 'all' else 'All Clients',
                            'period_received': 0,
                            'period_sent': 0,
                            'latest_received': 0,
                            'latest_sent': 0
                        }
                    
                    # Get period start values (for calculating changes)
                    period_start_received = next((safe_int_convert(rec['subscribers']) 
                        for rec in period_start.recommending_me if rec['creator'] == partner), 0)
                    period_start_sent = next((safe_int_convert(rec['subscribers']) 
                        for rec in period_start.my_recommendations if rec['creator'] == partner), 0)
                    
                    # Get period end values
                    period_end_received = next((safe_int_convert(rec['subscribers']) 
                        for rec in period_end.recommending_me if rec['creator'] == partner), 0)
                    period_end_sent = next((safe_int_convert(rec['subscribers']) 
                        for rec in period_end.my_recommendations if rec['creator'] == partner), 0)
                    
                    # Calculate period changes (end minus start)
                    partner_metrics[key]['period_received'] = period_end_received - period_start_received
                    partner_metrics[key]['period_sent'] = period_end_sent - period_start_sent
                    
                    # Store latest values for all-time balance
                    partner_metrics[key]['latest_received'] = period_end_received
                    partner_metrics[key]['latest_sent'] = period_end_sent
            
            # Convert accumulated metrics to results
            for metrics in partner_metrics.values():
                metrics['period_balance'] = metrics['period_received'] - metrics['period_sent']
                metrics['all_time_balance'] = metrics['latest_received'] - metrics['latest_sent']
                results.append(metrics)
            
            # Sort by absolute period balance
            results.sort(key=lambda x: abs(x['period_balance']), reverse=True)
            
            print("\n=== Debug Info ===")
            print(f"Date range: {start_date} to {end_date}")
            
            for acc_records in account_records.values():
                print(f"\nAccount: {acc_records[0].account_name}")
                print(f"Number of records: {len(acc_records)}")
                print(f"Record dates: {[r.date.strftime('%Y-%m-%d') for r in acc_records]}")
                
                if len(acc_records) <= 1:
                    print("Skipping - not enough records")
                    continue
                    
                period_start = acc_records[0]
                period_end = acc_records[-1]
                
                print(f"Period start date: {period_start.date}")
                print(f"Period end date: {period_end.date}")
                
                for partner in all_partners:
                    print(f"\nProcessing partner: {partner}")
                    
                    # Get values from records
                    period_start_received = next((safe_int_convert(rec['subscribers']) 
                        for rec in period_start.recommending_me if rec['creator'] == partner), 0)
                    period_start_sent = next((safe_int_convert(rec['subscribers']) 
                        for rec in period_start.my_recommendations if rec['creator'] == partner), 0)
                    
                    period_end_received = next((safe_int_convert(rec['subscribers']) 
                        for rec in period_end.recommending_me if rec['creator'] == partner), 0)
                    period_end_sent = next((safe_int_convert(rec['subscribers']) 
                        for rec in period_end.my_recommendations if rec['creator'] == partner), 0)
                    
                    print(f"  Period start received: {period_start_received}")
                    print(f"  Period start sent: {period_start_sent}")
                    print(f"  Period end received: {period_end_received}")
                    print(f"  Period end sent: {period_end_sent}")
                    
                    # Rest of the code remains the same
            
            return jsonify(results)
            
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error in partnership metrics: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/earliest-date')
def get_earliest_date():
    earliest_date = db.get_earliest_data_date()
    return jsonify({'earliest_date': earliest_date.isoformat() if earliest_date else None})

@app.route('/api/largest-imbalances')
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
def get_account_trends(account_name):
    try:
        days = request.args.get('days', default=30, type=int)
        end_date = request.args.get('end_date')
        
        if end_date:
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        else:
            end_date = datetime.now()
            
        start_date = end_date - timedelta(days=days)
        
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
        print(f"Error generating trends: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/trends/summary')
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

@app.route('/')
def home():
    return jsonify({
        "status": "ok",
        "message": "API is running",
        "endpoints": [
            "/api/partnership-metrics",
            "/api/earliest-date",
            "/api/largest-imbalances",
            "/api/trends/<account_name>",
            "/api/trends/summary",
            "/debug/db"
        ]
    })

@app.route('/debug/trends')
def debug_trends():
    account = request.args.get('account', 'test_account')
    days = int(request.args.get('days', '30'))
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    return jsonify({
        'trends': db.get_account_trends(account, start_date, end_date),
        'growth': db.calculate_growth_metrics(account, start_date, end_date),
        'summary': db.get_trends_summary(days)
    })

@app.route('/api/test/import-csv')
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
def get_partnership_trends():
    try:
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
                
            # Add debug logging here
            print(f"\nFetched records for {account} and {partner}")
            print(f"Date range: {start_date} to {end_date}")
            print(f"Number of records before interpolation: {len(records)}")
            for record in records:
                print(f"\nDate: {record.date}")
                sent = next((rec['subscribers'] for rec in record.my_recommendations if rec['creator'] == partner), 'None')
                received = next((rec['subscribers'] for rec in record.recommending_me if rec['creator'] == partner), 'None')
                print(f"Sent: {sent}")
                print(f"Received: {received}")
            
            # Apply interpolation to fill missing days
            records = interpolate_missing_days(records, start_date, end_date)
            
            # Get raw subscriber counts for each day
            response_data = {
                'historical_data': {
                    'dates': [r.date.strftime('%-m/%-d') for r in records],
                    'received': [
                        next((safe_int_convert(rec['subscribers']) 
                            for rec in r.recommending_me if rec['creator'] == partner), 0)
                        for r in records
                    ],
                    'sent': [
                        next((safe_int_convert(rec['subscribers']) 
                            for rec in r.my_recommendations if rec['creator'] == partner), 0)
                        for r in records
                    ]
                }
            }
            
            return jsonify(response_data)
            
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error in partnership trends: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/partnership-metrics', methods=['OPTIONS'])
def handle_options():
    response = jsonify({'status': 'ok'})
    return response

@app.route('/admin/database')
def database_admin():
    session = db.Session()
    try:
        # Get all records grouped by date and account
        records = session.query(ReferralData)\
            .order_by(ReferralData.date.desc(), ReferralData.account_name)\
            .all()

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
            </style>
        </head>
        <body>
            <div class="container mt-4">
                <h1>Database Management</h1>
                
                <div class="action-buttons">
                    <div class="primary-actions">
                        <button class="btn btn-success" onclick="runScraper()">Run Scraper Now</button>
                        <button id="bulkDeleteBtn" class="btn btn-danger" onclick="bulkDelete()">
                            Delete Selected (<span id="selectedCount">0</span>)
                        </button>
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
                        <tr class="record-row">
                            <td class="checkbox-column">
                                <input type="checkbox" class="record-checkbox" data-record-id="{{ record.id }}" onclick="updateSelectedCount()">
                            </td>
                            <td>{{ record.date.strftime('%Y-%m-%d %H:%M:%S') }}</td>
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
            </script>
        </body>
        </html>
        """
        return render_template_string(html, records=records)
    finally:
        session.close()

@app.route('/admin/delete-record/<int:record_id>', methods=['DELETE'])
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
def run_scraper():
    try:
        scheduler = ScraperScheduler()
        scheduler.run_scraper()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/last-scrape-time')
def get_last_scrape_time():
    try:
        with open('last_run.json', 'r') as f:
            data = json.load(f)
            return jsonify({'last_run': data.get('last_run')})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/admin/cleanup-duplicates', methods=['POST'])
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
def get_daily_changes():
    try:
        account = request.args.get('account')
        partner = request.args.get('partner')
        start_date = datetime.strptime(request.args.get('start'), '%Y-%m-%d')
        end_date = datetime.strptime(request.args.get('end'), '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        
        print(f"\nCalculating daily changes for {account} and {partner}")
        
        session = db.Session()
        try:
            records = session.query(ReferralData)\
                .filter(ReferralData.account_name == account)\
                .filter(ReferralData.date >= start_date)\
                .filter(ReferralData.date <= end_date)\
                .order_by(ReferralData.date)\
                .all()
            
            # Apply interpolation to fill missing days
            records = interpolate_missing_days(records, start_date, end_date)
            
            print(f"Found {len(records)} records after interpolation")
            print("Dates:", [r.date.strftime('%Y-%m-%d') for r in records])
            
            # Calculate daily changes
            changes = []
            for i in range(1, len(records)):
                prev = records[i-1]
                curr = records[i]
                
                print(f"\nComparing {prev.date.strftime('%Y-%m-%d')} to {curr.date.strftime('%Y-%m-%d')}")
                
                # Get sent values
                prev_sent_rec = next((rec for rec in prev.my_recommendations if rec['creator'] == partner), None)
                curr_sent_rec = next((rec for rec in curr.my_recommendations if rec['creator'] == partner), None)
                
                # Get received values
                prev_received_rec = next((rec for rec in prev.recommending_me if rec['creator'] == partner), None)
                curr_received_rec = next((rec for rec in curr.recommending_me if rec['creator'] == partner), None)
                
                print(f"Previous sent: {prev_sent_rec['subscribers'] if prev_sent_rec else 'None'}")
                print(f"Current sent: {curr_sent_rec['subscribers'] if curr_sent_rec else 'None'}")
                print(f"Previous received: {prev_received_rec['subscribers'] if prev_received_rec else 'None'}")
                print(f"Current received: {curr_received_rec['subscribers'] if curr_received_rec else 'None'}")
                
                # Calculate changes using interpolated values
                sent_change = 0
                received_change = 0
                
                if prev_sent_rec and curr_sent_rec:
                    sent_change = safe_int_convert(curr_sent_rec['subscribers']) - safe_int_convert(prev_sent_rec['subscribers'])
                
                if prev_received_rec and curr_received_rec:
                    received_change = safe_int_convert(curr_received_rec['subscribers']) - safe_int_convert(prev_received_rec['subscribers'])
                
                changes.append({
                    'date': curr.date.strftime('%-m/%-d'),
                    'sent': sent_change,
                    'received': received_change
                })
            
            print("\nFinal changes:", changes)
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
def serve_static(filename):
    # Handle avatar files specifically
    if filename.startswith('avatars/'):
        data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
        return send_from_directory(data_dir, filename)
    
    # Handle other static files
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data')
    return send_from_directory(data_dir, filename)

@app.route('/api/debug/record/<date>')
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
    app.run(host='0.0.0.0', port=5001, debug=True)
