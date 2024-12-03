from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
from datetime import datetime, timedelta
import sys
import os
import pandas as pd
import random
from sqlalchemy.sql import func

# Add the project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.data.db_manager import DatabaseManager, ReferralData

app = Flask(__name__)
# Configure CORS to allow all origins for development
CORS(app, resources={
    r"/api/*": {
        "origins": ["http://localhost:8000"],
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

@app.route('/api/partnership-metrics')
def get_partnership_metrics():
    account = request.args.get('account')
    start_date_str = request.args.get('start')
    end_date_str = request.args.get('end')
    
    print("\n=== Partnership Metrics API Request ===")
    print(f"Account: {account}")
    print(f"Start date: {start_date_str}")
    print(f"End date: {end_date_str}")
    
    try:
        # Convert dates and set time to include all of today
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').replace(hour=0, minute=0, second=0)
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        
        # Add logging to debug date ranges
        print(f"Querying data from {start_date} to {end_date}")
        
        session = db.Session()
        try:
            records = session.query(ReferralData)\
                .filter(ReferralData.account_name == account)\
                .filter(ReferralData.date <= end_date)\
                .filter(ReferralData.date >= start_date)\
                .order_by(ReferralData.date)\
                .all()
                
            print(f"Found {len(records)} records")
            for record in records:
                print(f"Record date: {record.date}")
                
            if not records:
                return jsonify([])
            
            # Use first and last records in the period
            baseline_record = records[0]  # First record in period
            latest_record = records[-1]   # Last record in period
            
            # Process metrics
            results = []
            all_partners = set()
            
            # Get all unique partners from both records
            for record in [baseline_record, latest_record]:
                all_partners.update(rec['creator'] for rec in record.recommending_me)
                all_partners.update(rec['creator'] for rec in record.my_recommendations)
            
            for partner in all_partners:
                # Get baseline values
                baseline_received = next((safe_int_convert(rec['subscribers']) 
                    for rec in baseline_record.recommending_me if rec['creator'] == partner), 0)
                baseline_sent = next((safe_int_convert(rec['subscribers']) 
                    for rec in baseline_record.my_recommendations if rec['creator'] == partner), 0)
                
                # Get latest values
                latest_received = next((safe_int_convert(rec['subscribers']) 
                    for rec in latest_record.recommending_me if rec['creator'] == partner), 0)
                latest_sent = next((safe_int_convert(rec['subscribers']) 
                    for rec in latest_record.my_recommendations if rec['creator'] == partner), 0)
                
                # Calculate metrics
                period_received = latest_received - baseline_received
                period_sent = latest_sent - baseline_sent
                period_balance = period_received - period_sent
                all_time_balance = latest_received - latest_sent
                
                print(f"\nPartner: {partner}")
                print(f"Baseline - Received: {baseline_received}, Sent: {baseline_sent}")
                print(f"Latest - Received: {latest_received}, Sent: {latest_sent}")
                print(f"Period changes - Received: {period_received}, Sent: {period_sent}")
                
                results.append({
                    'partner': partner,
                    'period_received': period_received,
                    'period_sent': period_sent,
                    'period_balance': period_balance,
                    'all_time_balance': all_time_balance
                })
            
            # Sort by absolute period balance
            results.sort(key=lambda x: abs(x['period_balance']), reverse=True)
            return jsonify(results)
            
        finally:
            session.close()
            
    except Exception as e:
        print(f"Error processing request: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/earliest-date')
def get_earliest_date():
    earliest_date = db.get_earliest_data_date()
    return jsonify({'earliest_date': earliest_date.isoformat() if earliest_date else None})

@app.route('/api/largest-imbalances')
def get_largest_imbalances():
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    
    try:
        session = db.Session()
        try:
            # Get latest data for all-time stats
            latest_date = session.query(func.max(ReferralData.date)).scalar()
            latest_records = session.query(ReferralData)\
                .filter(ReferralData.date == latest_date)\
                .all()
            
            # Initialize stats dictionaries
            latest_stats = {}
            period_stats = {}
            
            # Process latest data for all accounts
            for latest_record in latest_records:
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
                
                # Get all records in the period
                period_records = session.query(ReferralData)\
                    .filter(ReferralData.date.between(start, end))\
                    .all()
                
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
        
        print(f"\n=== Partnership Trends Request ===")
        print(f"Account: {account}")
        print(f"Partner: {partner}")
        print(f"Date range: {start_date} to {end_date}")
        
        session = db.Session()
        try:
            records = session.query(ReferralData)\
                .filter(ReferralData.account_name == account)\
                .filter(ReferralData.date >= start_date)\
                .filter(ReferralData.date <= end_date)\
                .order_by(ReferralData.date)\
                .all()
            
            print(f"Found {len(records)} records")
            for record in records:
                print(f"Record date: {record.date}")
            
            if not records:
                return jsonify({'error': 'No data found'})
            
            # Get baseline and latest records
            baseline_record = records[0]
            latest_record = records[-1]
            
            # Get metrics for the specific partner
            baseline_received = next((safe_int_convert(rec['subscribers']) 
                for rec in baseline_record.recommending_me if rec['creator'] == partner), 0)
            baseline_sent = next((safe_int_convert(rec['subscribers']) 
                for rec in baseline_record.my_recommendations if rec['creator'] == partner), 0)
            
            latest_received = next((safe_int_convert(rec['subscribers']) 
                for rec in latest_record.recommending_me if rec['creator'] == partner), 0)
            latest_sent = next((safe_int_convert(rec['subscribers']) 
                for rec in latest_record.my_recommendations if rec['creator'] == partner), 0)
            
            # Calculate changes
            received_change = latest_received - baseline_received
            sent_change = latest_sent - baseline_sent
            days_between = max((latest_record.date - baseline_record.date).days, 1)
            
            response_data = {
                'historical_data': {
                    'dates': [r.date.strftime('%Y-%m-%d') for r in records],
                    'received': [next((safe_int_convert(rec['subscribers']) 
                        for rec in r.recommending_me if rec['creator'] == partner), 0) for r in records],
                    'sent': [next((safe_int_convert(rec['subscribers']) 
                        for rec in r.my_recommendations if rec['creator'] == partner), 0) for r in records]
                },
                'daily_changes': {
                    'dates': [r.date.strftime('%Y-%m-%d') for r in records[1:]],  # Skip first date
                    'received': [
                        curr - prev for curr, prev in zip(
                            [next((safe_int_convert(rec['subscribers']) 
                                for rec in r.recommending_me if rec['creator'] == partner), 0) for r in records][1:],
                            [next((safe_int_convert(rec['subscribers']) 
                                for rec in r.recommending_me if rec['creator'] == partner), 0) for r in records][:-1]
                        )
                    ],
                    'sent': [
                        curr - prev for curr, prev in zip(
                            [next((safe_int_convert(rec['subscribers']) 
                                for rec in r.my_recommendations if rec['creator'] == partner), 0) for r in records][1:],
                            [next((safe_int_convert(rec['subscribers']) 
                                for rec in r.my_recommendations if rec['creator'] == partner), 0) for r in records][:-1]
                        )
                    ]
                },
                'growth': {
                    'daily_change': round((received_change + sent_change) / days_between, 1),
                    'growth_rate': round(((latest_received + latest_sent) / max(baseline_received + baseline_sent, 1) - 1) * 100, 1),
                    'trend_direction': 'Improving' if received_change > sent_change else 'Declining'
                }
            }
            
            print(f"Sending response: {response_data}")
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
                .record-row:hover { background-color: #f5f5f5; }
                .delete-btn { color: red; cursor: pointer; }
            </style>
        </head>
        <body>
            <div class="container mt-4">
                <h1>Database Management</h1>
                <div class="mb-3">
                    <button class="btn btn-primary" onclick="reimportCSV()">Reimport from CSV</button>
                    <button class="btn btn-danger" onclick="clearDatabase()">Clear Database</button>
                </div>
                <table class="table">
                    <thead>
                        <tr>
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
    app.run(host='0.0.0.0', port=5001, debug=True)
