from flask import Flask, request, jsonify
from flask_cors import CORS
from data.db_manager import DatabaseManager
from datetime import datetime
import os

app = Flask(__name__)
CORS(app)
db = DatabaseManager()

@app.route('/api/partnership-metrics')
def get_partnership_metrics():
    account = request.args.get('account')
    start_date = request.args.get('start')
    end_date = request.args.get('end')
    
    if not account:
        return jsonify({'error': 'Account is required'}), 400
        
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d') if start_date else None
        end = datetime.strptime(end_date, '%Y-%m-%d') if end_date else None
        
        metrics = db.generate_partnership_metrics(account, start, end)
        return jsonify(metrics)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)