from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, JSON, func, desc, case
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
from ..utils.config import DATABASE_URL
import json

Base = declarative_base()

class ReferralData(Base):
    __tablename__ = 'referral_data'
    
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, default=datetime.utcnow)
    account_name = Column(String)
    recommending_me = Column(JSON)
    my_recommendations = Column(JSON)

class DatabaseManager:
    def __init__(self):
        self.engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
    
    def save_data(self, account_name, recommending_me, my_recommendations):
        """Save referral data to database"""
        session = self.Session()
        try:
            data = ReferralData(
                account_name=account_name,
                recommending_me=recommending_me,
                my_recommendations=my_recommendations
            )
            session.add(data)
            session.commit()
            print(f"Data saved for account: {account_name}")
        except Exception as e:
            print(f"Error saving data: {str(e)}")
            session.rollback()
        finally:
            session.close()
    
    def get_latest_data(self, account_name=None):
        """Get latest data for one or all accounts"""
        session = self.Session()
        try:
            query = session.query(ReferralData)
            if account_name:
                query = query.filter(ReferralData.account_name == account_name)
            return query.order_by(ReferralData.date.desc()).all()
        finally:
            session.close()
    
    def generate_partnership_metrics(self, account_name, start_date=None, end_date=None):
        session = self.Session()
        try:
            print("\n=== Starting generate_partnership_metrics ===")
            print(f"Account: {account_name}")
            print(f"Start date: {start_date}")
            print(f"End date: {end_date}")
            
            # Get all data for the account
            query = session.query(ReferralData).filter(ReferralData.account_name == account_name)
            all_data = query.order_by(ReferralData.date).all()
            print(f"Total records found: {len(all_data)}")
            print(f"All data dates: {[r.date for r in all_data]}")

            if not all_data:
                return []

            # Get data within date range
            period_data = [r for r in all_data 
                          if (not start_date or r.date >= start_date) and 
                             (not end_date or r.date <= end_date)]
            print(f"Records in date range: {len(period_data)}")
            print(f"Period data dates: {[r.date for r in period_data]}")

            # For 24-hour view, we only need the latest two records
            if start_date and (end_date - start_date).days <= 1:
                print("Processing 24-hour view")
                # Get the two most recent records before end_date
                period_data = [r for r in all_data if r.date <= end_date]
                print(f"Records before end_date: {len(period_data)}")
                if len(period_data) >= 2:
                    period_data = period_data[-2:]
                    print(f"Using last two records: {[r.date for r in period_data]}")
                elif len(period_data) == 1:
                    print("Only one record found, duplicating it")
                    period_data = [period_data[0], period_data[0]]
                else:
                    return []

            if len(period_data) < 2:
                print("Insufficient data points")
                return []

            # Get earliest and latest records for period
            earliest_period_record = period_data[0]
            latest_period_record = period_data[-1]
            print(f"\nEarliest record date: {earliest_period_record.date}")
            print(f"Latest record date: {latest_period_record.date}")
            
            partnership_metrics = []
            
            # Process period metrics (calculate changes)
            earliest_received_map = {
                rec['creator']: int(rec.get('subscribers', '0').replace(',', ''))
                for rec in earliest_period_record.recommending_me
                if rec['creator'].lower() != 'convertkit'
            }
            print(f"\nEarliest received map: {earliest_received_map}")
            
            earliest_sent_map = {
                rec['creator']: int(rec.get('subscribers', '0').replace(',', ''))
                for rec in earliest_period_record.my_recommendations
                if rec['creator'].lower() != 'convertkit'
            }
            print(f"Earliest sent map: {earliest_sent_map}")
            
            latest_received_map = {
                rec['creator']: int(rec.get('subscribers', '0').replace(',', ''))
                for rec in latest_period_record.recommending_me
                if rec['creator'].lower() != 'convertkit'
            }
            print(f"Latest received map: {latest_received_map}")
            
            latest_sent_map = {
                rec['creator']: int(rec.get('subscribers', '0').replace(',', ''))
                for rec in latest_period_record.my_recommendations
                if rec['creator'].lower() != 'convertkit'
            }
            print(f"Latest sent map: {latest_sent_map}")

            # Combine all partners
            all_partners = set(list(latest_received_map.keys()) + 
                             list(latest_sent_map.keys()))
            print(f"\nAll partners: {all_partners}")

            # Create metrics for each partner
            for partner in all_partners:
                # Calculate period changes
                period_received = latest_received_map.get(partner, 0) - earliest_received_map.get(partner, 0)
                period_sent = latest_sent_map.get(partner, 0) - earliest_sent_map.get(partner, 0)
                period_balance = period_received - period_sent
                all_time_balance = latest_received_map.get(partner, 0) - latest_sent_map.get(partner, 0)

                print(f"\nPartner: {partner}")
                print(f"Period received: {period_received} ({latest_received_map.get(partner, 0)} - {earliest_received_map.get(partner, 0)})")
                print(f"Period sent: {period_sent} ({latest_sent_map.get(partner, 0)} - {earliest_sent_map.get(partner, 0)})")
                print(f"Period balance: {period_balance}")
                print(f"All-time balance: {all_time_balance}")

                partnership_metrics.append({
                    'partner': partner,
                    'period_received': period_received,
                    'period_sent': period_sent,
                    'period_balance': period_balance,
                    'all_time_balance': all_time_balance
                })

            # Sort by absolute value of period_balance
            partnership_metrics.sort(key=lambda x: abs(x['period_balance']), reverse=True)
            return partnership_metrics

        finally:
            session.close()
    def get_earliest_data_date(self):
        """Get the earliest date in the database"""
        session = self.Session()
        try:
            return session.query(func.min(ReferralData.date)).scalar()
        finally:
            session.close()

    def get_largest_imbalances(self, start_date=None, end_date=None):
        """Get the largest referral imbalances across all accounts"""
        session = self.Session()
        try:
            print("\n=== Starting get_largest_imbalances ===")
            print(f"Start date: {start_date}, End date: {end_date}")
            
            # Get the latest record for each unique account
            subquery = session.query(
                ReferralData.account_name,
                func.max(ReferralData.date).label('max_date')
            ).group_by(ReferralData.account_name).subquery()

            query = session.query(ReferralData).join(
                subquery,
                (ReferralData.account_name == subquery.c.account_name) &
                (ReferralData.date == subquery.c.max_date)
            )

            if start_date:
                query = query.filter(ReferralData.date >= start_date)
            if end_date:
                query = query.filter(ReferralData.date <= end_date)
            
            latest_data = query.all()
            print(f"Found {len(latest_data)} records")
            
            # Process all partnerships to find biggest imbalances
            imbalances = []
            
            for record in latest_data:
                print(f"Processing record for account: {record.account_name}")
                # Process "recommending me" entries
                received_map = {rec['creator']: int(rec.get('subscribers', '0').replace(',', '')) 
                              for rec in record.recommending_me}
                print(f"Received map: {received_map}")
                
                # Process "my recommendations" entries
                sent_map = {rec['creator']: int(rec.get('subscribers', '0').replace(',', '')) 
                           for rec in record.my_recommendations}
                print(f"Sent map: {sent_map}")
                
                # Calculate imbalances
                for partner in set(list(received_map.keys()) + list(sent_map.keys())):
                    received = received_map.get(partner, 0)
                    sent = sent_map.get(partner, 0)
                    imbalance = received - sent
                    
                    # Only add if there's an actual imbalance
                    if imbalance != 0:
                        imbalances.append({
                            'account': record.account_name,
                            'partner': partner,
                            'received': received,
                            'sent': sent,
                            'imbalance': imbalance,
                            'abs_imbalance': abs(imbalance)
                        })
            
            # Sort by absolute imbalance and get top 10
            sorted_imbalances = sorted(imbalances, key=lambda x: x['abs_imbalance'], reverse=True)[:10]
            print(f"Returning {len(sorted_imbalances)} imbalances")
            return sorted_imbalances
        finally:
            session.close()

    def convert_data_format(self, df):
        """Convert DataFrame to database format"""
        results = []
        
        # Group by account_name and date
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
                
                # Add to results
                results.append({
                    'date': datetime.strptime(str(date), '%Y-%m-%d') if len(str(date)) == 10 else datetime.strptime(str(date), '%Y-%m-%d %H:%M:%S'),
                    'account_name': account_name,
                    'recommending_me': recommending_list,
                    'my_recommendations': recommendations_list
                })
        
        return results

    def import_csv(self, csv_path='data/referral_data.csv'):
        """Import data from CSV file into database"""
        # Read CSV file
        df = pd.read_csv(csv_path)
        
        # Convert data to the right format
        data_to_import = self.convert_data_format(df)
        
        # Import each record
        for record in data_to_import:
            self.save_data(
                account_name=record['account_name'],
                recommending_me=record['recommending_me'],
                my_recommendations=record['my_recommendations']
            )
        
        print("Data imported successfully!")

    def create_html_viewer(self):
        """Create HTML viewer for referral analytics"""
        session = self.Session()
        try:
            # Get all unique account names (clients)
            accounts = [r.account_name for r in session.query(ReferralData.account_name).distinct()]
            accounts.sort()
            
            # Create client options HTML
            client_options = ['<option value="all">All Clients</option>']
            client_options.extend([f'<option value="{account}">{account}</option>' for account in accounts])
            
            # Get the largest imbalances
            imbalances = self.get_largest_imbalances()
            
            # Create table rows HTML
            table_rows = []
            for imb in imbalances:
                print(f"Debug - Imbalance data: {imb}")  # Add this debug line
                received = int(imb.get('received', 0))
                sent = int(imb.get('sent', 0))
                imbalance = int(imb.get('imbalance', 0))
                
                row = f'''
                    <tr class="table-row-clickable" 
                        onclick="showTrendsPanel({received}, {sent}, {imbalance}, '{imb['partner']}')">
                        <td>{imb['partner']}</td>
                        <td>{imb.get('account', '')}</td>
                        <td>{received:,}</td>
                        <td>{sent:,}</td>
                        <td>{imbalance:,}</td>
                    </tr>
                '''
                table_rows.append(row)
            
            html_parts = []
            html_parts.append('''
            <!DOCTYPE html>
            <html>
            <head>
                <title>Referral Analytics</title>
                <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
                <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
                <style>
                    .positive { color: green; }
                    .negative { color: red; }
                    .table-container { margin-bottom: 2rem; }
                    .trends-panel {
                        position: fixed;
                        right: -500px;
                        top: 0;
                        width: 500px;
                        height: 100vh;
                        background: white;
                        box-shadow: -2px 0 10px rgba(0,0,0,0.1);
                        transition: right 0.3s ease;
                        z-index: 1050;
                        padding: 20px;
                        overflow-y: auto;
                    }
                    .trends-panel.active { right: 0; }
                    .chart-container { margin-bottom: 20px; }
                    .table-row-clickable:hover {
                        background-color: #f5f5f5;
                        cursor: pointer;
                    }
                </style>
            </head>
            <body>
                <div class="container mt-4">
                    <h2 class="mb-4">Referral Analytics</h2>
                    
                    <!-- Add Client and Date Selectors -->
                    <div class="row mb-4">
                        <div class="col-md-6">
                            <div class="form-group">
                                <label for="clientSelect">Client</label>
                                <select class="form-select" id="clientSelect">
                                    ''' + '\n'.join(client_options) + '''
                                </select>
                            </div>
                        </div>
                        <div class="col-md-6">
                            <div class="form-group">
                                <label for="dateRangeSelect">Date Range</label>
                                <select class="form-select" id="dateRangeSelect">
                                    <option value="1">24 hours</option>
                                    <option value="7">7 days</option>
                                    <option value="14">14 days</option>
                                    <option value="30" selected>30 days</option>
                                    <option value="90">90 days</option>
                                </select>
                            </div>
                        </div>
                    </div>

                    <div id="defaultDashboard">
                        <h3 class="mb-4">Largest Referral Imbalances</h3>
                        <div class="table-responsive">
                            <table class="table table-hover">
                                <thead>
                                    <tr>
                                        <th onclick="sortTable('partner')" style="cursor: pointer;">Partner</th>
                                        <th onclick="sortTable('received')" style="cursor: pointer;">Received</th>
                                        <th onclick="sortTable('sent')" style="cursor: pointer;">Sent</th>
                                        <th onclick="sortTable('balance')" style="cursor: pointer;">Balance</th>
                                        <th onclick="sortTable('balance')" style="cursor: pointer;">All-Time Balance</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ''' + '\n'.join(table_rows) + '''
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>

                <!-- Trends Panel -->
                <div id="trendsPanel" class="trends-panel">
                    <div class="trends-header">
                        <h2>Partnership Trends</h2>
                        <span class="close-btn" onclick="closeTrendsPanel()">×</span>
                    </div>
                    <div class="metrics-container">
                        <div class="metric-box">
                            <h3>Received</h3>
                            <span id="receivedMetric">0</span>
                        </div>
                        <div class="metric-box">
                            <h3>Sent</h3>
                            <span id="sentMetric">0</span>
                        </div>
                        <div class="metric-box">
                            <h3>Current Balance</h3>
                            <span id="balanceMetric">0</span>
                        </div>
                    </div>
                    <div id="trendChart"></div>
                </div>

                <script>
                    let exchangeChart = null;
                    let currentSort = { column: 'balance', direction: 'desc' };
                    let tableData = [];

                    function formatNumber(num) {
                        return new Intl.NumberFormat().format(num);
                    }
                    
                    function showTrendsPanel(received, sent, balance, partner) {
                        console.log('Received:', received, 'Sent:', sent, 'Balance:', balance, 'Partner:', partner);

                        if (!partner) {
                            console.error('Partner is undefined');
                            return;
                        }

                        received = parseInt(received) || 0;
                        sent = parseInt(sent) || 0;
                        balance = parseInt(balance) || 0;

                        document.getElementById('receivedMetric').textContent = received.toLocaleString();
                        document.getElementById('sentMetric').textContent = sent.toLocaleString();
                        document.getElementById('balanceMetric').textContent = balance.toLocaleString();

                        const panel = document.getElementById('trendsPanel');
                        panel.classList.add('active');
                    }

                    function closeTrendsPanel() {
                        document.getElementById('trendsPanel').classList.remove('active');
                    }

                    function updateTrendChart(trendData) {
                        if (exchangeChart) exchangeChart.destroy();
                        
                        const ctx = document.getElementById('exchangeChart').getContext('2d');
                        exchangeChart = new Chart(ctx, {
                            type: 'line',
                            data: {
                                labels: trendData.dates,
                                datasets: [
                                    {
                                        label: 'Received',
                                        data: trendData.received,
                                        borderColor: 'rgb(75, 192, 192)',
                                        tension: 0.1
                                    },
                                    {
                                        label: 'Sent',
                                        data: trendData.sent,
                                        borderColor: 'rgb(255, 99, 132)',
                                        tension: 0.1
                                    },
                                    {
                                        label: 'Balance',
                                        data: trendData.balance,
                                        borderColor: 'rgb(54, 162, 235)',
                                        tension: 0.1
                                    }
                                ]
                            },
                            options: {
                                responsive: true,
                                plugins: {
                                    title: {
                                        display: true,
                                        text: 'Partnership Trend Over Time'
                                    }
                                }
                            }
                        });
                    }

                    function formatBalanceValue(value) {
                        if (isNaN(value)) value = 0;
                        const cls = value >= 0 ? 'positive' : 'negative';
                        return `<span class="${cls}">${value.toLocaleString()}</span>`;
                    }

                    // Add to the script section
                    // Initialize date inputs
                    document.addEventListener('DOMContentLoaded', function() {
                        // Populate client select
                        const clientSelect = document.getElementById('clientSelect');
                        // Get unique clients from your data
                        const clients = [...new Set(imbalances.map(item => item.partner))];
                        clients.forEach(client => {
                            const option = document.createElement('option');
                            option.value = client;
                            option.textContent = client;
                            clientSelect.appendChild(option);
                        });

                        // Add event listeners
                        dateRangeSelect.addEventListener('change', refreshData);
                        clientSelect.addEventListener('change', refreshData);
                    });

                    function refreshData() {
                        const dateRange = document.getElementById('dateRangeSelect').value;
                        const client = document.getElementById('clientSelect').value;
                        
                        console.log('Refreshing data for:', {
                            dateRange,
                            client
                        });
                        // TODO: Add API call to refresh data
                    }

                    // Add these function definitions before your event listeners
                    let imbalances = [];

                    function loadDefaultDashboard() {
                        console.log('Loading default dashboard');
                        const dateRange = document.getElementById('dateRangeSelect').value;
                        let url = 'http://localhost:5001/api/largest-imbalances';
                        
                        if (dateRange !== 'all') {
                            const now = new Date();
                            const startDate = new Date();
                            startDate.setDate(now.getDate() - parseInt(dateRange));
                            url += `?start=${startDate.toISOString().split('T')[0]}&end=${now.toISOString().split('T')[0]}`;
                        }
                        
                        fetch(url)
                            .then(response => response.json())
                            .then(data => {
                                imbalances = data;
                                updateTable(data);
                            })
                            .catch(error => {
                                console.error('Error:', error);
                                const tbody = document.querySelector('table tbody');
                                tbody.innerHTML = `<tr><td colspan="5" class="text-center text-danger">Error loading data: ${error.message}</td></tr>`;
                            });
                    }

                    function sortTable(column) {
                        const direction = currentSort.column === column && currentSort.direction === 'desc' ? 'asc' : 'desc';
                        
                        tableData.sort((a, b) => {
                            let valueA, valueB;
                            
                            switch(column) {
                                case 'partner':
                                    valueA = a.partner || '';
                                    valueB = b.partner || '';
                                    return direction === 'desc' ? valueB.localeCompare(valueA) : valueA.localeCompare(valueB);
                                case 'received':
                                    valueA = a.period_received || a.received || 0;
                                    valueB = b.period_received || b.received || 0;
                                    break;
                                case 'sent':
                                    valueA = a.period_sent || a.sent || 0;
                                    valueB = b.period_sent || b.sent || 0;
                                    break;
                                case 'balance':
                                    valueA = a.period_balance || a.imbalance || 0;
                                    valueB = b.period_balance || b.imbalance || 0;
                                    break;
                            }
                            
                            return direction === 'desc' ? valueA - valueB : valueB - valueA;
                        });
                        
                        currentSort = { column, direction };
                        renderTable();
                    }

                    function updateTableHeaders() {
                        const isDefaultView = document.getElementById('clientSelect').value === 'all';
                        const thead = document.querySelector('table thead tr');
                        
                        thead.innerHTML = isDefaultView ? `
                            <th onclick="sortTable('partner')" style="cursor: pointer;">Partner</th>
                            <th onclick="sortTable('account')" style="cursor: pointer;">Client</th>
                            <th onclick="sortTable('received')" style="cursor: pointer;">Received</th>
                            <th onclick="sortTable('sent')" style="cursor: pointer;">Sent</th>
                            <th onclick="sortTable('balance')" style="cursor: pointer;">Balance</th>
                            <th onclick="sortTable('balance')" style="cursor: pointer;">All-Time Balance</th>
                        ` : `
                            <th onclick="sortTable('partner')" style="cursor: pointer;">Partner</th>
                            <th onclick="sortTable('received')" style="cursor: pointer;">Received</th>
                            <th onclick="sortTable('sent')" style="cursor: pointer;">Sent</th>
                            <th onclick="sortTable('balance')" style="cursor: pointer;">Balance</th>
                            <th onclick="sortTable('balance')" style="cursor: pointer;">All-Time Balance</th>
                        `;
                    }

                    function renderTable() {
                        const tbody = document.querySelector('table tbody');
                        tbody.innerHTML = '';
                        
                        const isDefaultView = document.getElementById('clientSelect').value === 'all';
                        
                        tableData.forEach(item => {
                            const row = document.createElement('tr');
                            row.className = 'table-row-clickable';
                            const balanceClass = (item.period_balance || item.imbalance || 0) >= 0 ? 'positive' : 'negative';
                            
                            const received = item.period_received || item.received || 0;
                            const sent = item.period_sent || item.sent || 0;
                            const balance = item.period_balance || item.imbalance || 0;
                            
                            row.innerHTML = isDefaultView ? `
                                <td>${item.partner}</td>
                                <td>${item.account || ''}</td>
                                <td>${received.toLocaleString()}</td>
                                <td>${sent.toLocaleString()}</td>
                                <td class="${balanceClass}">${balance.toLocaleString()}</td>
                                <td class="${balanceClass}">${balance.toLocaleString()}</td>
                            ` : `
                                <td>${item.partner}</td>
                                <td>${received.toLocaleString()}</td>
                                <td>${sent.toLocaleString()}</td>
                                <td class="${balanceClass}">${balance.toLocaleString()}</td>
                                <td class="${balanceClass}">${balance.toLocaleString()}</td>
                            `;
                            
                            row.onclick = () => showTrendsPanel({
                                received: received,
                                sent: sent,
                                balance: balance
                            });
                            
                            tbody.appendChild(row);
                        });
                    }

                    function updateTable(data) {
                        tableData = data;
                        sortTable('balance'); // Default sort by balance
                    }

                    // Your existing event listener code remains the same
                    document.addEventListener('DOMContentLoaded', function() {
                        // Initial load of default dashboard
                        loadDefaultDashboard();

                        // Add event listeners for selectors
                        document.getElementById('clientSelect').addEventListener('change', function() {
                            updateTableHeaders(); // Update headers when view changes
                            const selectedClient = this.value;
                            console.log('Selected client:', selectedClient);
                            
                            if (selectedClient === 'all') {
                                loadDefaultDashboard();
                            } else {
                                fetch(`http://localhost:5000/api/partnership-metrics?account=${encodeURIComponent(selectedClient)}`)
                                    .then(response => response.json())
                                    .then(data => {
                                        console.log('API Response:', data);
                                        updateTable(data);
                                    })
                                    .catch(error => {
                                        console.error('Error:', error);
                                        const tbody = document.querySelector('table tbody');
                                        tbody.innerHTML = `<tr><td colspan="5" class="text-center text-danger">Error loading data: ${error.message}</td></tr>`;
                                    });
                            }
                        });

                        document.getElementById('dateRangeSelect').addEventListener('change', function() {
                            document.getElementById('clientSelect').dispatchEvent(new Event('change'));
                        });
                    });
                </script>
            </body>
            </html>
            ''')
            
            return '\n'.join(html_parts)
        finally:
            session.close()

    def get_account_trends(self, account_name, start_date, end_date):
        """Get trend data for a specific account over the specified date range"""
        session = self.Session()
        try:
            query = session.query(
                ReferralData.date,
                ReferralData.subscribers,
                ReferralData.conversions,
                ReferralData.earnings,
                func.round(
                    100.0 * ReferralData.conversions / 
                    case([(ReferralData.subscribers == 0, 1)], else_=ReferralData.subscribers),
                    2
                ).label('conversion_rate')
            ).filter(
                ReferralData.account_name == account_name,
                ReferralData.date.between(start_date, end_date)
            ).order_by(ReferralData.date.asc())

            results = query.all()
            
            return {
                'dates': [r.date.strftime('%Y-%m-%d') for r in results],
                'subscribers': [r.subscribers for r in results],
                'conversions': [r.conversions for r in results],
                'earnings': [r.earnings for r in results],
                'conversion_rates': [float(r.conversion_rate or 0) for r in results]
            }
        finally:
            session.close()

    def calculate_growth_metrics(self, account_name, start_date, end_date):
        """Calculate growth metrics comparing start and end of period"""
        session = self.Session()
        try:
            # Get start period metrics (average of first 3 days)
            start_metrics = session.query(
                func.avg(ReferralData.subscribers).label('start_subscribers'),
                func.avg(ReferralData.conversions).label('start_conversions'),
                func.avg(ReferralData.earnings).label('start_earnings')
            ).filter(
                ReferralData.account_name == account_name,
                ReferralData.date.between(start_date, start_date + timedelta(days=2))
            ).first()

            # Get end period metrics (average of last 3 days)
            end_metrics = session.query(
                func.avg(ReferralData.subscribers).label('end_subscribers'),
                func.avg(ReferralData.conversions).label('end_conversions'),
                func.avg(ReferralData.earnings).label('end_earnings')
            ).filter(
                ReferralData.account_name == account_name,
                ReferralData.date.between(end_date - timedelta(days=2), end_date)
            ).first()

            # Calculate growth percentages
            def calculate_growth(start, end):
                if not start or start == 0:
                    return 0
                return round(((end - start) / start) * 100, 2)

            return (
                calculate_growth(start_metrics.start_subscribers, end_metrics.end_subscribers),
                calculate_growth(start_metrics.start_conversions, end_metrics.end_conversions),
                calculate_growth(start_metrics.start_earnings, end_metrics.end_earnings)
            )
        finally:
            session.close()

    def get_trends_summary(self, days, min_earnings=0):
        """Get summary of top growing and declining partnerships"""
        session = self.Session()
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)

            # Subquery to get first and last metrics for each account
            account_metrics = session.query(
                ReferralData.account_name,
                func.min(ReferralData.date).label('first_date'),
                func.max(ReferralData.date).label('last_date'),
                func.avg(ReferralData.earnings).label('avg_earnings')
            ).filter(
                ReferralData.date.between(start_date, end_date)
            ).group_by(ReferralData.account_name).subquery()

            # Get accounts meeting minimum earnings threshold
            qualified_accounts = session.query(
                account_metrics.c.account_name
            ).filter(
                account_metrics.c.avg_earnings >= min_earnings
            ).subquery()

            # Calculate growth rates for qualified accounts
            growth_rates = []
            for account in session.query(qualified_accounts):
                growth = self.calculate_growth_metrics(account[0], start_date, end_date)
                growth_rates.append({
                    'account': account[0],
                    'subscriber_growth': growth[0],
                    'conversion_growth': growth[1],
                    'earnings_growth': growth[2],
                    'overall_growth': sum(growth) / 3  # Average of all growth metrics
                })

            # Sort by overall growth
            growth_rates.sort(key=lambda x: x['overall_growth'], reverse=True)

            return {
                'top_growing': growth_rates[:5],
                'top_declining': growth_rates[-5:][::-1]
            }
        finally:
            session.close()

    def get_worst_performers(self, days=None):
        """Get worst performing partnerships across all clients
        Args:
            days: If provided, only look at data from last X days
        """
        session = self.Session()
        try:
            query = session.query(ReferralData)
            if days:
                start_date = datetime.now() - timedelta(days=days)
                query = query.filter(ReferralData.date >= start_date)
            
            # Get latest records for each account
            latest_records = query.order_by(ReferralData.date.desc()).all()
            
            # Process all partnerships
            all_partnerships = []
            for record in latest_records:
                # Create a map of received subscribers
                received_map = {}
                for rec in record.recommending_me:
                    if rec['creator'].lower() != 'convertkit':
                        received_map[rec['creator']] = int(rec.get('subscribers', '0').replace(',', ''))
                
                # Process sent subscribers and calculate imbalances
                for rec in record.my_recommendations:
                    if rec['creator'].lower() != 'convertkit':
                        partner = rec['creator']
                        sent = int(rec.get('subscribers', '0').replace(',', ''))
                        received = received_map.get(partner, 0)
                        balance = received - sent
                        
                        all_partnerships.append({
                            'partner': partner,
                            'client': record.account_name,
                            'received': received,
                            'sent': sent,
                            'balance': balance
                        })
            
            # Sort by worst performing (most negative balance) first
            all_partnerships.sort(key=lambda x: x['balance'])
            return all_partnerships[:10]  # Return top 10 worst performers
            
        finally:
            session.close()

    def get_partnership_trend(self, account_name, partner_name, start_date=None, end_date=None):
        """Get trend data for a specific partnership over time"""
        session = self.Session()
        try:
            query = session.query(ReferralData).filter(
                ReferralData.account_name == account_name
            ).order_by(ReferralData.date.asc())
            
            if start_date:
                query = query.filter(ReferralData.date >= start_date)
            if end_date:
                query = query.filter(ReferralData.date <= end_date)
            
            records = query.all()
            
            trend_data = {
                'dates': [],
                'received': [],
                'sent': [],
                'balance': []
            }
            
            for record in records:
                received = 0
                for rec in record.recommending_me:
                    if rec['creator'] == partner_name:
                        received = int(rec.get('subscribers', '0').replace(',', ''))
                        break
                
                sent = 0
                for rec in record.my_recommendations:
                    if rec['creator'] == partner_name:
                        sent = int(rec.get('subscribers', '0').replace(',', ''))
                        break
                
                trend_data['dates'].append(record.date.strftime('%Y-%m-%d'))
                trend_data['received'].append(received)
                trend_data['sent'].append(sent)
                trend_data['balance'].append(received - sent)
            
            print(f"Trend data for {partner_name}: {trend_data}")  # Debug print
            return trend_data
            
        finally:
            session.close()