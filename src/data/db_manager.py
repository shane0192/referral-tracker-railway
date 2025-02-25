from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, JSON, func, desc, case, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
from ..utils.config import DATABASE_URL
import json
import os
import shutil
import random
import pytz

Base = declarative_base()

class ReferralData(Base):
    __tablename__ = 'referral_data'
    
    id = Column(Integer, primary_key=True)
    date = Column(DateTime, default=datetime.utcnow)
    account_name = Column(String)
    recommending_me = Column(JSON)
    my_recommendations = Column(JSON)

class AllowedAccount(Base):
    __tablename__ = 'allowed_accounts'
    
    id = Column(Integer, primary_key=True)
    account_name = Column(String, unique=True, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class DatabaseManager:
    def __init__(self):
        self.engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)
        self.timezone = pytz.timezone('America/Los_Angeles')
    
    def save_data(self, account_name, recommending_me, my_recommendations):
        """Save referral data to database with PT timezone"""
        session = self.Session()
        try:
            # Convert current time to PT
            pt_now = datetime.now(self.timezone)
            data = ReferralData(
                date=pt_now,
                account_name=account_name,
                recommending_me=recommending_me,
                my_recommendations=my_recommendations
            )
            session.add(data)
            session.commit()
            print(f"Data saved for account: {account_name} at {pt_now}")
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
        """Create HTML viewer with avatars"""
        try:
            # Get the latest data
            data = self.get_latest_data()
            
            # Setup paths
            source_avatar_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                'data',
                'avatars'
            )
            
            # Create destination directory next to the HTML file
            html_dir = os.path.dirname(__file__)
            dest_avatar_dir = os.path.join(html_dir, 'avatars')
            os.makedirs(dest_avatar_dir, exist_ok=True)
            
            # Copy avatar mapping and images
            avatar_mapping = {}
            if os.path.exists(os.path.join(source_avatar_dir, 'avatar_mapping.json')):
                # Load mapping
                with open(os.path.join(source_avatar_dir, 'avatar_mapping.json'), 'r') as f:
                    avatar_mapping = json.load(f)
                
                # Copy each avatar file
                for filename in os.listdir(source_avatar_dir):
                    if filename.endswith('.png'):
                        source_path = os.path.join(source_avatar_dir, filename)
                        dest_path = os.path.join(dest_avatar_dir, filename)
                        shutil.copy2(source_path, dest_path)
                
                print(f"Copied {len(avatar_mapping)} avatars to {dest_avatar_dir}")
            
            # Read the template
            template_path = os.path.join(os.path.dirname(__file__), 'referral_viewer.html')
            with open(template_path, 'r') as f:
                template = f.read()
                
            # Replace placeholders with data
            rows = []
            for row in data:
                partner_name = row['partner_name']
                avatar_file = avatar_mapping.get(partner_name, 'default.png')
                
                row_html = f"""
                    <tr class="border-none hover:bg-gray-50 cursor-pointer">
                        <td class="px-4 py-4 text-sm flex flex-row px-4 pt-3 pb-3 items-center">
                            <img 
                                src="avatars/{avatar_file}" 
                                alt="{partner_name}"
                                class="rounded-full object-cover object-center mr-4"
                                style="display: inline; width: 48px; height: 48px;"
                            >
                            <div class="inline text-sm leading-6 font-semibold">{partner_name}</div>
                        </td>
                        <td class="px-4 py-4 text-sm text-center">{row['received']}</td>
                        <td class="px-4 py-4 text-sm text-center">{row['sent']}</td>
                        <td class="px-4 py-4 text-sm text-center">{row['balance']}</td>
                        <td class="px-4 py-4 text-sm text-right">{row['all_time_balance']}</td>
                    </tr>
                """
                rows.append(row_html)
                
            # Join all rows and insert into template
            table_content = '\n'.join(rows)
            html_content = template.replace('{{ table_content }}', table_content)
            
            # Write the final HTML
            output_path = os.path.join(os.path.dirname(__file__), 'referral_viewer.html')
            with open(output_path, 'w') as f:
                f.write(html_content)
                
            return output_path
            
        except Exception as e:
            print(f"Error creating HTML viewer: {str(e)}")
            raise

    def get_account_trends(self, account_name, start_date, end_date):
        """Get trend data for a specific account over the specified date range"""
        session = self.Session()
        try:
            records = session.query(ReferralData)\
                .filter(ReferralData.account_name == account_name)\
                .filter(ReferralData.date.between(start_date, end_date))\
                .order_by(ReferralData.date.asc())\
                .all()
            
            trend_data = {
                'dates': [],
                'received': [],
                'sent': [],
                'balance': []
            }
            
            for record in records:
                # Calculate total received and sent for this date
                received = sum(int(rec['subscribers']) for rec in record.recommending_me)
                sent = sum(int(rec['subscribers']) for rec in record.my_recommendations)
                
                trend_data['dates'].append(record.date.strftime('%Y-%m-%d'))
                trend_data['received'].append(received)
                trend_data['sent'].append(sent)
                trend_data['balance'].append(received - sent)
            
            return trend_data
            
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

    def renderTableRow(self, row):
        partner_name = row['partner']  # or however you get the partner name
        avatar_path = f"avatars/{avatar_mapping.get(partner_name, 'default.png')}"
        
        return f"""
            <tr>
                <td class="px-4 py-4 text-sm">
                    <div class="flex flex-row items-center">
                        <img 
                            src="{avatar_path}" 
                            alt="{partner_name}"
                            class="rounded-full object-cover object-center mr-4"
                            style="width: 48px; height: 48px;"
                            onerror="this.src='avatars/default.png'"
                        >
                        <div class="text-sm leading-6 font-semibold">{partner_name}</div>
                    </div>
                </td>
                <td>{row['period_received']}</td>
                <td>{row['period_sent']}</td>
                <td class="{'text-success' if row['period_balance'] > 0 else 'text-danger'}">
                    {row['period_balance']}
                </td>
                <td class="{'text-success' if row['all_time_balance'] > 0 else 'text-danger'}">
                    {row['all_time_balance']}
                </td>
            </tr>
        """

    def generate_demo_data(self):
        """Generate fake referral data for demo purposes"""
        demo_partners = [
            "Creator Weekly", "Digital Academy", "Startup Guide", 
            "Tech Insights", "Growth Weekly", "Marketing School",
            "Business Academy", "Content Pro", "Freelance Weekly",
            "Indie Hackers Daily"
        ]
        
        # Generate data for last 30 days
        dates = [
            datetime.now() - timedelta(days=i) 
            for i in range(30)
        ]
        
        demo_data = []
        
        # Create base values with realistic numbers
        base_values = {}
        for partner in demo_partners:
            if partner in ["Creator Weekly", "Digital Academy", "Marketing School"]:
                # Growing partnerships (start smaller)
                base_values[partner] = random.randint(100, 300)
            elif partner in ["Startup Guide", "Growth Weekly"]:
                # Declining partnerships (start medium)
                base_values[partner] = random.randint(500, 800)
            else:
                # Stable partnerships (varied starting points)
                base_values[partner] = random.randint(200, 600)
        
        # Generate daily data with realistic trends
        for date in dates:
            receiving = []
            sending = []
            
            # Create receiving partnerships (4-6 partners)
            for partner in random.sample(demo_partners, random.randint(4, 6)):
                base = base_values[partner]
                
                # Add trend-based variations
                if partner in ["Creator Weekly", "Digital Academy", "Marketing School"]:
                    # Growing trend (small daily increases)
                    daily_change = random.randint(5, 15)
                elif partner in ["Startup Guide", "Growth Weekly"]:
                    # Declining trend (small daily decreases)
                    daily_change = random.randint(-15, -5)
                else:
                    # Stable with tiny variations
                    daily_change = random.randint(-3, 3)
                
                base_values[partner] += daily_change
                
                receiving.append({
                    "creator": partner,
                    "subscribers": str(max(0, base_values[partner])),
                    "conversion_rate": f"{random.uniform(1.5, 4.5):.1f}%"
                })
            
            # Create sending partnerships (3-5 partners)
            for partner in random.sample(demo_partners, random.randint(3, 5)):
                base = base_values[partner]
                sending.append({
                    "creator": partner,
                    "subscribers": str(max(0, base - random.randint(50, 150))),
                    "conversion_rate": f"{random.uniform(1.0, 3.5):.1f}%"
                })
            
            demo_record = ReferralData(
                date=date,
                account_name="Demo Client",
                recommending_me=receiving,
                my_recommendations=sending
            )
            demo_data.append(demo_record)
        
        return sorted(demo_data, key=lambda x: x.date, reverse=True)  # Most recent first

    def delete_entry(self, entry_id):
        """Delete an entry from the database"""
        try:
            query = "DELETE FROM referral_data WHERE id = ?"
            self.cursor.execute(query, (entry_id,))
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Failed to delete entry: {str(e)}")