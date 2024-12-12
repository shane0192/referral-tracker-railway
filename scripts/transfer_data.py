from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, text, JSON
from sqlalchemy.orm import declarative_base
from datetime import datetime
import pandas as pd
import os
import json

# Helper function to clean numeric strings
def clean_number(value):
    if isinstance(value, str):
        return int(value.replace(',', ''))
    return int(value)

# Get the DATABASE_URL from environment or use a default
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://localhost/referral_tracker')
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

Base = declarative_base()

class ReferralData(Base):
    __tablename__ = 'referral_data'
    
    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    account_name = Column(String)
    recommending_me = Column(JSON)
    my_recommendations = Column(JSON)

def transfer_data():
    print("Connecting to database...")
    engine = create_engine(DATABASE_URL)
    
    print("Dropping and recreating tables...")
    drop_table_sql = """
    DROP TABLE IF EXISTS referral_data;
    CREATE TABLE referral_data (
        id SERIAL PRIMARY KEY,
        date TIMESTAMP,
        account_name VARCHAR,
        recommending_me JSONB,
        my_recommendations JSONB
    );
    """
    
    with engine.connect() as conn:
        conn.execute(text(drop_table_sql))
        conn.commit()

    print("Reading from CSV...")
    df = pd.read_csv('src/data/referral_data.csv')
    
    grouped = df.groupby(['date', 'account_name'])
    
    records = []
    for (date, account), group in grouped:
        # Process recommending_me
        recommending_me = []
        my_recommendations = []
        
        # Handle recommending_me data
        recommending_group = group[group['tab'] == 'recommending_me']
        for _, row in recommending_group.iterrows():
            recommending_me.append({
                'creator': row['creator'],
                'subscribers': clean_number(row['subscribers']),
                'conversion': float(str(row['conversion_rate']).rstrip('%'))
            })
            
        # Handle my_recommendations data
        recommendations_group = group[group['tab'] == 'my_recommendations']
        for _, row in recommendations_group.iterrows():
            my_recommendations.append({
                'creator': row['creator'],
                'subscribers': clean_number(row['subscribers']),
                'conversion': float(str(row['conversion_rate']).rstrip('%'))
            })
        
        record = {
            'date': date,
            'account_name': account,
            'recommending_me': json.dumps(recommending_me),
            'my_recommendations': json.dumps(my_recommendations)
        }
        records.append(record)
    
    print(f"Inserting {len(records)} records...")
    with engine.connect() as conn:
        for record in records:
            conn.execute(
                text("""
                    INSERT INTO referral_data (date, account_name, recommending_me, my_recommendations)
                    VALUES (:date, :account_name, :recommending_me, :my_recommendations)
                """),
                record
            )
        conn.commit()
    
    print("✅ Transfer complete!")

if __name__ == '__main__':
    transfer_data()