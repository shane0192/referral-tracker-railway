import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from src.data.db_manager import DatabaseManager, ReferralData
import pytz

def convert_timestamps_to_pt():
    db = DatabaseManager()
    session = db.Session()
    try:
        # Get all records
        records = session.query(ReferralData).all()
        timezone = pytz.timezone('America/Los_Angeles')
        
        for record in records:
            if record.date.tzinfo is None:  # If timestamp is naive
                # Assume it's UTC and convert to PT
                utc_date = pytz.utc.localize(record.date)
                pt_date = utc_date.astimezone(timezone)
                record.date = pt_date
        
        session.commit()
        print(f"Successfully converted {len(records)} timestamps to PT")
        
    except Exception as e:
        session.rollback()
        print(f"Error converting timestamps: {str(e)}")
    finally:
        session.close()

if __name__ == "__main__":
    convert_timestamps_to_pt() 