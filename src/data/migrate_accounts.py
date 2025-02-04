import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from src.data.db_manager import DatabaseManager

def migrate_accounts():
    """Migrate accounts from JSON file to database"""
    print("\nStarting account migration...")
    
    db = DatabaseManager()
    if db.migrate_from_json():
        print("✅ Successfully migrated accounts to database")
    else:
        print("❌ Failed to migrate accounts")

if __name__ == "__main__":
    migrate_accounts() 