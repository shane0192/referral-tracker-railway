from src.data.db_manager import DatabaseManager
from sqlalchemy import text

def test_connection():
    print("\nTesting database connection...")
    try:
        # Initialize database manager
        db = DatabaseManager()
        
        # Get a session
        session = db.Session()
        
        # Try a simple query
        result = session.execute(text('SELECT 1'))
        print("✅ Successfully connected to database!")
        
        # Test table creation
        print("\nTesting table creation...")
        from src.data.db_manager import Base
        Base.metadata.create_all(db.engine)
        print("✅ Successfully created tables!")
        
        return True
        
    except Exception as e:
        print(f"❌ Error connecting to database: {str(e)}")
        return False
        
    finally:
        if 'session' in locals():
            session.close()

if __name__ == "__main__":
    test_connection() 