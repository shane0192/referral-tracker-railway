from referral_tracker.database.db_manager import DatabaseManager
import webbrowser
import os

def view_data():
    """Create and open HTML viewer for referral data"""
    try:
        print("Creating HTML viewer...")
        db = DatabaseManager()
        path = db.create_html_viewer()
        
        # Convert to absolute path and format for browser
        abs_path = os.path.abspath(path)
        file_url = f'file://{abs_path}'
        
        print(f"Opening viewer at: {file_url}")
        webbrowser.open(file_url)
        
    except Exception as e:
        print(f"Error viewing data: {str(e)}")

if __name__ == "__main__":
    view_data()