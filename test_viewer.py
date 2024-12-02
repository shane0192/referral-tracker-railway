from src.data.db_manager import DatabaseManager
import webbrowser
import os

def create_viewer():
    try:
        print("Creating HTML viewer...")
        db = DatabaseManager()
        html_content = db.create_html_viewer()
        
        # Write the content to a file
        filepath = 'data/referral_viewer.html'
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"HTML viewer created successfully at {filepath}")
        
        # Open the file in the default browser
        file_url = f"file://{os.path.abspath(filepath)}"
        print(f"Opening {file_url} in browser...")
        webbrowser.open(file_url)
        
    except Exception as e:
        print(f"Error creating viewer: {str(e)}")
        raise

if __name__ == '__main__':
    create_viewer()