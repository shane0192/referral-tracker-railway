import os
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from config directory
config_dir = Path(__file__).parent.parent.parent / 'config'
load_dotenv(config_dir / '.env')

# ConvertKit credentials and URLs
CONVERTKIT_EMAIL = os.getenv('CONVERTKIT_EMAIL', 'support@paperboystudios.co')
CONVERTKIT_PASSWORD = os.getenv('CONVERTKIT_PASSWORD', '!!Ytpammt4c!')
CONVERTKIT_LOGIN_URL = 'https://app.kit.com/'
CONVERTKIT_DASHBOARD_URL = 'https://app.kit.com/creator-network'

# Database settings - use Railway in production, SQLite in development
DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///data/referral_data.db')

# If using Railway URL, modify it for SQLAlchemy if needed
if DATABASE_URL.startswith('postgres://'):
    DATABASE_URL = DATABASE_URL.replace('postgres://', 'postgresql://', 1)

# Slack settings (for notifications)
SLACK_TOKEN = os.getenv('SLACK_TOKEN')
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL')