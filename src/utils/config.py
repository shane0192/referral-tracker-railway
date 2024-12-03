import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ConvertKit credentials and URLs
CONVERTKIT_EMAIL = 'support@paperboystudios.co'
CONVERTKIT_PASSWORD = '!!Ytpammt4s!'
CONVERTKIT_LOGIN_URL = 'https://app.kit.com/'
CONVERTKIT_DASHBOARD_URL = 'https://app.kit.com/creator-network'

# Database settings
DATABASE_URL = 'sqlite:///data/referral_data.db'

# Slack settings (for notifications)
SLACK_TOKEN = os.getenv('SLACK_TOKEN')
SLACK_CHANNEL = os.getenv('SLACK_CHANNEL')