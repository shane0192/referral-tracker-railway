import pandas as pd
from datetime import datetime

# Data from the successful scrape
data = {
    'date': datetime.now(),
    'account': 'Nathan Barry',
    'recommending_me': [
        {'name': 'Career coaching from a former Google executive', 'subscribers': 436, 'conversion': 15.27},
        {'name': '💎 Glow & Elevate | Rachel Sanders', 'subscribers': 245, 'conversion': 28.28},
        {'name': 'We Want Women to Make More Money.', 'subscribers': 244, 'conversion': 29.94},
        {'name': 'Weekly Wisdom by Graham Mann', 'subscribers': 208, 'conversion': 32.91},
        {'name': 'Salmon Theory, by Rob Estreitinho', 'subscribers': 144, 'conversion': 14.12},
        {'name': 'Smart Recommendations', 'subscribers': 106, 'conversion': 10.89},
        {'name': 'Frenchie Ferenczi', 'subscribers': 76, 'conversion': 18.45},
        {'name': 'Erin McGoff | AdviceWithErin ✨', 'subscribers': 70, 'conversion': 9.15},
        # ... add more entries as needed
    ],
    'my_recommendations': [
        {'name': 'Kieran Drew', 'subscribers': 958, 'conversion': 21.16},
        {'name': 'Tim Ferriss', 'subscribers': 564, 'conversion': 21.44},
        {'name': 'Get UNSTUCK by Pat Flynn', 'subscribers': 311, 'conversion': 19.52},
        {'name': 'David Perell', 'subscribers': 263, 'conversion': 18.13},
        {'name': 'Deliverability Dispatch', 'subscribers': 262, 'conversion': 16.41},
        {'name': 'Every', 'subscribers': 246, 'conversion': 17.94},
        # ... add more entries as needed
    ]
}

# Convert to DataFrame and save
df = pd.DataFrame({
    'date': data['date'],
    'account': data['account'],
    'name': [entry['name'] for entry in data['recommending_me']] + [entry['name'] for entry in data['my_recommendations']],
    'subscribers': [entry['subscribers'] for entry in data['recommending_me']] + [entry['subscribers'] for entry in data['my_recommendations']],
    'conversion': [entry['conversion'] for entry in data['recommending_me']] + [entry['conversion'] for entry in data['my_recommendations']],
    'type': ['recommending_me'] * len(data['recommending_me']) + ['my_recommendations'] * len(data['my_recommendations'])
})

# Create data directory if it doesn't exist
import os
os.makedirs('src/data', exist_ok=True)

# Save to CSV
df.to_csv('src/data/referral_data.csv', index=False)
print("✅ Data saved successfully!")