import requests
import json

from datetime import datetime, timedelta

# Replace 'your_api_key_here' with your actual API key
api_key = ''

url = "https://api.exorde.io/volume/history"
payload = {
    'startDate': '2024-07-01T00:00:00.000Z',
    'endDate': '2024-08-01T00:00:00.000Z',
    'interval': 60, # 1 day
    'limit': 1000,
    'keywords': 'msft,$msft,microsoft',
    'keywordsCondition': 'or'
}

headers = {
    'X-Exorde-Api-Version': 'v1',
    'Accept': 'application/json',
    'Authorization': f'Bearer {api_key}'
}


# print the query parameters
print(f"Query parameters: {payload}")

all_items = []
page_count = 0
next_page_url = url

while next_page_url:
    response = requests.get(next_page_url, headers=headers, params=payload)

    if response.status_code == 200:
        data = response.json()
        all_items.extend(data['items'])
        page_count += 1
        print(f"Fetched page {page_count}")

        # Check for pagination
        pagination = data.get('pagination', {})
        next_page_url = pagination.get('next', None)
        # Reset payload for subsequent requests
        payload = {}  # Clear payload for next requests if 'next' already includes all necessary parameters
    else:
        print(f"Request failed with status code {response.status_code}: {response.text}")
        break


# Store the aggregated results in a file
# owerwrite
with open('response.json' , 'w') as f:
    f.write(json.dumps(all_items, indent=4))
    print("Data written to response.json")

# Print the number of pages and items found
print(f"Total pages : {page_count}")
print(f"Total items fetched: {len(all_items)}")


# assert if we have data up to the end date
last_item = all_items[-1]
first_item = all_items[0]
first_item_date = datetime.strptime(first_item['endDate'], '%Y-%m-%dT%H:%M:%S.%fZ')
last_item_date = datetime.strptime(last_item['endDate'], '%Y-%m-%dT%H:%M:%S.%fZ')

print(f"first fetched item date: {first_item_date}")
print(f"last fetched item date: {last_item_date}")
