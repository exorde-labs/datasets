import requests
import json

# Function to fetch data from the Exorde API
def fetch_data(api_url, api_key, start_date, end_date, interval, limit, keywords, keywords_condition):
    """
    Fetch data from the Exorde API.

    Parameters:
    - api_url (str): The API endpoint URL.
    - api_key (str): The API key for authorization.
    - start_date (str): The start date for the data in ISO format.
    - end_date (str): The end date for the data in ISO format.
    - interval (int): The interval for data aggregation.
    - limit (int): The maximum number of records to fetch.
    - keywords (str): Keywords to search for.
    - keywords_condition (str): Condition for keywords ('or', 'and').

    Returns:
    - list: A list of data items fetched from the API.
    """
    payload = {
        'startDate': start_date,
        'endDate': end_date,
        'interval': interval,
        'limit': limit,
        'keywords': keywords,
        'keywordsCondition': keywords_condition
    }
    # This can be modified to include other parameters/filters (e.g., 'source', 'language', etc.)

    headers = {
        'X-Exorde-Api-Version': 'v1',
        'Accept': 'application/json',
        'Authorization': f'Bearer {api_key}'
    }

    all_items = []
    page_count = 0
    next_page_url = api_url

    while next_page_url:
        response = requests.get(next_page_url, headers=headers, params=payload)
        if response.status_code == 200:
            data = response.json()
            all_items.extend(data['items'])
            page_count += 1
            print(f"{api_url} - Fetched page {page_count} - {keywords}")

            # Check for pagination
            pagination = data.get('pagination', {})
            next_page_url = pagination.get('next', None)
            payload = {}  # Clear payload for next requests if 'next' already includes all necessary parameters
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
            break

    return all_items

## Fetch data for multiple sets of keywords
def fetch_data_for_keywords(api_url, api_key, start_date, end_date, interval, limit, keywords_dict, output_format='dict', keywords_condition='or'):
    """
    Fetch data for multiple sets of keywords and return results in a specified format.

    Parameters:
    - api_url (str): The API endpoint URL.
    - api_key (str): The API key for authorization.
    - start_date (str): The start date for the data in ISO format.
    - end_date (str): The end date for the data in ISO format.
    - interval (int): The interval for data aggregation.
    - limit (int): The maximum number of records to fetch.
    - keywords_list (list): A list of lists containing keywords.
    - output_format (str): The format for output ('dict' or 'json').
    - keywords_condition (str): Condition for keywords ('or', 'and').

    Returns:
    - dict or str: A dictionary or JSON string containing the fetched data.
    """
    # assert keywordCondition in ['or', 'and'], "Invalid keywords condition. Must be 'or' or 'and'."
    assert(output_format in ['dict', 'json']), "Invalid output format. Must be 'dict' or 'json'."  # Check for valid output format
    assert(isinstance(keywords_dict,dict)), "Keywords list must be a list of lists."
    assert(keywords_condition in ['or', 'and']), "Invalid keywords condition. Must be 'or' or 'and'."
    # if api_url is a list, then iterate over the list and fetch data for each URL
    if isinstance(api_url, list):
        results = {}
        for idx, url in enumerate(api_url):
            data = fetch_data_for_keywords(url, api_key, start_date, end_date, interval, limit, keywords_dict, output_format, keywords_condition)
            results[url] = data
        return results
    else:
        results = {}
        for (key,keywords) in zip(keywords_dict.keys(), keywords_dict.values()):
            print(f"Fetching data for keywords: {keywords}")
            keywords_str = ','.join(keywords)
            data = fetch_data(api_url, api_key, start_date, end_date, interval, limit, keywords_str, keywords_condition)
            results[key] = data

    if output_format == 'json':
        return json.dumps(results, indent=4)
    return results

if __name__ == "__main__":
    # Example usage    
    api_key = ''  # Replace with your actual API key
    # Set the API URLs for metrics we want to fetch
    api_url = ["https://api.exorde.io/volume/history", "https://api.exorde.io/sentiment/history"]
    ## Set the query parameters
    start_date = '2024-07-01T00:00:00.000Z'
    end_date = '2024-08-01T00:00:00.000Z'
    interval = 60
    limit = 500 # max limit per page is 100

    ## Set the keywords to search for
    keywords_dict = { # keys of the dict are free identifiers
        "bitcoin": ["$btc", "bitcoin"],
        "ethereum": ["$eth", "ethereum"],
        "dogecoin":['$doge', 'dogecoin'],
        "shiba":['$shiba', 'shiba inu'],
        "floki":['$floki', 'floki inu'],
        "pepe": ["$pepe","pepe token"],
        "wif": ["$wif", "wif token"],
        "solana": ["$sol", "solana"],
        "cardano": ["$ada", "cardano"],
        "polkadot": ["$dot", "polkadot"]
    }

    print(f"Fetching {len(api_url)} metrics for {len(keywords_dict)} sets of keywords...")

    # execute the function
    results = fetch_data_for_keywords(api_url, api_key, start_date, end_date, interval, limit, keywords_dict, output_format='dict')

    # store the results in a file
    with open('response.json', 'w') as f:
        f.write(json.dumps(results, indent=4))
        print("Data written to response.json")
        f.write(json.dumps(results, indent=4))
        print("Data written to response.json")
