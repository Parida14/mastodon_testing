import requests
import json

def fetch_my_user_id(base_url: str, access_token: str) -> str:
    headers = {'Authorization': f'Bearer {access_token}'}
    response = requests.get(f'{base_url}/api/v1/accounts/verify_credentials', headers=headers)
    if response.status_code == 200:
        user_data = json.loads(response.text)
        return user_data['id']
    else:
        print(f"Failed to fetch user ID: {response.status_code}")
        return None


# Database Parameters
db_params = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'lp_database_testing',
    'user': 'username',
    'password': 'password'
}

# API Parameters
base_url = "https://mastodon.social"
access_token = "Is0UxBl-x9GZoUdsJJ7OYlGNnKQTXpnkO1gXipGinxw"


# Fetch your own user ID
my_user_id = fetch_my_user_id(base_url, access_token)

# Output your user ID
if my_user_id:
    print(f"My user ID is: {my_user_id}")

