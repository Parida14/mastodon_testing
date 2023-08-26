from typing import List, Dict
import requests
import json


def fetch_mastodon_usernames(base_url: str, access_token: str, limit: int = 100) -> List[str]:
    headers = {'Authorization': f'Bearer {access_token}'}
    params = {'limit': limit}
    usernames = []
    response = requests.get(f'{base_url}/api/v1/timelines/public', headers=headers, params=params)
    if response.status_code == 200:
        posts = json.loads(response.text)
        for post in posts:
            usernames.append(post['account']['username'])
    else:
        print(f"Failed to fetch data: {response.status_code}")
    return usernames


# def fetch_following(base_url: str, access_token: str, user_id: str, limit: int = 10) -> List[str]:
#     headers = {'Authorization': f'Bearer {access_token}'}
#     params = {'limit': limit}
#     usernames = []
#     response = requests.get(f'{base_url}/api/v1/accounts/{user_id}/following', headers=headers, params=params)
#     if response.status_code == 200:
#         following = json.loads(response.text)
#         for account in following:
#             usernames.append(account['username'])
#     else:
#         print(f"Failed to fetch data: {response.status_code}")
#     return usernames


def fetch_mastodon_profiles(base_url: str, access_token: str, usernames: List[str]):
    headers = {'Authorization': f'Bearer {access_token}'}
    profiles = []
    for username in usernames:
        response = requests.get(f'{base_url}/api/v1/accounts/search', headers=headers, params={'q': username})
        if response.status_code == 200:
            profiles.extend(json.loads(response.text))
        else:
            print(f"Failed to fetch profile for {username}: {response.status_code}")
    return profiles