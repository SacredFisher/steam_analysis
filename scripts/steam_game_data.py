"""
steam_game_data.py - Collects game data from SteamSpy API
"""

# Import required libraries
import requests
import json
import time
import csv
import os
import re
import random
from datetime import datetime
# from pathlib import Path  # Unused, removed

def get_steamspy_data(appid=None):
    """
    Fetch game data from SteamSpy API.
    
    Args:
        appid (str, optional): Specific app ID to fetch. If None, fetches all games.
        
    Returns:
        dict: JSON response from SteamSpy API
    """
    base_url = "https://steamspy.com/api.php"
    
    if appid:
        params = {
            'request': 'appdetails',
            'appid': appid
        }
    else:
        params = {
            'request': 'all',  # Get all games
        }
    
    try:
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: Received status code {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching data from SteamSpy: {e}")
        return None

def process_steamspy_data(data):
    """
    Process and clean the data from SteamSpy.
    
    Args:
        data (dict): Raw data from SteamSpy API
        
    Returns:
        list: List of dictionaries containing processed game data
    """
    processed_data = []
    
    for appid, game_data in data.items():
        # Skip hidden games
        if appid == '999999':
            continue
            
        # Parse owners range into min and max values
        owners_range = game_data.get('owners', '0 .. 0')
        try:
            owners_min, owners_max = owners_range.replace(',', '').split(' .. ')
            owners_min = int(owners_min)
            owners_max = int(owners_max)
            owners_estimate = (owners_min + owners_max) // 2  # Midpoint estimate
        except Exception as e:
            owners_min = 0
            owners_max = 0
            owners_estimate = 0
            raise e
        
        # Parse tags into a dictionary with scores
        tags = {}
        for key, value in game_data.items():
            if key.startswith('tags'):
                tag_name = key[4:]  # Remove 'tags' prefix
                if tag_name:
                    tags[tag_name] = value
        
        # Create processed game entry
        processed_game = {
            'appid': appid,
            'name': game_data.get('name', ''),
            'developer': game_data.get('developer', ''),
            'publisher': game_data.get('publisher', ''),
            'owners_min': owners_min,
            'owners_max': owners_max,
            'owners_estimate': owners_estimate,
            'average_playtime_forever': game_data.get('average_forever', 0),
            'average_playtime_2weeks': game_data.get('average_2weeks', 0),
            'median_playtime_forever': game_data.get('median_forever', 0),
            'median_playtime_2weeks': game_data.get('median_2weeks', 0),
            'ccu': game_data.get('ccu', 0),
            'price_cents': game_data.get('price', 0),
            'initial_price_cents': game_data.get('initialprice', 0),
            'discount_percent': game_data.get('discount', 0),
            'languages': game_data.get('languages', ''),
            'genre': game_data.get('genre', ''),
            'tags': json.dumps(tags),
            'positive_reviews': game_data.get('positive', 0),
            'negative_reviews': game_data.get('negative', 0),
            'revenue_estimate': owners_estimate * int(game_data.get('price', 0)) / 100,  # Convert cents to dollars
            'collection_timestamp': datetime.now().isoformat()
        }
        
        processed_data.append(processed_game)
    
    return processed_data

def save_to_csv(data, filename="steam_games_data.csv", append=False, deduplicate_on="appid", debug=False):
    """
    Save processed data to CSV file, with optional append and deduplication.
    Args:
        data (list): List of dictionaries containing game data
        filename (str): Name of the output CSV file
        append (bool): If True, append to existing file
        deduplicate_on (str): Field to deduplicate on (e.g., 'appid')
        debug (bool): If True, print debug logs
    """
    if not data:
        if debug:
            print("No data to save")
        return
    output_dir = "steam_data"
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    fieldnames = list(data[0].keys())
    existing_data = []
    if append and os.path.exists(filepath):
        with open(filepath, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                existing_data.append(row)
    # Combine and deduplicate
    all_data = existing_data + data
    seen = set()
    deduped_data = []
    for row in all_data:
        key = row[deduplicate_on]
        if key not in seen:
            deduped_data.append(row)
            seen.add(key)
    mode = 'w'
    with open(filepath, mode, newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(deduped_data)
    if debug:
        print(f"Data saved to {filepath}. Rows: {len(deduped_data)}")

def fetch_and_store_incremental(filename="steam_games_data.csv", debug=False):
    """
    Fetch latest SteamSpy data and append only new/updated entries to the CSV.
    Args:
        filename (str): Name of the output CSV file
        debug (bool): If True, print debug logs
    Returns:
        list: The combined dataset after update
    """
    data = get_steamspy_data()
    if data is None:
        if debug:
            print("No data fetched from SteamSpy.")
        return []
    processed = process_steamspy_data(data)
    output_dir = "steam_data"
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    # Load existing data if present
    existing_data = []
    existing_ids = set()
    if os.path.exists(filepath):
        with open(filepath, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                existing_data.append(row)
                existing_ids.add(row['appid'])
    # Only keep new/updated entries
    new_entries = [row for row in processed if row['appid'] not in existing_ids]
    if debug:
        print(f"Existing entries: {len(existing_data)}; New entries: {len(new_entries)}")
    # Save combined data
    all_data = existing_data + new_entries
    save_to_csv(all_data, filename=filename, append=False, deduplicate_on='appid', debug=debug)
    return all_data


def calculate_days_on_steam(release_date_str):
    """
    Calculate days a game has been on Steam based on release date string.
    Args:
        release_date_str: Release date string from SteamSpy
    Returns:
        int: Number of days on Steam, or None if unavailable
    """
    today = datetime.now()
    if not release_date_str or release_date_str == 'N/A':
        return None
    date_match = re.search(r'(\w+ \d+, \d{4})', release_date_str)
    if date_match:
        try:
            release_date = datetime.strptime(date_match.group(1), '%b %d, %Y')
            return (today - release_date).days
        except Exception as e:
            raise e
    year_match = re.search(r'(\d{4})', release_date_str)
    if year_match:
        try:
            year = int(year_match.group(1))
            if 2005 <= year <= today.year:
                release_date = datetime(year, 7, 1)
                return (today - release_date).days
        except Exception as e:
            raise e
    if any(x in release_date_str.lower() for x in ['coming', 'soon', 'tba', 'tbd']):
        return None
    return None

def sample_games_by_normalized_playtime(total_sample_size=500, num_quantiles=5):
    """
    Sample games across spectrum using median playtime normalized by time on Steam.
    Args:
        total_sample_size: Total number of games to sample
        num_quantiles: Number of popularity segments to create
    Returns:
        list: List of game information dictionaries
    """
    all_games = get_steamspy_data()
    if not all_games:
        return []
    games_list = []
    for appid, data in all_games.items():
        if data.get('type', '') not in ['game', '', 'dlc']:
            continue
        median_playtime = int(data.get('median_forever', 0))
        release_date_str = data.get('release_date', '')
        days_on_steam = calculate_days_on_steam(release_date_str)
        if median_playtime > 0 and days_on_steam and days_on_steam > 0:
            normalized_playtime = median_playtime / days_on_steam
            games_list.append({
                'appid': appid,
                'name': data.get('name', ''),
                'median_playtime': median_playtime,
                'days_on_steam': days_on_steam,
                'normalized_playtime': normalized_playtime,
                'is_free': (int(data.get('price', 0)) == 0),
                'release_date': release_date_str
            })
    if not games_list:
        print("No games with valid playtime and release date found.")
        return []
    sorted_games = sorted(games_list, key=lambda x: x['normalized_playtime'], reverse=True)
    quantile_size = max(1, len(sorted_games) // num_quantiles)
    sampled_games = []
    for i in range(num_quantiles):
        start_idx = i * quantile_size
        end_idx = (i + 1) * quantile_size if i < num_quantiles - 1 else len(sorted_games)
        quantile_games = sorted_games[start_idx:end_idx]
        if not quantile_games:
            continue
        sample_count = total_sample_size // num_quantiles
        if len(quantile_games) < sample_count:
            sample = quantile_games
        else:
            sample = random.sample(quantile_games, sample_count)
        sampled_games.extend(sample)
    print(f"Sampled {len(sampled_games)} games across {num_quantiles} quantiles by normalized playtime")
    # Fetch full details for each sampled game
    detailed_games = []
    for idx, game in enumerate(sampled_games):
        appid = game['appid']
        print(f"Fetching details for sampled game {idx+1}/{len(sampled_games)} (AppID: {appid})...")
        detailed = get_steamspy_data(appid)
        if detailed:
            processed = process_steamspy_data({appid: detailed})
            if processed:
                detailed_games.extend(processed)
        time.sleep(1)  # Respect SteamSpy API rate limit
    return detailed_games

def main():
    """
    Main entry: Sample 500 games by normalized playtime and save to CSV.
    """
    print("Sampling 500 games by normalized playtime...")
    sampled_games_data = sample_games_by_normalized_playtime(total_sample_size=500, num_quantiles=5)
    save_to_csv(sampled_games_data, filename="steam_games_sampled_norm_playtime.csv")
    print("Done.")

if __name__ == "__main__":
    main()