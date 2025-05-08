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

def get_steamspy_data(appid=None, max_retries=5, retry_delay=100):
    """
    Fetch game data from SteamSpy API.
    
    Args:
        appid (str, optional): Specific app ID to fetch. If None, fetches all games.
        max_retries (int): Maximum number of retries for failed requests
        retry_delay (int): Delay in seconds between retries
        
    Returns:
        dict: JSON response from SteamSpy API
    """
    base_url = "https://steamspy.com/api.php"
    
    if appid:
        params = {
            'request': 'appdetails',
            'appid': appid
        }
        for attempt in range(max_retries):
            try:
                response = requests.get(base_url, params=params)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 500:
                    print(f"Server error (500) on attempt {attempt + 1}/{max_retries}. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    continue
                else:
                    print(f"Error: Received status code {response.status_code}")
                    return None
            except Exception as e:
                print(f"Error fetching data from SteamSpy: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                continue
        return None
    else:
        # Fetch all games with pagination
        all_games = {}
        progress_file = "steam_data/fetch_progress.json"
        os.makedirs("steam_data", exist_ok=True)
        
        # Load progress if exists
        if os.path.exists(progress_file):
            try:
                with open(progress_file, 'r') as f:
                    progress = json.load(f)
                    page = progress.get('last_page', 0) + 1  # Start from next page
                    all_games = progress.get('games', {})
                    print(f"Resuming from page {page} with {len(all_games)} games already fetched")
            except Exception as e:
                print(f"Error loading progress: {e}")
                page = 0
        else:
            page = 0
        
        while True:
            page_success = False
            for attempt in range(max_retries):
                try:
                    # Add 60 second delay between requests as per API requirements
                    if page > 0:
                        time.sleep(60)
                        
                    params = {
                        'request': 'all',
                        'page': page
                    }
                    
                    print(f"Fetching page {page}...")
                    response = requests.get(base_url, params=params)
                    
                    if response.status_code == 200:
                        games = response.json()
                        
                        # If we get an empty response or no games, we've reached the end
                        if not games:
                            print("Reached end of data")
                            return all_games
                            
                        # Add games from this page to our collection
                        all_games.update(games)
                        print(f"Got {len(games)} games from page {page}")
                        
                        # Save progress after each successful page
                        try:
                            with open(progress_file, 'w') as f:
                                json.dump({
                                    'last_page': page,
                                    'games': all_games
                                }, f)
                            print(f"Progress saved: page {page}, total games: {len(all_games)}")
                        except Exception as e:
                            print(f"Error saving progress: {e}")
                        
                        page += 1
                        page_success = True
                        break  # Success, move to next page
                        
                    elif response.status_code == 500:
                        print(f"Server error (500) on attempt {attempt + 1}/{max_retries}. Retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        continue
                    else:
                        print(f"Error: Received status code {response.status_code}")
                        return all_games
                        
                except Exception as e:
                    print(f"Error fetching page {page}: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                    continue
            
            # If we've exhausted all retries for this page, move to next page
            if not page_success:
                print(f"Failed to fetch page {page} after {max_retries} attempts. Moving to next page.")
                page += 1
                continue

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
        try:
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
            except (ValueError, AttributeError):
                owners_min = 0
                owners_max = 0
                owners_estimate = 0
            
            # Parse tags into a dictionary with scores
            tags = {}
            for key, value in game_data.items():
                if key.startswith('tags'):
                    tag_name = key[4:]  # Remove 'tags' prefix
                    if tag_name:
                        tags[tag_name] = value
            
            # Safely convert numeric values with defaults
            def safe_int(value, default=0):
                if value is None:
                    return default
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return default
            
            # Create processed game entry
            processed_game = {
                'appid': appid,
                'name': game_data.get('name', ''),
                'developer': game_data.get('developer', ''),
                'publisher': game_data.get('publisher', ''),
                'owners_min': owners_min,
                'owners_max': owners_max,
                'owners_estimate': owners_estimate,
                'average_playtime_forever': safe_int(game_data.get('average_forever')),
                'average_playtime_2weeks': safe_int(game_data.get('average_2weeks')),
                'median_playtime_forever': safe_int(game_data.get('median_forever')),
                'median_playtime_2weeks': safe_int(game_data.get('median_2weeks')),
                'ccu': safe_int(game_data.get('ccu')),
                'price_cents': safe_int(game_data.get('price')),
                'initial_price_cents': safe_int(game_data.get('initialprice')),
                'discount_percent': safe_int(game_data.get('discount')),
                'languages': game_data.get('languages', ''),
                'genre': game_data.get('genre', ''),
                'tags': json.dumps(tags),
                'positive_reviews': safe_int(game_data.get('positive')),
                'negative_reviews': safe_int(game_data.get('negative')),
                'revenue_estimate': owners_estimate * safe_int(game_data.get('price')) / 100,  # Convert cents to dollars
                'collection_timestamp': datetime.now().isoformat()
            }
            
            processed_data.append(processed_game)
            
        except Exception as e:
            print(f"Error processing game {appid}: {e}")
            continue
    
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

def sample_games_by_normalized_playtime(games_per_quintile=1000):
    """
    Sample games across spectrum using median playtime normalized by time on Steam.
    
    Args:
        games_per_quintile: Number of games to sample from each quintile
        
    Returns:
        list: List of game information dictionaries
    """
    # Get all games from SteamSpy
    all_games = get_steamspy_data()
    if not all_games:
        return []
    
    print(f"Retrieved {len(all_games)} games from SteamSpy API")
    
    # Extract games with necessary data
    games_list = []
    for appid, data in all_games.items():
        # Skip non-game apps
        if data.get('type', '') not in ['game', '', 'dlc']:
            continue
            
        # Get median playtime (in minutes)
        median_playtime = int(data.get('median_forever', 0))
        
        # Get release date and calculate days on Steam
        release_date_str = data.get('release_date', '')
        days_on_steam = calculate_days_on_steam(release_date_str)
        
        # Only include games with playtime data and valid release dates
        # Minimum 30 days on Steam to avoid very new games with unreliable data
        if median_playtime > 0 and days_on_steam and days_on_steam > 30:
            # Calculate normalized playtime (minutes per day on Steam)
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
    
    print(f"Found {len(games_list)} games with valid playtime and release date data")
    
    # Sort by normalized playtime
    sorted_games = sorted(games_list, key=lambda x: x['normalized_playtime'], reverse=True)
    
    # Determine quintile boundaries
    total_games = len(sorted_games)
    quintile_size = total_games // 5
    
    # Sample from each quintile
    sampled_games = []
    for i in range(5):
        start_idx = i * quintile_size
        end_idx = (i + 1) * quintile_size if i < 4 else total_games
        
        # Get games in this quintile
        quintile_games = sorted_games[start_idx:end_idx]
        
        # Sample from this quintile
        if len(quintile_games) <= games_per_quintile:
            # Take all games if there aren't enough
            quintile_sample = quintile_games
        else:
            # Random sampling without replacement
            quintile_sample = random.sample(quintile_games, games_per_quintile)
        
        # Track quintile number for each game
        for game in quintile_sample:
            game['quintile'] = i + 1
        
        sampled_games.extend(quintile_sample)
    
    # Report on the sampling
    print(f"Sampled {len(sampled_games)} games across 5 quintiles of normalized playtime")
    for i in range(5):
        count = sum(1 for game in sampled_games if game['quintile'] == i + 1)
        print(f"  Quintile {i+1}: {count} games")
    
    return sampled_games

def main():
    """
    Main entry: Fetch all games from SteamSpy and save to CSV.
    """
    print("Fetching all games from SteamSpy...")
    data = get_steamspy_data()
    if data:
        processed_data = process_steamspy_data(data)
        save_to_csv(processed_data, filename="steam_games_data.csv")
        print(f"Saved data for {len(processed_data)} games")
    else:
        print("Failed to fetch game data")

if __name__ == "__main__":
    main()