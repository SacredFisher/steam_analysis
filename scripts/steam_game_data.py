"""
steam_game_data.py - Collects game data from SteamSpy API
"""

# Import required libraries
import requests
import json
import time
import csv
import os
from datetime import datetime
from pathlib import Path

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
        except:
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

def save_to_csv(data, filename="steam_games_data.csv"):
    """
    Save processed data to CSV file.
    
    Args:
        data (list): List of dictionaries containing game data
        filename (str): Name of the output CSV file
    """
    if not data:
        print("No data to save")
        return
        
    output_dir = "steam_data"
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    
    fieldnames = data[0].keys()
    
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Data saved to {filepath}")

def fetch_top_games(count=100):
    """
    Fetch data for top games by player count.
    
    Args:
        count (int): Number of top games to fetch
        
    Returns:
        list: Processed data for top games
    """
    # First get the list of all games to find top ones by player count
    print("Fetching list of all games...")
    all_games = get_steamspy_data()
    
    if not all_games:
        return []
    
    # Sort games by current player count (CCU)
    sorted_games = sorted(all_games.items(), 
                          key=lambda x: int(x[1].get('ccu', 0)), 
                          reverse=True)
    
    # Take top N games
    top_games = sorted_games[:count]
    
    # Now fetch detailed data for each top game
    processed_games = []
    for i, (appid, _) in enumerate(top_games):
        print(f"Fetching data for game {i+1}/{count} (AppID: {appid})...")
        game_data = get_steamspy_data(appid)
        
        if game_data:
            processed_game = process_steamspy_data({appid: game_data})
            if processed_game:
                processed_games.extend(processed_game)
        
        # Sleep to avoid hitting rate limits
        time.sleep(1)
    
    return processed_games

def fetch_and_store_incremental(store_path="steam_data/last_update.json"):
    """
    Store timestamp of last update and only fetch newer data next time
    
    Args:
        store_path (str): Path to store the last update timestamp
        
    Returns:
        list: List of dictionaries containing game data
    """
    # Load last update time
    last_update = datetime.now()
    if os.path.exists(store_path):
        with open(store_path, 'r') as f:
            try:
                data = json.load(f)
                last_update = datetime.fromisoformat(data.get('last_update', datetime.now().isoformat()))
            except:
                pass
    
    # Fetch new data
    print(f"Last update was at {last_update}")
    print("Fetching new game data...")
    data = fetch_top_games(100)  # Fetch top 100 games
    
    # Store current time as last update
    os.makedirs(os.path.dirname(store_path), exist_ok=True)
    with open(store_path, 'w') as f:
        json.dump({'last_update': datetime.now().isoformat()}, f)
    
    return data

def main():
    print("Steam Game Data Collector")
    print("------------------------")
    print("1. Fetch all games (slow)")
    print("2. Fetch top 100 games by player count")
    print("3. Fetch specific game by AppID")
    
    choice = input("Enter your choice (1-3): ")
    
    if choice == '1':
        print("Fetching data for all games...")
        data = get_steamspy_data()
        if data:
            processed_data = process_steamspy_data(data)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_to_csv(processed_data, f"steam_all_games_{timestamp}.csv")
    
    elif choice == '2':
        num_games = int(input("Enter number of top games to fetch: "))
        processed_data = fetch_top_games(num_games)
        if processed_data:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_to_csv(processed_data, f"steam_top_{num_games}_games_{timestamp}.csv")
    
    elif choice == '3':
        appid = input("Enter AppID: ")
        print(f"Fetching data for game with AppID {appid}...")
        data = get_steamspy_data(appid)
        if data:
            processed_data = process_steamspy_data({appid: data})
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_to_csv(processed_data, f"steam_game_{appid}_{timestamp}.csv")
    
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()