"""
steam_reviews.py - Collects game reviews from Steam API
"""

# Import required libraries
import requests
import json
import pickle
import csv
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

def get_user_reviews(appid, params):
    """
    Fetch user reviews for a specific game from Steam API.
    
    Args:
        appid (str): Steam AppID of the game
        params (dict): Parameters for the API request
        
    Returns:
        dict: JSON response from Steam API
    """
    user_review_url = f'https://store.steampowered.com/appreviews/{appid}'
    
    try:
        req_user_review = requests.get(user_review_url, params=params)
        
        if req_user_review.status_code != 200:
            print(f'Failed to get response. Status code: {req_user_review.status_code}')
            return {"success": 2}
        
        try:
            user_reviews = req_user_review.json()
            return user_reviews
        except:
            print("Failed to parse JSON response")
            return {"success": 2}
            
    except Exception as e:
        print(f"Error fetching reviews: {e}")
        return {"success": 2}

def fetch_all_reviews(appid, game_name=None, language='english', time_range=None):
    """
    Fetch all reviews for a game, with optional time range filtering.
    Limited to 20 pages of reviews.
    
    Args:
        appid (str): Steam AppID of the game
        game_name (str, optional): Name of the game (for file naming)
        language (str): Language of reviews to fetch
        time_range (tuple, optional): (start_time, end_time) as datetime objects
        
    Returns:
        list: List of dictionaries containing review data
    """
    # Set default game name if not provided
    if not game_name:
        game_name = f"App{appid}"
    
    # Set up parameters for API request
    params = {
        'json': 1,
        'language': language,
        'cursor': '*',
        'num_per_page': 100,
        'filter': 'recent'
    }
    
    # Set up time range filtering if provided
    if time_range:
        start_time, end_time = time_range
        print(f"Collecting reviews from {start_time} to {end_time}")
    else:
        start_time, end_time = None, None
        print("Collecting all available reviews")
    
    # Track time filtering state
    passed_start_time = False if start_time else True
    passed_end_time = False if end_time else True
    
    # List to store collected reviews
    selected_reviews = []
    page_count = 0
    max_pages = 20  # Limit to 20 pages
    
    print(f"Fetching reviews for {game_name} (AppID: {appid})...")
    
    while page_count < max_pages:  # Add page limit check
        # Fetch reviews for current page
        reviews_response = get_user_reviews(appid, params)
        page_count += 1
        
        # Check for API success
        if reviews_response["success"] != 1:
            print("API request failed")
            break
            
        # Check if there are any reviews
        if reviews_response["query_summary"]["num_reviews"] == 0:
            print("No reviews found")
            break
        
        # Process reviews from this page
        for review in reviews_response["reviews"]:
            timestamp_created = review['timestamp_created']
            
            # Apply time range filtering if specified
            if not passed_end_time and end_time:
                if timestamp_created > end_time.timestamp():
                    continue
                else:
                    passed_end_time = True
            
            if not passed_start_time and start_time:
                if timestamp_created < start_time.timestamp():
                    passed_start_time = True
                    break
            
            # Extract review data
            my_review_dict = {
                'recommendationid': review['recommendationid'],
                'author_steamid': review['author']['steamid'],
                'playtime_at_review_minutes': review['author'].get('playtime_at_review', 0),
                'playtime_forever_minutes': review['author'].get('playtime_forever', 0),
                'playtime_last_two_weeks_minutes': review['author'].get('playtime_last_two_weeks', 0),
                'last_played': review['author'].get('last_played', 0),
                'review_text': review['review'],
                'timestamp_created': timestamp_created,
                'timestamp_updated': review['timestamp_updated'],
                'voted_up': review['voted_up'],
                'votes_up': review['votes_up'],
                'votes_funny': review['votes_funny'],
                'weighted_vote_score': review['weighted_vote_score'],
                'steam_purchase': review.get('steam_purchase', False),
                'received_for_free': review.get('received_for_free', False),
                'written_during_early_access': review.get('written_during_early_access', False),
            }
            
            selected_reviews.append(my_review_dict)
        
        # Check if we need to stop based on time filtering
        if passed_start_time and start_time:
            print(f"Reached reviews older than start time")
            break
        
        # Try to get cursor for next page
        try:
            cursor = reviews_response['cursor']
        except:
            cursor = ''
        
        # If no cursor or empty cursor, we've reached the end
        if not cursor:
            print("Reached the end of all reviews")
            break
        
        # Update cursor for next page
        params['cursor'] = cursor
        print(f"Moving to page {page_count+1}. Next cursor: {cursor[:20]}...")
        
        # Add a small delay to avoid rate limiting
        time.sleep(0.5)
    
    print(f"Collected {len(selected_reviews)} reviews for {game_name}")
    return selected_reviews

def save_reviews_to_pickle(reviews, appid, game_name=None, time_range=None):
    """
    Save collected reviews to a pickle file.
    
    Args:
        reviews (list): List of review dictionaries
        appid (str): Steam AppID of the game
        game_name (str, optional): Name of the game
        time_range (tuple, optional): (start_time, end_time) as datetime objects
    """
    if not game_name:
        game_name = f"App{appid}"
    
    # Clean game name for filename
    clean_name = ''.join(c if c.isalnum() else '_' for c in game_name)
    
    # Create folder structure
    folder_name = f"{appid}_{clean_name}"
    output_dir = Path("steam_data", folder_name)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create filename with time range if specified
    if time_range:
        start_time, end_time = time_range
        filename = f"{appid}_{clean_name}_reviews_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}.pkl"
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{appid}_{clean_name}_reviews_{timestamp}.pkl"
    
    # Save pickle file
    output_path = output_dir / filename
    with open(output_path, 'wb') as f:
        pickle.dump(reviews, f)
    
    print(f"Reviews saved to {output_path}")

def save_reviews_to_csv(reviews, appid, game_name=None, time_range=None):
    """
    Save collected reviews to a CSV file in a standardized format for database import.
    
    Args:
        reviews (list): List of review dictionaries
        appid (str): Steam AppID of the game
        game_name (str, optional): Name of the game
        time_range (tuple, optional): (start_time, end_time) as datetime objects
    """
    if not reviews:
        print("No reviews to save")
        return
        
    if not game_name:
        game_name = f"App{appid}"
    
    # Create reviews directory if it doesn't exist
    output_dir = Path("steam_data/reviews")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Create filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"reviews_{appid}_{timestamp}.csv"
    
    # Prepare CSV file
    output_path = output_dir / filename
    
    # Add game_id to each review for easy joining
    for review in reviews:
        review['game_id'] = appid
        review['game_name'] = game_name
        review['collection_timestamp'] = datetime.now().isoformat()
    
    # Define fieldnames in a specific order for consistency
    fieldnames = [
        'game_id',           # For joining with game data
        'game_name',         # For reference
        'recommendationid',  # Unique review ID
        'author_steamid',    # Reviewer ID
        'playtime_at_review_minutes',
        'playtime_forever_minutes',
        'playtime_last_two_weeks_minutes',
        'last_played',
        'review_text',
        'timestamp_created',
        'timestamp_updated',
        'voted_up',
        'votes_up',
        'votes_funny',
        'weighted_vote_score',
        'steam_purchase',
        'received_for_free',
        'written_during_early_access',
        'collection_timestamp'  # When we collected this review
    ]
    
    # Write to CSV
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(reviews)
    
    print(f"Saved {len(reviews)} reviews to {output_path}")

def fetch_recent_reviews(appid, game_name=None, language='english', years_back=5):
    """
    Fetch reviews from the last X years
    
    Args:
        appid (str): Steam AppID of the game
        game_name (str, optional): Name of the game (for file naming)
        language (str): Language of reviews to fetch
        years_back (int): Number of years of reviews to fetch
        
    Returns:
        list: List of dictionaries containing review data
    """
    end_time = datetime.now()
    start_time = end_time - timedelta(days=365 * years_back)
    
    print(f"Collecting reviews from {start_time.strftime('%Y-%m-%d')} to present")
    
    # Use existing fetch_all_reviews function with time range
    return fetch_all_reviews(appid, game_name, language, (start_time, end_time))

def main():
    print("Steam Reviews Collector")
    print("----------------------")
    print("1. Fetch reviews for a specific game")
    print("2. Fetch reviews within a time range")
    
    choice = input("Enter your choice (1-2): ")
    
    if choice == '1':
        appid = input("Enter AppID of the game: ")
        game_name = input("Enter game name (optional): ")
        language = input("Enter language (default: english): ") or 'english'
        
        reviews = fetch_all_reviews(appid, game_name, language)
        
        if reviews:
            # Save as both pickle and CSV
            save_reviews_to_pickle(reviews, appid, game_name)
            save_reviews_to_csv(reviews, appid, game_name)
    
    elif choice == '2':
        appid = input("Enter AppID of the game: ")
        game_name = input("Enter game name (optional): ")
        language = input("Enter language (default: english): ") or 'english'
        
        print("Enter time range:")
        start_year = int(input("Start year (e.g., 2023): "))
        start_month = int(input("Start month (1-12): "))
        start_day = int(input("Start day (1-31): "))
        
        end_year = int(input("End year (e.g., 2024): "))
        end_month = int(input("End month (1-12): "))
        end_day = int(input("End day (1-31): "))
        
        start_time = datetime(start_year, start_month, start_day, 0, 0, 0)
        end_time = datetime(end_year, end_month, end_day, 23, 59, 59)
        
        time_range = (start_time, end_time)
        reviews = fetch_all_reviews(appid, game_name, language, time_range)
        
        if reviews:
            # Save as both pickle and CSV
            save_reviews_to_pickle(reviews, appid, game_name, time_range)
            save_reviews_to_csv(reviews, appid, game_name, time_range)
    
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()