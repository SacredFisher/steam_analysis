"""
get_all_data.py - Downloads game data and reviews from Steam
"""

# Import required libraries
import os
import sys
import time
import logging
from datetime import datetime
import steam_game_data
import steam_reviews

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("steam_updates.log", mode='w'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('steam_updater')

def update_game_data():
    """Update game ownership and metadata"""
    logger.info("Starting game data update")
    try:
        # Get all games data
        data = steam_game_data.get_steamspy_data()
        if data:
            processed_data = steam_game_data.process_steamspy_data(data)
            steam_game_data.save_to_csv(processed_data, filename="steam_games_data.csv")
            logger.info(f"Successfully updated data for {len(processed_data)} games")
            return processed_data
        else:
            logger.error("Failed to fetch game data")
            return []
    except Exception as e:
        logger.error(f"Error updating game data: {e}")
        return []

def update_reviews(games):
    """Update reviews for provided games"""
    logger.info(f"Starting review updates for {len(games)} games")
    for i, game in enumerate(games):
        try:
            appid = game['appid']
            name = game['name']
            logger.info(f"Updating reviews for {name} ({i+1}/{len(games)})")
            
            # Get reviews (limited to 20 pages)
            reviews = steam_reviews.fetch_all_reviews(appid, name)
            
            # Save reviews
            if reviews:
                steam_reviews.save_reviews_to_csv(reviews, appid, name)
            
            # Don't hammer the API
            time.sleep(2)
        except Exception as e:
            logger.error(f"Error updating reviews for {name}: {e}")

if __name__ == "__main__":
    logger.info("Starting data download")
    
    # Update game data first
    games = update_game_data()
    
    # Then update reviews for those games
    if games:
        update_reviews(games)
    
    logger.info("Data download completed")