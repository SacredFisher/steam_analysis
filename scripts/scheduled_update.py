"""
scheduled_update.py - Scheduled updates for game data and reviews
"""


# Import required libraries
import os
import sys
import time
import logging
from datetime import datetime, timedelta
import glob
import steam_game_data
import steam_reviews

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("steam_updates.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('steam_updater')

def cleanup_old_data(years_to_keep=5, debug=False):
    """
    Remove data files older than specified years
    
    Args:
        years_to_keep (int): Number of years of data to keep
        debug (bool): If True, print debug logs
    """
    logger.info(f"Cleaning up data older than {years_to_keep} years")
    cutoff_date = datetime.now() - timedelta(days=365 * years_to_keep)
    # Clean up game data files (only timestamped, not the main CSV)
    game_files = glob.glob("steam_data/steam_games_*.csv")
    for file_path in game_files:
        try:
            filename = os.path.basename(file_path)
            # Only remove files with timestamp in filename
            parts = filename.split('_')
            if len(parts) >= 4 and parts[2].isdigit():
                date_str = parts[2:4]  # Get YYYYMMDD and HHMMSS parts
                file_date = datetime.strptime(f"{date_str[0]}_{date_str[1].split('.')[0]}", "%Y%m%d_%H%M%S")
                if file_date < cutoff_date:
                    os.remove(file_path)
                    logger.info(f"Removed old game data file: {filename}")
                    if debug:
                        logger.debug(f"Removed old game data file: {filename}")
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
    # Clean up review files
    review_dirs = [d for d in glob.glob("steam_data/*_*") if os.path.isdir(d)]
    for review_dir in review_dirs:
        try:
            review_files = glob.glob(os.path.join(review_dir, "*.csv"))
            for file_path in review_files:
                filename = os.path.basename(file_path)
                date_parts = filename.split('_')[-2:]  # Get last two parts
                try:
                    file_date = datetime.strptime(f"{date_parts[0]}_{date_parts[1].split('.')[0]}", "%Y%m%d_%H%M%S")
                    if file_date < cutoff_date:
                        os.remove(file_path)
                        logger.info(f"Removed old review file: {filename}")
                        if debug:
                            logger.debug(f"Removed old review file: {filename}")
                except Exception:
                    continue
            # Remove empty directories
            if not os.listdir(review_dir):
                os.rmdir(review_dir)
                logger.info(f"Removed empty directory: {review_dir}")
                if debug:
                    logger.debug(f"Removed empty directory: {review_dir}")
        except Exception as e:
            logger.error(f"Error processing directory {review_dir}: {e}")

def update_game_data(debug=False):
    """Update game ownership and metadata"""
    logger.info("Starting game data update")
    try:
        data = steam_game_data.fetch_and_store_incremental(debug=debug)
        logger.info(f"Successfully updated data for {len(data)} games")
        if debug:
            logger.debug(f"Updated data for {len(data)} games")
        return data
    except Exception as e:
        logger.error(f"Error updating game data: {e}")

def update_reviews(games, years_back=5):
    """Update reviews for provided games"""
    logger.info(f"Starting review updates for {len(games)} games")
    for i, game in enumerate(games):
        try:
            appid = game['appid']
            name = game['name']
            logger.info(f"Updating reviews for {name} ({i+1}/{len(games)})")
            
            # Get reviews from last X years
            reviews = steam_reviews.fetch_recent_reviews(
                appid, name, years_back=years_back
            )
            
            # Save reviews
            if reviews:
                steam_reviews.save_reviews_to_csv(reviews, appid, name)
            
            # Don't hammer the API
            time.sleep(5)
        except Exception as e:
            logger.error(f"Error updating reviews for {name}: {e}")

if __name__ == "__main__":
    logger.info("Starting scheduled update")
    
    # Update game data first
    games = update_game_data()
    
    # Then update reviews for those games
    update_reviews(games)
    
    # Clean up old data
    cleanup_old_data()
    
    logger.info("Scheduled update completed")