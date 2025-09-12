"""Script to update local DuckDB database with latest FFXIV data from xivapi/ffxiv-datamining GitHub repo"""

import duckdb
from typing import List, Optional, Dict, Union
import polars as pl
import os
import requests
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from functools import wraps
import time
import logging
from main import Config, setup_logger



logger = setup_logger(__name__)

# Load environment variables
load_dotenv()



def retry_on_error(max_retries: int = 3, delay: int = 1):
    """Decorator to retry functions on failure with exponential backoff."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        logger.error(f"Failed after {max_retries} attempts: {e}")
                        raise
                    wait_time = delay * (2 ** (retries - 1))
                    logger.warning(f"Attempt {retries} failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
            return None
        return wrapper
    return decorator

def local_last_updated(file: str) -> Optional[datetime]:
    """Get the last update time of a local file.
    
    Args:
        file (str): Name of the file to check
        
    Returns:
        Optional[datetime]: The last modified time in UTC, or None if file not found
    """
    file_path = Path("csv_dump") / file
    try:
        updated_datetime = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
        logger.debug(f"Last modified time for {file}: {updated_datetime}")
        return updated_datetime
    except FileNotFoundError:
        logger.info(f"File not found: {file}")
        return None

@retry_on_error(max_retries=Config.MAX_RETRIES)
def git_last_updated(file: str) -> Optional[datetime]:
    """Get the last update time of a file from GitHub.
    
    Args:
        file (str): Name of the file to check
        
    Returns:
        Optional[datetime]: The last commit time in UTC, or None if request fails
    """
    url = f"https://api.github.com/repos/xivapi/ffxiv-datamining/commits?path=csv/{file}"
    headers = {"Authorization": f"Bearer {os.environ.get("GITHUB_API_KEY")}"}
    
    try:
        response = requests.get(url, headers=headers, timeout=Config.REQUEST_TIMEOUT)
        response.raise_for_status()
        updated_datetime = datetime.fromisoformat(response.json()[0]["commit"]["author"]["date"])
        return updated_datetime
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching latest commit info for {file}: {e}")
        return None

def save_csv(file: str) -> bool:
    """Save a CSV file from GitHub.
    
    Args:
        file (str): Name of the file to save
        
    Returns:
        bool: True if successful, False otherwise
    """
    url = f"https://github.com/xivapi/ffxiv-datamining/blob/master/csv/{file}?raw=true"
    headers = {"Authorization": f"Bearer {Config.GH_KEY}"}
    try:
        response = requests.get(url, headers=headers, timeout=Config.REQUEST_TIMEOUT)
        response.raise_for_status()
        
        os.makedirs("csv_dump", exist_ok=True)
        with open(fr"csv_dump\{file}", "w", newline='',encoding='utf-8') as f:
            f.write(response.text)
            
        logger.info(f"Successfully downloaded {file}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Error downloading {file}: {e}")
        return False

def update_csv(files: List[str]) -> List[str]:
    """Update CSV files from GitHub if newer versions exist.
    
    Args:
        files: List of files to check and update
        
    Returns:
        List of files that were updated
    """
    updated_tables = []
    for file in files:
        local_latest = local_last_updated(file)
        git_latest = git_last_updated(file)
        
        logger.debug(f"File: {file} - Local: {local_latest}, GitHub: {git_latest}")
        
        if git_latest is None:
            logger.warning(f"Could not fetch update info for {file}, skipping updates and using local files")
            break
        elif (local_latest is None) or (git_latest > local_latest):
            logger.info(f"Updating {file} from GitHub...")
            if save_csv(file):
                updated_tables.append(file)
                logger.info(f"Updated {file}")
            else:
                logger.error(f"Failed to save {file}")
        else:
            logger.info(f"Local {file} is up to date")

    logger.info(f"{len(files) - len(updated_tables)} of {len(files)} files current")
    logger.info(f"{len(updated_tables)} of {len(files)} files updated")
    if updated_tables:
        logger.debug(f"Updated files: {updated_tables}")
    
    return updated_tables

def db_update(updated_files: List[str]) -> None:
    """Process database updates for the updated files.
    
    Args:
        updated_files: List of files that need to be updated in the database
    """
    if not updated_files:
        logger.info("No files to update in database")
        return

    
    with duckdb.connect(Config.DB_NAME) as db:
        for file in updated_files:
            filename = os.path.splitext(file)[0]
            logger.debug(f"Processing {filename} for database update")
            
            df = pl.read_csv(
                fr"csv_dump\{file}", 
                skip_rows=1, 
                skip_rows_after_header=1
            )
            df = df.select(pl.all().name.map(lambda col_name: col_name.replace('{', '_').replace('[', '_').replace('}', '').replace(']', '')))

            db.execute(fr"CREATE SCHEMA IF NOT EXISTS imported")
            db.execute(fr"CREATE OR REPLACE TABLE imported.{filename} AS SELECT * FROM df")
            logger.info(f"Updated {filename} table in database")
    
        with open("create_recipe_price.sql", "r") as f:
           query = f.read()
           df = db.sql(query).pl()
           db.execute(fr"CREATE OR REPLACE TABLE main.recipe_price AS SELECT * FROM df")
           logger.info("Created main.recipe_price table")


def main():
    """Main function to update database with latest FFXIV data."""
    try:
        if Config.OFFLINE_MODE:
            logger.info("Running in offline mode; skipping CSV updates")
            updated = Config.FILES  # Use all files in offline mode
        else:
            updated = update_csv(Config.FILES)
            
        if updated:
            db_update(updated)  # Pass the entire list of files
            logger.info("Database update completed successfully")
        else:
            logger.info("No database updates needed")
            
    except Exception as e:
        logger.error(f"Error in main execution: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()

