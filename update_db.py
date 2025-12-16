"""Script to update local DuckDB database with latest FFXIV data from xivapi/ffxiv-datamining GitHub repo"""

from typing import List, Optional, Dict, Union
import requests
from pathlib import Path
from datetime import datetime, timezone
import duckdb
import polars as pl
import os

import config
from utils import utils

logger = utils.setup_logger(__name__)
csv_files =["Item.csv", "ItemFood.csv", "ItemLevel.csv", "ItemSearchCategory.csv",
        "ItemSeries.csv", "ItemSortCategory.csv", "ItemUICategory.csv",
        "RecipeNotebookList.csv", "Recipe.csv", "RecipeLevelTable.csv",
        "RecipeLookup.csv", "RecipeNotebookList.csv", "RecipeSubCategory.csv",
        "GilShop.csv", "GilShopInfo.csv", "GilShopItem.csv"]


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

def git_last_updated(file: str) -> Optional[datetime]:
    """Get the last update time of a file from GitHub.
    
    Args:
        file (str): Name of the file to check
        
    Returns:
        Optional[datetime]: The last commit time in UTC, or None if request fails
    """
    url = f"https://api.github.com/repos/xivapi/ffxiv-datamining/commits?path=csv/{file}"
    headers = {"Authorization": f"Bearer {config.GH_TOKEN}"}
    
    try:
        response = requests.get(url, headers=headers)
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
    headers = {"Authorization": f"Bearer {config.GH_TOKEN}"}
    try:
        response = requests.get(url, headers=headers)
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
    """Saves CSV file from GitHub if newer versions exist.
    
    Args:
        files: List of files to check and update
        
    Returns:
        List of files that were updated
    """
    updated_csv = []
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
                updated_csv.append(file)
                logger.info(f"Updated {file}")
            else:
                logger.error(f"Failed to save {file}")
        else:
            logger.info(f"Local {file} is up to date")

    logger.info(f"{len(files) - len(updated_csv)} of {len(files)} files current")
    logger.info(f"{len(updated_csv)} of {len(files)} files updated")
    if updated_csv:
        logger.debug(f"Updated files: {updated_csv}")
    
    return updated_csv

def update_duckdb(updated_files: List[str]) -> None:
    """Process database updates for the updated files.
    
    Args:
        updated_files: List of files that need to be updated in the database
    """
    
    with duckdb.connect(config.DB_NAME) as db:
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
    
        with open("recipe_price.sql", "r") as f:
           query = f.read()
           df = db.sql(query).pl()
           db.execute(fr"CREATE OR REPLACE TABLE main.recipe_price AS SELECT * FROM df")
           logger.info("Created main.recipe_price table")


def main():
    """Main function to update database with latest FFXIV data."""
    tables_to_update = update_csv(csv_files)

    try:
        if tables_to_update:
            update_duckdb(tables_to_update)  # Pass list of files to write to DuckDB
            logger.info("Database update completed successfully")
        else:
            logger.info("No database updates needed")
            
    except Exception as e:
        logger.error(f"Error in main execution: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    # Offline mode check in this module no longer necessary as this is now checked in main.py
    if config.OFFLINE_MODE:
        logger.info("Running in offline mode; skipping CSV updates from GitHub repo")
        tables_to_update = list(csv_files)  # Use locally cached files in offline mode
    else:
        main()

