"""Script to update local DuckDB database with latest FFXIV data from xivapi/ffxiv-datamining GitHub repo"""

from typing import List, Optional, Dict, Union
import requests
from pathlib import Path
from datetime import datetime, timezone
import duckdb
import polars as pl
import os
from dotenv import load_dotenv
from utils import utils

load_dotenv(dotenv_path='./.env')
GH_TOKEN  = os.getenv("GH_TOKEN")

DB_NAME = "ffxiv_price.duckdb"

logger = utils.setup_logger(__name__)
csv_files =["ClassJob.csv", "Item.csv", "GilShopItem.csv", "Recipe.csv", "World.csv", "WorldDCGroupType.csv",
            ]
 
"""
 unused_csv_files ["ItemFood.csv", "ItemLevel.csv", "ItemSearchCategory.csv",
        "ItemSeries.csv", "ItemSortCategory.csv", "ItemUICategory.csv",
        "RecipeNotebookList.csv", "RecipeLevelTable.csv",
        "RecipeLookup.csv", "RecipeNotebookList.csv", "RecipeSubCategory.csv",
        "GilShop.csv", "GilShopInfo.csv", ]
"""

def local_last_updated(file: str) -> Optional[datetime]:

    file_path = Path("csv") / file
    try:
        updated_datetime = datetime.fromtimestamp(file_path.stat().st_mtime, tz=timezone.utc)
        logger.debug(f"Last modified time for {file}: {updated_datetime}")
        return updated_datetime
    except FileNotFoundError:
        logger.info(f"File not found: {file}")
        return None

def git_last_updated(owner:str, repo: str, file: str) -> Optional[datetime]:
    """Get the last update time of a file from GitHub.
    
    Args:
        file (str): Name of the file to check
        
    Returns:
        Optional[datetime]: The last commit time in UTC, or None if request fails
    """
    
    headers = {"Authorization": f"Bearer {GH_TOKEN}"}

    url = f"https://api.github.com/repos/{owner}/{repo}/contents/csv/{file}"
    try:
        response = requests.get(url, headers=headers)
        if not response.status_code == 200:
            logger.warning(f"File {file} not found at {url}")
            return None
    except:
        logger.warning(f"File {file} not found at {url}")
        return None
     
    url = f"https://api.github.com/repos/{owner}/{repo}/commits?path=csv/{file}"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        if not response.json():
            logger.warning(f"No commits found for {file} in {owner}/{repo}")
            return None
        updated_datetime = datetime.fromisoformat(response.json()[0]["commit"]["author"]["date"])
        return updated_datetime
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching latest commit info for {file}: {e}")
        return None

def save_csv(owner: str, repo: str, file: str) -> bool:
    """Save a CSV file from GitHub.
    
    Args:
        file (str): Name of the file to save
        
    Returns:
        bool: True if successful, False otherwise
    """
    url = f"https://github.com/{owner}/{repo}/blob/master/csv/{file}?raw=true"
    headers = {"Authorization": f"Bearer {GH_TOKEN}"}
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        os.makedirs("csv", exist_ok=True)
        with open(fr"csv/{file}", "w", newline='',encoding='utf-8') as f:
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
        if os.getenv("GITHUB_ACTIONS") == "true":
            local_latest = git_last_updated("benwlew", "xivcraftprofit", file)
        else:
            local_latest = local_last_updated(file)
        git_latest = git_last_updated("xivapi", "ffxiv-datamining", file)
        
        logger.debug(f"File: {file} - Local: {local_latest}, GitHub: {git_latest}")
        
        if git_latest is None:
            logger.warning(f"Could not fetch update info for {file}, skipping updates and using local files")
            break
        elif (local_latest is None) or (git_latest > local_latest):
            logger.info(f"Updating {file} from GitHub...")
            if save_csv("xivapi", "ffxiv-datamining", file):
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
        updated_csv = True
    else:
        updated_csv = False

    return updated_csv

def update_duckdb() -> None:  
    with duckdb.connect(DB_NAME) as db:
        for file in csv_files:
            filename = os.path.splitext(file)[0]
            logger.debug(f"Processing {filename} for database update")
            
            df = pl.read_csv(
                fr"csv/{file}", 
                skip_rows=1, 
                skip_rows_after_header=1
            )
            df = df.select(pl.all().name.map(lambda col_name: col_name.replace('{', '_').replace('[', '_').replace('}', '').replace(']', '')))

            db.execute(fr"CREATE SCHEMA IF NOT EXISTS imported")
            db.execute(fr"CREATE OR REPLACE TABLE imported.{filename} AS SELECT * FROM df")
            logger.info(f"Updated imported.{filename} table in database")

        with open("recipe_price.sql", "r") as f:
            query = f.read()
            df = db.sql(query).pl()
            db.execute(fr"CREATE OR REPLACE TABLE main.recipe_price AS SELECT * FROM df")
            logger.info("Created main.recipe_price table")

        with open("world_dc.sql", "r") as f:
            query = f.read()
            df = db.sql(query).pl()
            db.execute(fr"CREATE OR REPLACE TABLE main.world_dc AS SELECT * FROM df")
            logger.info("Created main.world_dc table")

def main():
    """Main function to update database with latest FFXIV data."""
    db_update_required = update_csv(csv_files)

    if db_update_required:
        update_duckdb()  # Pass list of files to write to DuckDB
        logger.info("Database update completed successfully")
    else:
        logger.info("No database updates needed")

if __name__ == "__main__":
    main()

