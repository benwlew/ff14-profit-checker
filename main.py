"""TODO:
Unit tests
Frontend
Make recursive for subcrafts
"""

from typing import List, Optional, Dict, Union
import requests
from statistics import median

import polars as pl
import duckdb


import config
from utils import utils
logger = utils.setup_logger(__name__)
import update_db

def split_listings_by_quality(listings: List[dict]) -> tuple[List[dict], List[dict]]:
    """Split listings into HQ and NQ lists.
    
    Args:
        listings: List of market listings
        
    Returns:
        Tuple of (nq_listings, hq_listings)
    """
    nq_listings = [listing["pricePerUnit"] for listing in listings if listing.get('hq') == False]
    hq_listings = [listing["pricePerUnit"] for listing in listings if listing.get('hq') == True]
    return nq_listings, hq_listings

def calculate_market_stats(listings: List[dict]) -> Dict:
    """Calculate market statistics from listings, separating HQ and NQ items.
    
    Args:
        listings: List of market listings
        
    Returns:
        Dictionary containing market statistics for both HQ and NQ items
    """
    if not listings:
        return None
    
    # Split listings by quality
    nq_prices, hq_prices = split_listings_by_quality(listings)

    stats = {}
    
    d = {"nq": nq_prices,
         "hq": hq_prices}
    # Calculate stats
    try:
        for k,v in d.items():
            stats[k] = {
                'medianPrice': median(v),
                'minPrice': min(v)}
    except:
        pass
    return stats


def get_ingredients(item: int) -> Optional[list]:
    """Get item ingredient list from local db

    Args:
        item (int): ID of the item  to check
        
    Returns:
        Optional[dict]: Item crafting ingredient dict, or None if request failsprint(details)
    """
    logger.info(f"Getting item {item} from local db")
    with duckdb.connect(config.DB_NAME) as db:
        try:
            # Query the database and convert to polars DataFrame
            df = db.sql(f"SELECT * FROM main.recipe_price WHERE result_id = {item}").pl()
            
            if df.is_empty():
                logger.warning(f"No recipe found for item {item}")
                return None
            
            # Select only relevant columns
            df = df.select(pl.col("^result.*$|^ingredient.*$"))
            
            # Convert DataFrame to structured dictionary
            item_dict = {}
            for col in df.columns:
                if col.startswith(("result", "ingredient")):
                    prefix, *suffix = col.split("_")
                    item_dict.setdefault(prefix, {})["_".join(suffix)] = df[col][0]
            
            # Remove ingredients with zero amount
            ingredients_to_remove = [
            prefix for prefix in item_dict 
            if prefix.startswith("ingredient") 
            and item_dict[prefix].get("amount", 0) == 0
            ]
            
            for ingredient in ingredients_to_remove:
                item_dict.pop(ingredient)
            
            return item_dict

        except Exception as e:
            logger.error(f"Error processing recipe for item {item}: {e}")
            return None


def fetch_universalis(item_id: int, region: str = "Japan") -> Optional[dict]:
    """Fetch market data from Universalis API for an item and its ingredients.

    Args:
        item_id: ID of the item to check
        region: Server/DC/Region to check prices for
        
    Returns:
        Optional[dict]: Dictionary containing market data for item and ingredients
    """
    try:
        # Get recipe information first
        item_data = get_ingredients(item_id)
        if not item_data:
            logger.warning(f"No recipe found for item {item_id}")
            return None
        
        # Get all item IDs we need to check
        item_ids = []
        for k, v in item_data.items():
            if 'id' in v:
                item_ids.append(str(v['id']))

        ### Change logic to top 100 nq and top 100
        url = f"https://universalis.app/api/v2/{region}/{','.join(item_ids)}"
        parameters = {"listings": 200,
                      "fields": "items.listings.pricePerUnit,items.listings.onMannequin,items.listings.hq,items.nqSaleVelocity,items.hqSaleVelocity"}

        logger.info(f"Fetching market data from: {url}")
        response = requests.get(url, params = parameters)
        response.raise_for_status()
        market_data = response.json()
        
        # Process market data        
        for ingredient, details in item_data.items():           
            item_id = str(details['id'])  # Convert to string for dictionary lookup
            if item_id not in market_data.get('items', {}):
                logger.warning(f"No market data found for item {item_id}")
                continue

            item_market = market_data['items'][item_id]
            
            # Get all valid listings (not on mannequins)
            all_listings = [
                listing for listing in item_market.get('listings', [])
                if not listing.get('onMannequin', False)
            ]
            
            if not all_listings:
                logger.info(f"No valid listings found for item {item_id}")
                continue
            
            # Calculate market stats separating HQ and NQ
            market_stats = calculate_market_stats(all_listings)

            if not market_stats:
                logger.warning(f"Could not calculate market stats for item {item_id}")
                continue
            
            
            for i in ["nq","hq"]:
                if i in market_stats:
                    details[i] = {
                        'medianPrice': market_stats[i]['medianPrice'],
                        'minPrice': market_stats[i]['minPrice'],
                        'velocity': item_market.get(f'{i}SaleVelocity'),
                    }


        print(item_data)        
        
        def format_price_data(v: dict) -> dict:
            ### move to market stats?
            name = v.get("name")
            id = v.get("id")
            amount = v.get("amount")
            shop_unit = v.get("shop_price") if v.get("shop_price") else None
            nq_unit = v.get("nq",{}).get(f"minPrice",None)
            hq_unit = v.get("hq",{}).get(f"minPrice",None)
            shop_total = shop_unit * amount if shop_unit else None
            nq_total = nq_unit * amount if nq_unit else None
            hq_total = hq_unit * amount if hq_unit else None
            nq_velocity = v.get("nq",{}).get(f"velocity",None)
            hq_velocity = v.get("hq",{}).get(f"velocity",None)
            
            return{"name": name,
                   "id": id,
                   "amount": amount,
                   "shop_unit": shop_unit,
                   "nq_unit": nq_unit,
                   "hq_unit": hq_unit,
                   "shop_total": shop_total,
                   "nq_total": nq_total,
                   "hq_total": hq_total,
                   "nq_velocity": nq_velocity,
                   "hq_velocity": hq_velocity}
        
        def print_formatted_price_data(item: dict) -> None:
            print(f'Item: {item["name"]}')
            # print(f'ID: {item["id"]}')
            print(f'Number per craft: {item["amount"]}')
            print(f'Shop price: {item["shop_unit"]:,.0f}') if item["shop_unit"]  else None
            print(f'Shop price x Number: {item["shop_total"]:,.0f}') if item["shop_total"] and item["amount"] > 1 else None
            print(f'Marketboard NQ price: {item["nq_unit"]:,.0f}') if item["nq_unit"]  else None
            print(f'Marketboard NQ price x Number: {item["nq_total"]:,.0f}') if item["nq_total"] and item["amount"] > 1 else None
            print(f'Marketboard NQ velocity: {item["nq_velocity"]:,.0f} items/day') if item["nq_velocity"]  else None
            print(f'Marketboard HQ price: {item["hq_unit"]:,.0f}') if item["hq_unit"]  else None           
            print(f'Marketboard HQ price x Number: {item["hq_total"]:,.0f}') if item["hq_total"] and item["amount"] > 1 else None
            print(f'Marketboard HQ velocity: {item["hq_velocity"]:,.0f} items/day') if item["hq_velocity"]  else None
            
    
        for k, v in item_data.items():        
            if k.startswith('result'):
                item = format_price_data(v)
                print("\n=== Crafted Item ===")
                print_formatted_price_data(item)
                buy_nq_total = item["nq_total"]
                buy_hq_total = item["hq_total"]
                break
        
        nq_craft_cost = 0
        hq_craft_cost = 0
        for k, v in item_data.items():        
            if k.startswith('ingredient'):
                item = format_price_data(v)
                print(f"\n=== {k.title()} ===")
                print_formatted_price_data(item)

                ###TODO: Add source/quality of each item
                nq_craft_cost += min(x for x in [item["shop_total"], item["nq_total"], item["hq_total"]] if x is not None)
                
                if item["hq_total"] is not None:
                    hq_craft_cost += min(x for x in [item["shop_total"], item["hq_total"]] if x is not None and item["hq_total"])
                else:
                    hq_craft_cost += min(x for x in [item["shop_total"], item["nq_total"], item["hq_total"]] if x is not None)
                # print(f"Cumulative Crafting Cost buying NQ mats: {nq_craft_cost:,.0f}")
                # print(f"Cumulative Crafting Cost buying HQ mats: {hq_craft_cost:,.0f}")

        nq_craft_pl = buy_hq_total - nq_craft_cost
        hq_craft_pl = buy_hq_total - hq_craft_cost
        nq_craft_pl_perc =  nq_craft_pl / buy_hq_total
        hq_craft_pl_perc =  hq_craft_pl / buy_hq_total

        print(f"\n=== Profit Analysis ===")
        print(f"Buy completed NQ cost: {buy_nq_total:,.0f}") if buy_nq_total else None
        print(f"Buy completed HQ cost: {buy_hq_total:,.0f}") if buy_hq_total else None
        print(f"Craft HQ from buying NQ items cost: {nq_craft_cost:,.0f}") if nq_craft_cost else None
        print(f"Craft HQ from buying HQ items cost: {hq_craft_cost:,.0f}") if hq_craft_cost else None
        print("")
        print(f"Craft HQ from buying NQ items P/L: {buy_hq_total:,.0f} - {nq_craft_cost:,.0f} = {nq_craft_pl:,.0f} ({nq_craft_pl_perc:,.2%})")
        if nq_craft_pl_perc <= 0:
            print("Warning: Crafting this item will result in a loss!")
        elif nq_craft_pl_perc < 0.2:
            print("Note: Low profit margin (below 20%)!")
                
        print(f"\nCraft HQ from buying HQ items P/L: {buy_hq_total:,.0f} - {hq_craft_cost:,.0f} = {hq_craft_pl:,.0f} ({hq_craft_pl_perc:,.2%})")
        if hq_craft_pl_perc <= 0:
            print("Warning: Crafting this item will result in a loss!")
        elif hq_craft_pl_perc < 0.2:
            print("Note: Low profit margin (below 20%)!")
        return item_data

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching market data: {e}")
        return None
    # except Exception as e:
    #     logger.error(f"Unexpected error in fetch_universalis: {e}")
    #     return None

if __name__ == "__main__":
    if config.OFFLINE_MODE:
        logger.info("Running in offline mode; skipping CSV updates from GitHub repo")
    else:
        update_db.main()
        
    # Example item ID
    item_id = 47185
    
    # Fetch market data
    market_data = fetch_universalis(item_id)