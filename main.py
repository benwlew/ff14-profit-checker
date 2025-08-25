# import requests
import duckdb
from pprint import pprint
from garlandtools import GarlandTools
import polars as pl
import time
import json
import os

con = duckdb.connect("ffxiv_price.duckdb")
items_export_csv = "items.csv"
item_export_folder = os.path.dirname(os.path.abspath(items_export_csv))
export_frequency = 50
items_columns = {
    'id': 'INTEGER PRIMARY KEY',
    'name': 'VARCHAR',
    'description': 'VARCHAR',
    'attr': 'VARCHAR',
    'attr_hq': 'VARCHAR',
    'category': 'VARCHAR',
    'convertable': 'VARCHAR',
    'craft': 'VARCHAR',
    'delivery': 'VARCHAR',
    'downgrades': 'VARCHAR',
    'dyeable': 'VARCHAR',
    'dyecount': 'VARCHAR',
    'elvl': 'VARCHAR',
    'equip': 'VARCHAR',
    'glamourous': 'VARCHAR',
    'icon': 'VARCHAR',
    'ilvl': 'VARCHAR',
    'jobCategories': 'VARCHAR',
    'jobs': 'VARCHAR',
    'models': 'VARCHAR',
    'patch': 'VARCHAR',
    'patchCategory': 'VARCHAR',
    'price': 'VARCHAR',
    'rarity': 'VARCHAR',
    'repair': 'VARCHAR',
    'repair_item': 'VARCHAR',
    'sell_price': 'VARCHAR',
    'slot': 'VARCHAR',
    'sockets': 'VARCHAR',
    'stackSize': 'VARCHAR',
    'tradeable': 'VARCHAR',
    'ingredients': 'VARCHAR'
}
tables = {'items': {'columns': items_columns, 'df' : 'items_df'}}

def row_count_success_message(destination, table):
    row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"{destination} updated successfully with total {row_count} rows")

def export_to_duckdb(table, df):
    ### Create table if missing, then Upsert
    con.execute(f"""--sql
    CREATE TABLE IF NOT EXISTS {table} ({',\n'.join(f'{col} {type}' for col, type in tables[table]['columns'].items())});
    INSERT OR REPLACE INTO {table} BY NAME (SELECT * FROM df);
""")
    row_count_success_message("DuckDB", table)

def reorder_duckdb_table(table):
    con.execute(f"""--sql
    CREATE TABLE {table}_temp AS SELECT * FROM {table} ORDER BY id;
    CREATE OR REPLACE TABLE {table} ({',\n'.join(f'{col} {type}' for col, type in tables[table]['columns'].items())});
    INSERT INTO {table} SELECT * FROM {table}_temp;
    DROP TABLE {table}_temp;
    """)
    con.sql(f"SELECT * FROM {table}").show()
    row_count_success_message("DuckDB", table)

def export_to_csv(table):
    con.execute(f"""--sql
        COPY {table} TO '{table}.csv' (FORMAT CSV, HEADER)""")
    row_count_success_message("CSV", table)
    # os.startfile(items_export_folder)


def test_garland_fetch(item_id):
    api = GarlandTools()      
    response = api.item(item_id)
    data = response.json()
    print(response.status_code)
    pprint(data)


def cast_lists_to_strings(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns([
        pl.col(col)
        .map_elements(lambda x: str(x) if x is not None else None, return_dtype=pl.String)
        .alias(col)
        for col in df.columns
    ])

def garland_fetch(item_id):
    api = GarlandTools()      
    response = api.item(item_id)
    print(response.status_code)
    
    if response.status_code == 200:
        data = response.json()

        item_json = pl.json_normalize(data["item"])
        lookup_item_df = pl.DataFrame(item_json)

        for col in lookup_item_df.columns:
            if lookup_item_df.schema.get(col) == pl.List:
                string = lookup_item_df[col][0].to_list()
                string = json.dumps(string)
                lookup_item_df = lookup_item_df.with_columns(pl.lit(string).alias(col))
                print(lookup_item_df[col][0])
        
        ### Add ingredients if exist
        try:
            ingredients_string = json.dumps(data["ingredients"])
            lookup_item_df = lookup_item_df.with_columns(pl.lit(ingredients_string).alias("ingredients"))
        except:
            # print("No ingredients found for", item_id, lookup_item_df["name"][0])
            pass
        # lookup_item_df = lookup_item_df.reindex(columns=items_columns,fill_value=None)
        # print(lookup_item_df)
    elif response.status_code == 404:
        lookup_item_df = pl.DataFrame({'id': item_id, 'name': ""}) ### Return blank row but fill id
    else:
        print(f"Failed to fetch data for {item_id}: {response.status_code}")

    lookup_item_df = cast_lists_to_strings(lookup_item_df)
    lookup_item_df = lookup_item_df.select(col for col in items_columns.keys() if col in lookup_item_df.columns)
    return {'response_status_code': response.status_code, 'items_df': lookup_item_df}
    
### update Item DB from GarlandTools
def update_item_db_from_garland():
    ### Initiate list of already registered item IDs and df from duckdb
    while True:
        try:
            registered_item_ids = con.sql("SELECT distinct id FROM items ORDER BY id").pl()
            registered_item_ids = registered_item_ids["id"].to_list()
            # print(registered_item_ids)
            item_db_df = con.sql("SELECT * FROM items ORDER BY id").pl()
            break
        except:
            ### Start new database if it can't be found
            garland_response = garland_fetch(47187) ## TODO Change back to 1 / 47187 is for testing
            export_to_duckdb('items', item_db_df)
            export_to_csv('items')

    # Fetch missing items
    export_batch_countdown = export_frequency
    for item_id in range(1,49200):  ### Max is 49200
        # print(registered_item_ids)
        if item_id in registered_item_ids:
            pass
        else:
            time.sleep(0.5)
            garland_response = garland_fetch(item_id)
            lookup_item_df = garland_response['items_df']
            item_db_df = pl.concat([item_db_df, lookup_item_df],how='diagonal_relaxed')

            if garland_response['response_status_code'] == 200:
                print("Item ID", item_id, ":", garland_response['items_df']["name"][0], "added successfully")
            elif garland_response['response_status_code'] == 404:
                print("Item ID", item_id, "added as blank")
            
            print(item_db_df)
            # print(len(item_db_df))

            export_batch_countdown -= 1
            print("Saving data to disk in",export_batch_countdown,"entries")
            
            if export_batch_countdown <= 0:
                export_to_duckdb('items', item_db_df)
                export_to_csv('items')
                export_batch_countdown = export_frequency

    export_to_duckdb('items', item_db_df)
    export_to_csv('items')

update_item_db_from_garland()
# test_garland_fetch(47187)
# reorder_duckdb_table('items')
    
### Query other API
# params = {'version': None}
# response = requests.get('https://v2.xivapi.com/api/sheet', params=params)
# print(response.text)params = {'key1': 'value1', 'key2': 'value2'}
# response = requests.get('https://example.com', params=params)

### fetch prices from universalis api
### insert item price data into duckdb


