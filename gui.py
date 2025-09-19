"""
TODO
- Handle multiple servers
- Host online
"""

from typing import List, Optional, Dict, Union
import duckdb
import requests
import polars as pl
import streamlit as st
# import json

import config


@st.cache_data
def get_all_recipes() -> pl.DataFrame:
    with duckdb.connect(config.DB_NAME) as con:
        query = """SELECT *, CONCAT(result_name, ' (', result_id, ')') as result_text from  recipe_price"""
        df = con.sql(query).pl()
    return df


@st.cache_data
def get_recipe_items(item_id: int) -> pl.DataFrame:
    # Get all item IDs to be passed to API request

    with duckdb.connect(config.DB_NAME) as con:
        query = f"""SELECT * from recipe_price where result_id = '{item_id}' """
        df = con.sql(query).pl()
        ingr_data = df.to_dicts()

    # ingr_data

    lookup_items = {"id": [], "ingredient_of": [], "amount": [], "shop_price": []}
    temp = []
    ### TODO Make recursive
    ## Get IDs where >0; save to a list
    for k, v in ingr_data[0].items():
        if "_" in k and k.split("_")[1] == "id" and v > 0:
            lookup_items["id"].append(str(v))
            if "result" in k:
                lookup_items["ingredient_of"].append(None)
            elif "ingredient" in k:
                lookup_items["ingredient_of"].append(item_id)
            if k.split("_")[0] not in temp:
                temp.append(k.split("_")[0])

    ## Get other details of items where IDs >0
    for k, v in ingr_data[0].items():
        for item in temp:
            if "_" in k and k.split("_")[0] == item:
                if "amount" in k:
                    lookup_items["amount"].append(v)
                if "shop_price" in k:
                    lookup_items["shop_price"].append(v)

    # lookup_items
    return pl.from_dict(lookup_items)


@st.cache_data
def price_lookup(lookup_items_df: pl.DataFrame, region: str = "Japan") -> pl.DataFrame:
    lookup_item_ids = [x for x in lookup_items_df["id"]]
    url = f"https://universalis.app/api/v2/{region}/{','.join(lookup_item_ids)}"

    # GET from Universalis twice per item; once each for NQ/HQ
    raw_market_data = {}
    for hq in [False, True]:
        parameters = {
            "hq": hq,
            "listings": 100,
            "fields": "items.nqSaleVelocity,items.hqSaleVelocity,items.listings.pricePerUnit,items.listings.onMannequin,listings.worldName",
        }
        response = requests.get(url, params=parameters)
        response.raise_for_status()
        if hq:
            raw_market_data["hq"] = response.json()
        else:
            raw_market_data["nq"] = response.json()

    # Calculate price listing, but skip mannequin listings
    market_data = {
        "id": [],
        "nq_price": [],
        "hq_price": [],
        "nq_velocity": [],
        "hq_velocity": [],
    }
    for quality in raw_market_data:
        for item in raw_market_data[quality]["items"]:
            if item not in market_data["id"]:
                market_data["id"].append(item)
                market_data["nq_velocity"].append(
                    int(raw_market_data[quality]["items"][item]["nqSaleVelocity"])
                )
                market_data["hq_velocity"].append(
                    int(raw_market_data[quality]["items"][item]["hqSaleVelocity"])
                )

            min_listing = float("inf")
            for listing in raw_market_data[quality]["items"][item]["listings"]:
                if not listing["onMannequin"]:
                    min_listing = min(min_listing, listing.get("pricePerUnit"))
            if min_listing == float("inf"):
                min_listing = None

            if quality == "nq":
                market_data["nq_price"].append(min_listing)
            elif quality == "hq":
                market_data["hq_price"].append(min_listing)

    market_data = pl.from_dict(market_data)
    return market_data
    ### TODO LEFT JOIN market_data onto table

    ### TODO Add multiple regions recursive
    ### TODO Make recursive
    ### TODO Add item images?
    ### TODO Add item source, e.g. currency if vendor


def join_dfs(lookup_items_df: pl.DataFrame, prices_df: pl.DataFrame) -> pl.DataFrame:
    combined_df = lookup_items_df.join(
        prices_df, on="id", maintain_order="left"
    ).with_row_index()

    with duckdb.connect(config.DB_NAME) as con:
        query = f"""--sql
        SELECT combined_df.*, name, icon, canbehq from combined_df left join imported.item on combined_df.id = item."#" order by index
        """
        combined_df = con.sql(query).pl()

    return combined_df


def print_result(df: pl.DataFrame):
    ### Create grid for item
    result_grid = {}
    for x in range(2):
        y = st.columns(6)
        for y, col in enumerate(y):
            coord = (x, y)
            tile = col.container()
            result_grid[coord] = tile

    row = 0
    result_grid[(row, 0)].markdown("##### Item")
    result_grid[(row, 1)].markdown("##### ID:")
    result_grid[(row, 2)].markdown("##### Number per craft:")
    result_grid[(row, 3)].markdown("##### Shop price:")
    result_grid[(row, 4)].markdown("##### NQ Price:")
    result_grid[(row, 5)].markdown("##### HQ Price:")

    df_dict = df.to_dicts()
    # df_dict
    for row, item in enumerate(df_dict, start=0):
        if item["ingredient_of"] == None:
            for k, v in item.items():
                if k.lower() == "name":
                    result_grid[(row, 0)].write(v)
                if k.lower() == "id":
                    result_grid[(row, 1)].write(v)
                if k.lower() == "amount":
                    result_grid[(row, 2)].write(f"{v}")
                with result_grid[(row, 3)]:
                    if k.lower() == "shop_price":
                        if v is None:
                            st.write(":red[No Shop]")
                        else:
                            st.write(f"{v:,}")
                with result_grid[(row, 4)]:
                    if k.lower() == "nq_price":
                        if v is None:
                            st.write(":red[No NQ]")
                        else:
                            st.write(f"{v:,}")
                with result_grid[(row, 5)]:
                    if k.lower() == "hq_price":
                        if v is None:
                            st.write(":red[No HQ]")
                        else:
                            st.write(f"{v:,}")


def print_metrics(item_id: str, df: pl.DataFrame, craft_cost_each: int) -> float:
    st.markdown("## Craft Details")

    prices = (
        duckdb.sql(
            f"select shop_price, nq_price, hq_price from df where id = {item_id}"
        )
        .pl()
        .to_dicts()[0]
    )
    prices = {k: v for k, v in prices.items() if v is not None}
    result_min_source = min(prices, key=prices.get)
    result_min_price = prices[result_min_source]

    metric_col1, metric_col2, metric_col3, metric_col4, metric_col5, metric_col6 = (
        st.columns(6)
    )

    amount = df["amount"][0]
    hq_price_each = df["hq_price"][0]
    hq_price_total = hq_price_each * amount
    pl_each = hq_price_each - craft_cost_each
    pl_total = pl_each * amount
    pl_total_formatted = f"{pl_total:,}"
    pl_perc = pl_each / hq_price_each
    pl_perc_formatted = f"{pl_perc:,.2%}"
    with metric_col5:
        with st.container(border=True):
            if df["amount"][0] == 1:
                st.metric(
                    f"Cheapest price: :blue[{result_min_source.split('_')[0]}]",
                    f"{result_min_price:,}",
                )
            elif df["amount"][0] > 1:
                st.metric(
                    f"Cheapest price: :blue[{result_min_source.split('_')[0]}]",
                    f"{result_min_price * amount:,} ({result_min_price:,} each)",
                )
    with metric_col6:
        with st.container(border=True):
            if df["hq_price"] is not None:
                if df["amount"][0] == 1:
                    st.metric(f"HQ Price", f"{hq_price_each:,}")
                if df["amount"][0] > 1:
                    st.metric(
                        f"HQ Price", f"{hq_price_total:,} ({hq_price_each:,} each)"
                    )
    with metric_col1:
        with st.container(border=True):
            if df["amount"][0] == 1:
                st.metric(f"Craft Cost", f"{craft_cost_each:,}", pl_total_formatted)
            if df["amount"][0] > 1:
                st.metric(
                    f"Craft Cost",
                    f"{craft_cost_each * amount:,} ({craft_cost_each:,} each)",
                    pl_total_formatted,
                )
    with metric_col2:
        with st.container(border=True):
            st.metric(f"Profit/Loss %", pl_perc_formatted)

    return pl_perc


def print_warning(pl_perc: float) -> None:
    if pl_perc < 0:
        st.error("Buying ingredients and crafting this item will result in a loss!")
    elif pl_perc < 0.2:
        st.warning("Note: Low profit margin (below 20%)!")


@st.fragment
def print_ingredients(df: pl.DataFrame) -> int:
    st.markdown("#")
    st.markdown("# Ingredients")

    ### Create grid for items
    ingr_grid = {}
    for x in range(len(df)):
        y = st.columns(7)
        for y, col in enumerate(y):
            coord = (x, y)
            tile = col.container()
            ingr_grid[coord] = tile

    row = 0
    ingr_grid[(row, 0)].markdown("##### Ingredient")
    ingr_grid[(row, 1)].markdown("##### ID")
    ingr_grid[(row, 2)].markdown("##### Number Needed")
    ingr_grid[(row, 3)].markdown("##### Shop price")
    ingr_grid[(row, 4)].button("**NQ**", help="Click to set all to NQ", type="tertiary")
    ingr_grid[(row, 5)].button("**HQ**", help="Click to set all to HQ", type="tertiary")
    ingr_grid[(row, 6)].markdown("##### Cost")

    craft_cost_each = 0
    row_cost = 0
    df_dict = df.to_dicts()
    # df_dict
    for row, item in enumerate(df_dict, start=0):
        if item["ingredient_of"] == None:
            continue
        else:
            for k, v in item.items():
                if k.lower() == "name":
                    ingr_grid[(row, 0)].write(v)
                if k.lower() == "id":
                    ingr_grid[(row, 1)].write(v)
                if k.lower() == "amount":
                    ingr_grid[(row, 2)].write(v)
                with ingr_grid[(row, 3)]:
                    if k.lower() == "shop_price":
                        if v is None:
                            st.write(":red[No Shop]")
                        else:
                            st.write(f"{v}")
                            shop_qty = st.number_input(
                                "num_shop",
                                min_value=0,
                                max_value=item["amount"],
                                key=f"{item}_shop_qty",
                                label_visibility="hidden",
                            )
                            shop_total = item["shop_price"] * shop_qty
                            st.write(f"{shop_total}")
                            row_cost += shop_total
                with ingr_grid[(row, 4)]:
                    if k.lower() == "nq_price":
                        if v is None:
                            st.write(":red[No NQ]")
                        else:
                            st.write(f"{v}")
                            nq_qty = st.number_input(
                                "num_nq",
                                min_value=0,
                                max_value=item["amount"],
                                value=item["amount"],
                                key=f"{item}_nq_qty",
                                label_visibility="hidden",
                            )
                            nq_total = item["nq_price"] * nq_qty
                            st.write(f"{nq_total}")
                            row_cost += nq_total
                with ingr_grid[(row, 5)]:
                    if k.lower() == "hq_price":
                        if v is None:
                            st.write(":red[No HQ]")
                        else:
                            st.write(f"{v}")
                            hq_qty = st.number_input(
                                "num_hq",
                                min_value=0,
                                max_value=item["amount"],
                                key=f"{item}_hq_qty",
                                label_visibility="hidden",
                            )
                            hq_total = item["hq_price"] * hq_qty
                            st.write(f"{hq_total}")
                            row_cost += hq_total
            ingr_grid[(row, 6)].write(f"{row_cost}")
        craft_cost_each += row_cost

    item = df_dict[0]
    craft_cost_total = item["amount"] * craft_cost_each

    st.write(f"#### Total ingredient cost (each): :red[{craft_cost_each} gil]")
    if item["amount"] > 1:
        st.write(
            f"#### Total ingredient cost per craftable amount ({item['amount']}): :red[{craft_cost_total}]"
        )

    return craft_cost_each

  


###TODO add velocities


if __name__ == "__main__":
    st.set_page_config(layout="wide")

    recipe_list = get_all_recipes()  ### List of recipes for all craftable items in game
    selectbox_recipe_list = recipe_list.select(["result_text", "result_id"])

    st.title("FFXIV Crafting Profit/Loss Checker")
    st.markdown("")
    st.text(
        """Select recipe to check if better value to craft from ingredients or buy from marketboard.\nNumber in parentheses is item id."""
    )
    item_selectbox = st.selectbox(
        "label", options=selectbox_recipe_list, index=0, label_visibility="hidden"
    )
    ###TODO Change index back to None

    if not st.session_state:
        st.session_state["loading"] = False

    if item_selectbox:
        st.session_state.loading = True

        item_id = selectbox_recipe_list.filter(
            pl.col("result_text") == item_selectbox
        ).select("result_id")
        item_id = item_id[0, 0]

        cont_warning = st.container()
        cont_analysis = st.container()
        cont_result = st.container()
        cont_ingr = st.container()

        lookup_items_df = get_recipe_items(item_id)
        # lookup_items_df
        prices_df = price_lookup(lookup_items_df)
        # prices_df
        combined_df = join_dfs(lookup_items_df, prices_df)
        # combined_df

        with cont_ingr:
            craft_cost_each = print_ingredients(combined_df)

        with cont_result:
            print_result(combined_df)
        with cont_analysis:
            pl_perc = print_metrics(item_id, combined_df, craft_cost_each)
        with cont_warning:
            print_warning(pl_perc)


### TODO number_input dependencies
"""
TOTAL = 100

def update(last):
    change = ss.A + ss.B + ss.C - TOTAL
    sliders = ['A','B','C']    
    last = sliders.index(last)
    # Modify to 'other two'
    # Add logic here to deal with rounding and to protect against edge cases (if one of the 'others'
    # doesn't have enough room to accomodate the change)
    ss[sliders[(last+1)%3]] -= change/2
    ss[sliders[(last+2)%3]] -= change/2


st.number_input('A', key='A', min_value=0, max_value=100, value = 50, on_change=update, args=('A',))
st.number_input('B', key='B', min_value=0, max_value=100, value = 25, on_change=update, args=('B',))
st.number_input('C', key='C', min_value=0, max_value=100, value = 25, on_change=update, args=('C',))
"""