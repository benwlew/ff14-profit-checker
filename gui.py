"""
TODO
- Handle multiple regions/servers
- Host online
- Add st.query_params to allow sharing of specific item links
- Add item images?
- Add item source, e.g. currency if vendor
- Add item number dependencies for number_input (never exceed total needed); not sure how to implement
"""

from typing import List, Optional, Dict, Union
import duckdb
import requests
import polars as pl
import streamlit as st


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


def print_result(df: pl.DataFrame) -> int:
    ### Create grid for item
    result_grid = {}
    for x in range(2):
        y = st.columns(6)
        for y, col in enumerate(y):
            coord = (x, y)
            tile = col.container()
            result_grid[coord] = tile

    row = 0
    result_grid[(row, 0)].markdown("**Item**")
    result_grid[(row, 1)].markdown("**ID**")
    result_grid[(row, 2)].markdown("**Number per craft**")
    result_grid[(row, 3)].markdown(
        "**Shop price**"
    )
    result_grid[(row, 4)].markdown(
        "**NQ Price**"
    )
    result_grid[(row, 5)].markdown(
        "**HQ Price**"
    )

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
                            st.write(":red[N/A  \n(Not sold in shop)]")
                        else:
                            st.write(f"{v:,}")
                with result_grid[(row, 4)]:
                    if k.lower() == "nq_price":
                        if v is None:
                            st.write(":red[N/A  \n(No NQ available)]")
                        else:
                            st.write(f"{v:,}")
                            st.write(f"Velocity: {item['nq_velocity']:,}/day")
                with result_grid[(row, 5)]:
                    if k.lower() == "hq_price":
                        if v is None:
                            st.write(":red[N/A  \n(No HQ available)]")
                        else:
                            st.write(f"{v:,}")
                            st.write(f"Velocity: {item['hq_velocity']:,}/day")
    try:
        hq_velocity = df[0]["hq_velocity"][0]
    except:
        hq_velocity = 0
    try:
        nq_velocity = df[0]["nq_velocity"][0]
    except:
        nq_velocity = 0

    total_velocity = hq_velocity + nq_velocity
    return total_velocity


def print_metrics(item_id: str, df: pl.DataFrame, craft_cost_total: int) -> float:
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
    craft_cost_each = int(craft_cost_total / amount)
    hq_price_each = df["hq_price"][0]
    hq_price_total = hq_price_each * amount
    pl_each = hq_price_each - craft_cost_each
    pl_total = pl_each * amount
    pl_total_formatted = f"{pl_total:,}"
    pl_perc = pl_each / hq_price_each
    pl_perc_formatted = f"{pl_perc:,.2%}"

    with metric_col1:
        with st.container(border=True):
            if df["amount"][0] == 1:
                st.metric(f"Craft Cost", f"{craft_cost_each:,}")
            if df["amount"][0] > 1:
                st.metric(
                    f"Craft Cost",
                    f"{craft_cost_total:,} ({craft_cost_each:,})"
                )
    with metric_col2:
        with st.container(border=True):
            if df["amount"][0] == 1:
                st.metric(
                    f"Profit/Loss",
                    f"{pl_each:,}",
                    pl_perc_formatted,
                    help="Calculated agains HQ buy price - assuming that crafters will always aim for HQ",
                )
            if df["amount"][0] > 1:
                st.metric(
                    f"Profit/Loss",
                    f"{pl_total:,} ({pl_each:,})",
                    pl_perc_formatted,
                    help="Calculated agains HQ buy price - assuming that crafters will always aim for HQ",
                )
    with metric_col4:
        with st.container(border=True):
            if df["amount"][0] == 1:
                st.metric(
                    f"Cheapest price: :blue[{result_min_source.split('_')[0]}]",
                    f"{result_min_price:,}",
                )
            elif df["amount"][0] > 1:
                st.metric(
                    f"Cheapest price: :blue[{result_min_source.split('_')[0]}]",
                    f"{result_min_price * amount:,} ({result_min_price:,})",
                    help="Number in parentheses is single item cost",
                )
    with metric_col5:
        with st.container(border=True):
            if df["hq_price"] is not None:
                if df["amount"][0] == 1:
                    st.metric(f"HQ Price", f"{hq_price_each:,}")
                if df["amount"][0] > 1:
                    st.metric(
                        f"HQ Price",
                        f"{hq_price_total:,} ({hq_price_each:,})",
                        help="Number in parentheses is single item cost",
                    )

    return pl_perc


def print_pl_warning(pl_perc: float) -> None:
    if pl_perc < 0:
        st.error(
            f"Buying ingredients and crafting this item will result in a loss: {pl_perc:,.2%}",
            icon="🔥",
        )
    elif pl_perc < 0.25:
        st.warning(f"Low profit margin (below 25%): {pl_perc:,.2%}", icon="🚨")
    else:
        st.success(f"Profit above 25%: {pl_perc:,.2%}", icon="🥳")


def print_velocity_warning(velocity: int) -> None:
    if velocity < 15:
        st.error(f"Item won't sell: {velocity:,}/day", icon="🔥")
    elif velocity < 99:
        st.warning(f"Item will sell really slowly: {velocity:,}/day", icon="🚨")
    else:
        st.success(f"Item will sell: {velocity:,}/day", icon="🥳")


@st.fragment
def print_ingredients(df: pl.DataFrame) -> int:
    st.markdown("")
    st.markdown("# Ingredients")
    st.text(
        "All items are set to NQ by default, but can be adjusted. The cost will update dynamically as item amounts are changed.  \nEach column is set to max out at the number needed, but it is possible to increase numbers over this limit when using multiple columns; to avoid this, users should lower NQ after increasing HQ and vice versa."
    )
    st.markdown("")
    ### Create grid for items
    ingr_grid = {}
    for x in range(len(df)):
        y = st.columns(7)
        for y, col in enumerate(y):
            coord = (x, y)
            tile = col.container()
            ingr_grid[coord] = tile

    row = 0
    ingr_grid[(row, 0)].markdown("**Ingredient**")
    ingr_grid[(row, 1)].markdown("**ID**", help="Click link to lookup item profit/loss of subcraft (opens in new tab)")
    ingr_grid[(row, 2)].markdown("**Required**")
    ingr_grid[(row, 3)].markdown(
        "**Shop price**"
    )
    ### TODO Buttons to set all don't work yet
    ingr_grid[(row, 4)].button(
        "**NQ**",
        help="Click to set all to NQ",
        type="tertiary",
    )
    ingr_grid[(row, 5)].button(
        "**HQ**",
        help="Click to set all to HQ",
        type="tertiary",
    )
    ingr_grid[(row, 6)].markdown("**Cost**")

    craft_cost_total = 0
    df_dict = df.to_dicts()
    for row, item in enumerate(df_dict, start=0):
        row_cost = 0
        if item["ingredient_of"] == None:
            continue
        else:
            for k, v in item.items():
                if k.lower() == "name":
                    ingr_grid[(row, 0)].write(v)
                if k.lower() == "id":
                    with ingr_grid[(row, 1)]:
                        if int(v) in recipe_list["result_id"].to_list():
                            st.markdown(f"[{v}](/?id={v})")
                            # if id_button:
                            #     st.session_state.default_item_index = selectbox_recipe_list.select(pl.col("result_id").index_of(v))[0,0]
                            #     st.rerun()
                        else:
                            st.write(v)
                if k.lower() == "amount":
                    ingr_grid[(row, 2)].write(f"{v}")
                with ingr_grid[(row, 3)]:
                    if k.lower() == "shop_price":
                        if v is None:
                            st.write(":red[N/A  \n(Not sold in shop)]")
                        else:
                            shop_qty = st.number_input(
                                "num_shop",
                                min_value=0,
                                max_value=item["amount"],
                                key=f"{item}_shop_qty",
                                label_visibility="hidden",
                            )
                            shop_total = item["shop_price"] * shop_qty
                            st.write(f"{shop_total:,} gil ({v:,} gil each)")
                            row_cost += shop_total
                with ingr_grid[(row, 4)]:
                    if k.lower() == "nq_price":
                        if v is None:
                            st.write(":red[N/A  \n(No NQ available)]")
                        else:
                            nq_qty = st.number_input(
                                "num_nq",
                                min_value=0,
                                max_value=item["amount"],
                                value=item["amount"],
                                key=f"{item}_nq_qty",
                                label_visibility="hidden",
                            )
                            nq_total = item["nq_price"] * nq_qty
                            st.write(f"{nq_total:,} gil ({v:,} gil each)")
                            st.write(f"Velocity: {item['nq_velocity']:,}/day")
                            row_cost += nq_total
                with ingr_grid[(row, 5)]:
                    if k.lower() == "hq_price":
                        if v is None:
                            st.write(":red[N/A  \n(No HQ available)]")
                        else:
                            hq_qty = st.number_input(
                                "num_hq",
                                min_value=0,
                                max_value=item["amount"],
                                key=f"{item}_hq_qty",
                                label_visibility="hidden",
                            )
                            hq_total = item["hq_price"] * hq_qty
                            st.write(f"{hq_total:,} gil ({v:,} gil each)")
                            st.write(f"Velocity: {item['nq_velocity']:,}/day")
                            row_cost += hq_total
            ingr_grid[(row, 6)].write(f"{row_cost:,}")
        craft_cost_total += row_cost

    item = df_dict[0]
    craft_cost_each = int(craft_cost_total / item["amount"])

    if item["amount"] > 1:
        st.write(
            f"#### Total ingredient cost per craftable amount ({item['amount']}): :red[{craft_cost_total}]"
        )
    st.write(f"#### Total ingredient cost (each): :red[{craft_cost_each} gil]")

    with cont_analysis:
        pl_perc = print_metrics(item_id, combined_df, craft_cost_total)

    with cont_pl_warning:
        print_pl_warning(pl_perc)

    return craft_cost_total

@st.fragment
def print_ingredients_table(df: pl.DataFrame) -> int:

    st.markdown("### Test for ingredient data editor")
    
    cleaned_output_df = df
    cleaned_output_df = cleaned_output_df.with_columns(
        pl.lit(None).alias("shop_amount"),
        pl.lit(None).alias("shop_cost"),
        pl.col("amount").alias("nq_amount"),
        pl.lit(None).alias("nq_cost"),
        pl.lit(None).alias("hq_amount"),
        pl.lit(None).alias("hq_cost"),
        pl.lit(None).alias("total_cost"),
    )

    cleaned_output_df = cleaned_output_df.select(
        [
            pl.col("Name"),
            pl.col("amount").alias("Required"),
            pl.col("shop_price").alias("Shop Price"),
            pl.col("shop_amount").alias("Shop Amount"),
            pl.col("shop_cost").alias("Shop Cost"),
            pl.col("nq_price").alias("NQ Price"),
            pl.col("nq_amount").alias("NQ Amount"),
            pl.col("nq_cost").alias("NQ Cost"),
            pl.col("nq_velocity").alias("NQ Velocity"),
            pl.col("hq_price").alias("HQ Price"),
            pl.col("hq_amount").alias("HQ Amount"),
            pl.col("hq_cost").alias("HQ Cost"),
            pl.col("hq_velocity").alias("HQ Velocity"),
            pl.col("total_cost").alias("Total Cost"),
        ]
    ).slice(1,None)
    
    def grid_update(editable_df: pl.DataFrame) -> pl.DataFrame:
        editable_df = editable_df.with_columns(
            (pl.col("Shop Price").cast(pl.Int64) * pl.col("Shop Amount").cast(pl.Int64)).alias("Shop Cost"))
        editable_df = editable_df.with_columns(
            (pl.col("NQ Price").cast(pl.Int64) * pl.col("NQ Amount").cast(pl.Int64)).alias("NQ Cost"))
        editable_df = editable_df.with_columns(
            (pl.col("HQ Price").cast(pl.Int64) * pl.col("HQ Amount").cast(pl.Int64)).alias("HQ Cost"))
        editable_df = editable_df.with_columns(pl.sum_horizontal("Shop Cost", "NQ Cost", "HQ Cost").alias("Total Cost"))
        return editable_df

    if "ingredients_df" not in st.session_state:
        st.session_state.ingredients_df = cleaned_output_df
        st.session_state.ingredients_df = grid_update(st.session_state.ingredients_df)
    
    editable_df = st.data_editor(st.session_state.ingredients_df,
                             disabled=["Name", "Required", "Shop Price", "Shop Cost", "NQ Price", "NQ Cost", "NQ Velocity",
                                        "HQ Price", "HQ Cost", "HQ Velocity", "Total Cost"],
                                        key="ingredient_data_editor")



    if not editable_df.equals(st.session_state.ingredients_df):
        editable_df = grid_update(editable_df)
        st.session_state.ingredients_df = editable_df
        # editable_df
        st.rerun()
    



if __name__ == "__main__":
    st.set_page_config(layout="wide", page_title="FFXIV Crafting Profit/Loss Checker")

    recipe_list = get_all_recipes()  ### List of recipes for all craftable items in game
    selectbox_recipe_list = recipe_list.select(["result_text", "result_id"])

    col1, col2 = st.columns(2)
    with col1:
        st.title("FFXIV Crafting Profit/Loss Checker")
    st.markdown("")
    
    with col2:
        with st.container(horizontal_alignment="right"):
            region_selectbox = st.selectbox("Select region", "Japan", width=200)

    col1, col2 = st.columns(2)
    with col1:
        st.text(
            "Select recipe to check if better value to craft from ingredients or buy from marketboard.  \nNumber in parentheses is item id."
        )

    with col2:
        with st.container(horizontal_alignment="right", vertical_alignment="top"):
            region_selectbox = st.selectbox("Select server", "Masamune", width=200)

    
    if "default_item_index" not in st.session_state:
        # Pass id parameter from url if available
        try:
            st.session_state.default_item_index = selectbox_recipe_list.select(pl.col("result_id").index_of(st.query_params["id"]))[0,0]
        except:
            st.session_state.default_item_index = None
        
    item_selectbox = st.selectbox(
        "label", options=selectbox_recipe_list["result_text"], index=st.session_state.default_item_index, label_visibility="hidden"
    )

    if item_selectbox:
        item_id = selectbox_recipe_list.filter(
            pl.col("result_text") == item_selectbox
        ).select("result_id")
        item_id = item_id[0, 0]       
        st.set_page_config(layout="wide", page_title=item_selectbox)

        cont_pl_warning = st.empty()
        cont_velocity_warning = st.empty()
        cont_analysis = st.empty()
        cont_result = st.empty()
        cont_ingr = st.empty()
        cont_ingr_table = st.container()

        lookup_items_df = get_recipe_items(item_id)
        # lookup_items_df
        prices_df = price_lookup(lookup_items_df)
        # prices_df
        combined_df = join_dfs(lookup_items_df, prices_df)
        # combined_df

        with cont_ingr:
            craft_cost_total = print_ingredients(combined_df)
        # with cont_ingr_table:
        #     print_ingredients_table(combined_df)
        with cont_result:
            velocity = print_result(combined_df)
        with cont_analysis:
            pl_perc = print_metrics(item_id, combined_df, craft_cost_total)
        with cont_pl_warning:
            print_pl_warning(pl_perc)
        with cont_velocity_warning:
            print_velocity_warning(velocity)


### TODO number_input dependencies


# TOTAL = 100

# def update(last):
#     change = ss.A + ss.B + ss.C - TOTAL
#     sliders = ['A','B','C']
#     last = sliders.index(last)
#     # Modify to 'other two'
#     # Add logic here to deal with rounding and to protect against edge cases (if one of the 'others'
#     # doesn't have enough room to accomodate the change)
#     ss[sliders[(last+1)%3]] -= change/2
#     ss[sliders[(last+2)%3]] -= change/2


# st.number_input('A', key='A', min_value=0, max_value=100, value = 50, on_change=update, args=('A',))
# st.number_input('B', key='B', min_value=0, max_value=100, value = 25, on_change=update, args=('B',))
# st.number_input('C', key='C', min_value=0, max_value=100, value = 25, on_change=update, args=('C',))
