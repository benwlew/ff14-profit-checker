"""
TODO
- Remove items that are craftable but not sellable
- Add item source, e.g. currency if vendor
- Add Japanese language support
- Make sure large numbers fit in st.metrics
- Add world functionality (cheapest selling on dc vs world)
- Add checkbox to consider p/l against NQ
"""

import duckdb
import requests
import polars as pl
import streamlit as st
import json


DB_NAME = "ffxiv_price.duckdb"



@st.cache_data
def get_worlds_dc() -> pl.DataFrame:
    with duckdb.connect(DB_NAME) as con:
        query = """SELECT * from  world_dc"""
        df = con.sql(query).pl()
    return df

@st.cache_data
def get_all_recipes() -> pl.DataFrame:
    with duckdb.connect(DB_NAME) as con:
        query = """SELECT * from  recipe_price"""
        df = con.sql(query).pl()


    results_df = df.filter(pl.col("recipe_part") == "result")
    two_job_craftable = results_df.filter(pl.col("item_id").is_duplicated())

    # Concat item_id to the end of item_name to make selectbox easily searchable
    # Some items can be crafted by two jobs (ARM/BSM) with slightly different recipes, so appending job name to the end as well
    df = df.with_columns(
        pl.when(df["recipe_id"].is_in(two_job_craftable["recipe_id"]))
        .then(pl.concat_str(["item_name", pl.lit(" ("), "item_id", pl.lit(")"),pl.lit(" ("), "job", pl.lit(")")]))
        .otherwise(pl.concat_str(["item_name", pl.lit(" ("), "item_id", pl.lit(")")]))
        .alias("selectbox_label")
    )

    return df


@st.cache_data
def get_prices_from_universalis(lookup_items_df: pl.DataFrame, region: str) -> pl.DataFrame:
    ## Get market price data from universalis API
    
    # Prepare API call with parameters
    lookup_item_ids  =  [
        str(id) for id in lookup_items_df["item_id"]]
    url = f"https://universalis.app/api/v2/{region}/{','.join(lookup_item_ids)}"
    
    # GET data from Universalis API twice per item - once each for NQ/HQ
    raw_market_data = {}
    for hq in [False, True]:
        parameters = {
            "hq": hq,
            "listings": 100,
            "fields": "items.nqSaleVelocity,items.hqSaleVelocity,items.listings.pricePerUnit,items.listings.onMannequin,listings.worldName",
        }
        
        try:
            response = requests.get(url, params=parameters)
            response.raise_for_status()
        except Exception as e:
            st.error("No response from Universalis.app - please try again")
            st.markdown(f"e: response.status_code")
            st.stop()


        # Unpivot and unnest json data
        data = response.json()["items"]
        df = pl.DataFrame(data).unpivot().rename({"variable": "id"}).unnest("value")
        df = df.explode("listings").unnest("listings")
        
        # Filter out Mannquin items (irrelevant listings)
        df = df.filter(
            (pl.col("onMannequin") == False) | pl.col("onMannequin").is_null()
        )

        # Find minimum price for each group, then merge NQ/HQ data together
        df = df.group_by("id").min()
        if hq:
            hq_df = df.select(
                pl.col("id"),
                pl.col("pricePerUnit").alias("hq_price"),
                pl.col("hqSaleVelocity").alias("hq_velocity"),
            )
        elif not hq:
            nq_df = df.select(
                pl.col("id"),
                pl.col("pricePerUnit").alias("nq_price"),
                pl.col("nqSaleVelocity").alias("nq_velocity"),
            )

    prices_df = nq_df.join(hq_df, on="id").sort("id").cast(pl.Int64).rename({"id":"item_id"})

    return prices_df


def print_result(df: pl.DataFrame) -> int:
    st.markdown("## Craft Details")
    
    ## Create grid for result item

    # Create grid for data
    result_grid = {}
    for x in range(2):
        y = st.columns(5, border=True, gap=None)
        for y, col in enumerate(y):
            coord = (x, y)
            tile = col.container()
            result_grid[coord] = tile
    
    name = df.item(0, "item_name")
    id = df.item(0, "item_id")
    amount = df.item(0, "item_amount")
    shop_price = df.item(0, "shop_price")
    nq_price = df.item(0, "nq_price")
    nq_velocity = df.item(0, "nq_velocity")
    hq_price = df.item(0, "hq_price")
    hq_velocity = df.item(0, "hq_velocity")
    icon_url = make_icon_url(df.item(0, "item_icon"))

    # Populate header row with column names
    row = 0
    result_grid[(row, 0)].markdown("**Item**",
                                   help="Number below item name is item ID")
    result_grid[(row, 1)].markdown("**Number per craft**")
    result_grid[(row, 2)].markdown("**Shop price**")
    result_grid[(row, 3)].markdown("**NQ Price**")
    result_grid[(row, 4)].markdown("**HQ Price**")

    # Populate second row with result item data
    row = 1
    with result_grid[(row, 0)]:
        st.markdown(f"![{name}]({icon_url}) {name} ({str(id)})")
    result_grid[(row, 1)].write(f"{amount}")
    with result_grid[(row, 2)]:
        if shop_price is None:
            st.write(":red[N/A  \n(Not sold in shop)]")
        else:
            st.write(f"{shop_price:,}")
    with result_grid[(row, 3)]:
        if nq_price is None:
            st.write(":red[N/A  \n(No NQ available)]")
        else:
            st.write(f"{nq_price:,}")
            st.write(f"Velocity: {nq_velocity:,}/day")
    with result_grid[(row, 4)]:
        if hq_price is None:
            st.write(":red[N/A  \n(No HQ available)]")
        else:
            st.write(f"{hq_price:,}")
            st.write(f"Velocity: {hq_velocity:,}/day")


def print_metrics(result_df: pl.DataFrame, craft_cost_total: int) -> float:
    ## Create summary table at top of page
    # Calculate source of cheapest item
    min_price_df = result_df.select(pl.col("shop_price", "nq_price","hq_price")).unpivot(variable_name="source", value_name="price")
    min_price_df = min_price_df.filter(pl.col("price") == pl.col("price").min()).unique()
    try:
        result_min_source = min_price_df.select(pl.col("source")).item()
    except: 
    # Prioritise HQ > Shop > NQ if multiple sources have the same price
        if "hq_price" in min_price_df["source"].to_list():
            result_min_source = "hq_price"
        elif "shop_price" in min_price_df["source"].to_list():
            result_min_source = "shop_price"
        elif "nq_price" in min_price_df["source"].to_list():
            result_min_source = "nq_price"
    result_min_price = min_price_df.select(pl.col("price")).unique().item()
    

    # Calculate numbers to go into grid
    result_amount = result_df.item(0, "item_amount")
    craft_cost_each = int(craft_cost_total / result_amount)
    
    # TODO Handle result item with no HQ available
    try:
        hq_price_each = result_df.item(0, "hq_price")
        hq_price_total = hq_price_each * result_amount
        pl_each = hq_price_each - craft_cost_each
        pl_total = pl_each * result_amount
        pl_perc = pl_each / hq_price_each

    # Create empty grid for result item data    
        metric_col1, metric_col2, metric_col3, metric_col4, metric_col5, metric_col6 = (
        st.columns(6, border=True, gap=None)
    )
        with metric_col1:
            with st.container():
                if result_amount == 1:
                    st.metric(f"Craft Cost", f"{craft_cost_each:,} gil")
                elif result_amount > 1:
                    st.metric(
                        f"Craft Cost", f"{craft_cost_total:,} ({craft_cost_each:,} gil)"
                    )
        with metric_col2:
            with st.container():
                if result_amount == 1:
                    st.metric(
                        f"Profit/Loss (NQ)",
                        f"{pl_each:,} gil",
                        f"{pl_perc:.2%}",
                        help="Calculated agains HQ buy price - assuming that crafters will always aim for HQ",
                    )
                if result_amount > 1:
                    st.metric(
                        f"Profit/Loss (NQ)",
                        f"{pl_total:,} gil ({pl_each:,} gil each)",
                        f"{pl_perc:.2%}",
                        help="Calculated agains HQ buy price - assuming that crafters will always aim for HQ",
                    )
        with metric_col3:
            with st.container():
                if result_amount == 1:
                    st.metric(
                        f"Profit/Loss (HQ)",
                        f"{pl_each:,} gil",
                        f"{pl_perc:.2%}",
                        help="Calculated agains HQ buy price - assuming that crafters will always aim for HQ",
                    )
                if result_amount > 1:
                    st.metric(
                        f"Profit/Loss (HQ)",
                        f"{pl_total:,} gil ({pl_each:,} gil each)",
                        f"{pl_perc:.2%}",
                        help="Calculated agains HQ buy price - assuming that crafters will always aim for HQ",
                    )
        
        with metric_col4:
            with st.container():
                if result_amount == 1:
                    st.metric(
                        f"Cheapest price: :blue[{result_min_source.split('_')[0]}]",
                        f"{result_min_price:,} gil",
                    )
                elif result_amount > 1:
                    st.metric(
                        f"Cheapest price: :blue[{result_min_source.split('_')[0]}]",
                        f"{result_min_price * result_amount:,} gil ({result_min_price:,} gil each)",
                        help="Number in parentheses is single item cost",
                    )
        with metric_col5:
            with st.container():
                if nq_price is not None:
                    if result_amount == 1:
                        st.metric(f"NQ Price", f"{hq_price_each:,} gil")
                    if result_amount > 1:
                        st.metric(
                            f"NQ Price",
                            f"{nq_price_total:,} gil ({nq_price_each:,} gil each)",
                            help="Number in parentheses is single item cost",
                        )
        with metric_col6:
            with st.container():
                if hq_price is not None:
                    if result_amount == 1:
                        st.metric(f"HQ Price", f"{hq_price_each:,} gil")
                    if result_amount > 1:
                        st.metric(
                            f"HQ Price",
                            f"{hq_price_total:,} gil ({hq_price_each:,} gil each)",
                            help="Number in parentheses is single item cost",
                        )

    except:
        nq_price_each = result_df.item(0, "nq_price")
        nq_price_total = nq_price_each * result_amount
        pl_each = nq_price_each - craft_cost_each
        pl_total = pl_each * result_amount
        pl_perc = pl_each / nq_price_each

    return pl_perc #should handle nq+hq pl_perc



def print_pl_warning(pl_perc: float):
    ## Display advice based on profit/loss %
    if pl_perc < 0:
        st.error(
            f"&nbsp; Buying ingredients and crafting this item will result in a loss: {pl_perc:,.2%}",
            icon="🔥",
        )
    elif pl_perc < 0.25:
        st.warning(f"&nbsp; Low profit margin (below 25%): {pl_perc:,.2%}", icon="🚨")
    else:
        st.success(f"&nbsp; Profit above 25%: {pl_perc:,.2%}", icon="🥳")


def print_velocity_warning(velocity: int) -> None:
    ## Display advice based on velocity
    if velocity is None:
        return
    elif velocity < 15:
        st.error(f"&nbsp; Item won't sell: average {velocity:,} sold/day", icon="🔥")
    elif velocity < 99:
        st.warning(f"&nbsp; Item will sell really slowly: average {velocity:,} sold/day", icon="🚨")
    else:
        st.success(f"&nbsp; Item will sell: average {velocity:,} sold/day", icon="🥳")


@st.fragment
def print_ingredients(df: pl.DataFrame):
    ## List ingredient data in a grid for clarity
    
    # Split data into result & ingredient dfs
    result_df = df.filter(pl.col("recipe_part") == "result")
    ingr_df = df.filter(pl.col("recipe_part").str.contains("ingredient"))
    
    # Create header for section
    st.markdown("")
    st.markdown("# Ingredients")
    st.text(
        "All items are set to NQ by default, but can be adjusted.\n"
        "Cost will update dynamically as item amounts are changed.\n"
        "All items are set to NQ by default - if increasing other columns, make sure to decrease NQ as this is not automatic."
    )
    st.markdown("")

    ### Create blank grid for items
    ingr_grid = {}
    for x in range(len(ingr_df) + 1):
        y = st.columns(6, border=True, gap=None)
        for y, col in enumerate(y):
            coord = (x, y)
            tile = col.container()
            ingr_grid[coord] = tile

    
    # Populate grid header row with column names
    row = 0
    ingr_grid[(row, 0)].markdown("**Ingredient**",
                                 help="Number below item name is item ID; click link to lookup item profit/loss of subcraft (only for craftable items)")
    ingr_grid[(row, 1)].markdown("**Required**")
    ingr_grid[(row, 2)].markdown("**Shop price**")
    ingr_grid[(row, 3)].markdown("**NQ**")
    ingr_grid[(row, 4)].markdown("**HQ**")
    ingr_grid[(row, 5)].markdown("**Cost**")

    ## Populate grid using data from ingredient df

    # Initialise variables from ingredient df
    craft_cost_total = 0

    for row in range(1, len(ingr_df) + 1):
        row_cost = 0
        index = row - 1
        name = ingr_df.item(index, "item_name")
        id = ingr_df.item(index, "item_id")
        amount = ingr_df.item(index, "item_amount")
        shop_price = ingr_df.item(index, "shop_price")
        nq_price = ingr_df.item(index, "nq_price")
        nq_velocity = ingr_df.item(index, "nq_velocity")
        hq_price = ingr_df.item(index, "hq_price")
        hq_velocity = ingr_df.item(index, "hq_velocity")
        icon_url = make_icon_url(df.item(index, "item_icon"))


        # Ingredient name column
        with ingr_grid[(row, 0)]:
            if int(id) in results_df["item_id"]:
                st.markdown(f"![{name}]({icon_url}) {name} ([{id}](/?id={id}))")
            else:
                st.markdown(f"![{name}]({icon_url}) {name} ({id})")

        # Ingredient amount column
        ingr_grid[(row, 1)].markdown(str(amount))

        # Ingredient shop quantity/price column
        with ingr_grid[(row, 2)]:
            if shop_price is None:
                st.write(":red[N/A  \n(Not sold in shop)]")
            else:
                shop_qty = st.number_input(
                    "num_shop",
                    min_value=0,
                    max_value=amount,
                    key=f"{id}_shop_qty",
                    label_visibility="hidden",
                )
                shop_total = shop_price * shop_qty
                st.write(f"{shop_total:,} gil ({shop_price:,} gil each)")
                row_cost += shop_total

        # Ingredient market NQ quantity/price column
        with ingr_grid[(row, 3)]:
            if nq_price is None:
                st.write(":red[N/A  \n(No NQ available)]")
            else:
                nq_qty = st.number_input(
                    "num_nq",
                    min_value=0,
                    max_value=amount,
                    value=amount,
                    key=f"{id}_nq_qty",
                    label_visibility="hidden",
                )
                nq_total = nq_price * nq_qty
                st.write(f"{nq_total:,} gil ({nq_price:,} gil each)")
                st.write(f"Velocity: {nq_velocity:,}/day")
                row_cost += nq_total
        
        # Ingredient market HQ quantity/price column
        with ingr_grid[(row, 4)]:
            if hq_price is None:
                st.write(":red[N/A  \n(No HQ available)]")
            else:
                hq_qty = st.number_input(
                    "num_hq",
                    min_value=0,
                    max_value=amount,
                    key=f"{id}_hq_qty",
                    label_visibility="hidden",
                )
                hq_total = hq_price * hq_qty
                st.write(f"{hq_total:,} gil ({hq_price:,} gil each)")
                st.write(f"Velocity: {nq_velocity:,}/day")
                row_cost += hq_total

        # Ingredient total cost (per ingredient) column
        ingr_grid[(row, 5)].write(f"{row_cost:,}")
        


        # Calculate and display ingredient total cost (all ingredients) row
        craft_cost_total += row_cost
    result_amount = result_df.item(0, "item_amount")
    nq_velocity = result_df.item(0, "nq_velocity")
    hq_velocity = result_df.item(0, "hq_velocity")

    total_velocity = nq_velocity + hq_velocity
    craft_cost_each = int(craft_cost_total / result_amount)

    if result_amount > 1:
        st.write(
            f"#### Total ingredient cost per craftable amount ({result_amount}): :red[{craft_cost_total:,}]"
        )
    st.write(f"#### Total ingredient cost: :red[{craft_cost_each:,} gil each]")

    if total_velocity is None:
        with cont_analysis:
            st.error(
                "Error fetching price data from Universalis - item may be too new or Universalis may be down."
            )
            st.stop()
    else:
        # Display summary of calculations at top of page
        with cont_analysis:
            pl_perc = print_metrics(result_df, craft_cost_total)
        with cont_result:
            print_result(result_df)
        with cont_pl_warning:
            print_pl_warning(pl_perc)
            print_velocity_warning(total_velocity)

    return


def make_icon_url(icon: int) -> str:
    folder = f"{icon:0>6}"
    folder = folder[:3] + "000"
    icon_url = f"https://v2.xivapi.com/api/asset?path=ui/icon/{folder}/{icon:0>6}.tex&format=png"

    return icon_url


if __name__ == "__main__":
    st.set_page_config(layout="wide", page_title="FFXIV Crafting Profit Calculator")

    # Initialise page params
    # Doesn't seem to be working on streamlit cloud?
    if "dc" not in st.session_state:
        try:
            st.session_state.dc = st.query_params.dc
        except:
            st.session_state.dc = "Mana"
    if "item" not in st.session_state:
        try:
            st.session_state.item = st.query_params.item
        except:
            st.session_state.item = None


    # Initialise data centre and world dfs/lists
    worlds_dc_df = get_worlds_dc()
    dc_list = worlds_dc_df.select("datacentre").unique().to_series().to_list()
    dc_list.sort()

    world_list = worlds_dc_df.select("world").to_series().to_list()
    world_list.sort()

    @st.fragment
    def filter_world(world_list, dc):
        world_list = (
            worlds_dc_df.filter(pl.col("datacentre") == dc)
            .select("world")
            .to_series()
            .to_list()
        )
        return world_list
    

    # Initialise item and recipe dfs/lists
    all_recipes_df = get_all_recipes()  
    results_df = all_recipes_df.filter(pl.col("recipe_part") == "result")
    ingr_df = all_recipes_df.filter(pl.col("recipe_part").str.contains("ingredient"))


    ## Create page elements
    # Create sidebar for settings
    
    with st.sidebar:
        dc_selectbox = st.selectbox(
            label="Select datacentre", options=dc_list, index=dc_list.index(st.session_state.dc)
        )
   
        world_selectbox = st.selectbox(
            "Select world (optional)", world_list, index=None, width=200
        )


    # Create main page elements
    st.title("FFXIV Crafting Profit Calculator")
        
    # CSS injection to set non-rounded corners on columns to make them look like tables
    # CSS injection to remove gap between rows of columns to make them look like tables
    st.markdown(
    """
    <style>
    .stColumn {
        border-radius: 0px;
        border: 2.5px solid grey;
        gap: 0rem;
    }
            .stVerticalBlock :only-child {
                gap: 0px !important;
            }
        </style>
    """, unsafe_allow_html=True)

    st.markdown(
        "### Lookup item prices by crafted item to check if it's better value to craft from ingredients or buy direct from the marketboard/shops! ###"
    )
    st.markdown("")
        
    
    # Create recipe selectbox, including formatting data
    recipe_selectbox_df = results_df.select(pl.col("selectbox_label","recipe_id", "item_id"))

    def selectbox_index() -> int | None:
    # Converts "item" query parameter to index used in selectbox
        try:
            item_id = int(st.session_state.item)
            index = recipe_selectbox_df["item_id"].to_list().index(item_id)
            return index
        except:
            return None

    item_selectbox = st.selectbox(
        label="Select recipe (number in parentheses is item id)",
        options=recipe_selectbox_df["selectbox_label"],
        index=selectbox_index())
    


    # Create elements and manipulate data that are loaded once item has been selected
    if item_selectbox:

        item_id = recipe_selectbox_df.filter(
            pl.col("selectbox_label") == item_selectbox
        ).select("item_id").item()

        recipe_id = recipe_selectbox_df.filter(
            pl.col("selectbox_label") == item_selectbox
        ).select("recipe_id").item()
        
        # Update session state, page URL and page title after item is selected
        if st.session_state.item == item_id:
            pass
        else:
            st.session_state.item = item_id
            st.switch_page("gui.py", query_params={"dc":dc_selectbox, "item":item_id})
        
        st.set_page_config(layout="wide", page_title=item_selectbox)


        # Create empy containers that will display information
        
        cont_analysis = st.container()
        cont_pl_warning = st.container()
        cont_result = st.container()
        cont_ingr = st.container()


        # Prepare data needed for Universalis API GET
        items_to_lookup_df = all_recipes_df.filter(pl.col("recipe_id") == recipe_id)
        market_prices_df = get_prices_from_universalis(items_to_lookup_df, st.session_state.dc)
        if world_selectbox:
            pass
            #TODO world_result_cheapest =     

        # Combine item data with market data
        output_df = items_to_lookup_df.join(market_prices_df, on="item_id", how="left").with_row_index()        
        
        # Fill containers with content from output_df; output of several containers nested inside print_ingredients()
        with cont_ingr:
            print_ingredients(output_df)


    # Update session state, page URL and page title after dc is selected
    if dc_selectbox:
        if st.session_state.dc == dc_selectbox:
            world_list = filter_world(world_list, dc_selectbox)
        else:
            st.session_state.dc = dc_selectbox
            st.switch_page("gui.py", query_params={"dc":dc_selectbox, "item":item_id})

