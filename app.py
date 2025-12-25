"""
TODO
- Fix warning banners
- Add item source, e.g. currency if vendor; SpecialShop.csv; nontrivial effort
- Add Japanese language support; not sure where source is
- Support recursive crafts (subcrafts); not sure how to implement
"""

import duckdb
import requests
import polars as pl
import streamlit as st
import time

DB_NAME = "ffxiv_price.duckdb"
home_page = st.Page("app.py", default=True)

@st.cache_data(show_spinner=False)
def get_worlds_dc() -> pl.DataFrame:
    # Read world & dc data from local duckdb
    with duckdb.connect(DB_NAME) as con:
        query = """SELECT * from  world_dc"""
        df = con.sql(query).pl()
    return df

@st.cache_data(show_spinner=False)
def get_all_recipes() -> pl.DataFrame:
    # Read recipe data from local duckdb
    with duckdb.connect(DB_NAME) as con:
        query = """SELECT * from  recipe_price"""
        df = con.sql(query).pl()

    results_df = df.filter(pl.col("recipe_part") == "result")
    
    # Concat item_id to the end of item_name to make selectbox easily searchable
    # Some items can be crafted by two jobs (ARM/BSM) with slightly different recipes, so appending job name to the end as well
    two_job_craftable = results_df.filter(pl.col("item_id").is_duplicated())

    df = df.with_columns(
        pl.when(df["recipe_id"].is_in(two_job_craftable["recipe_id"].implode()))
        .then(pl.concat_str(["item_name", pl.lit(" ("), "item_id", pl.lit(")"),pl.lit(" ("), "job", pl.lit(")")]))
        .otherwise(pl.concat_str(["item_name", pl.lit(" ("), "item_id", pl.lit(")")]))
        .alias("selectbox_label")
    )

    return df


@st.cache_data(show_spinner=False, show_time=True)
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
            "fields": "items.nqSaleVelocity,items.hqSaleVelocity,items.listings.pricePerUnit,items.listings.onMannequin,items.listings.worldName",
        }
        
        try:
            response = requests.get(url, params=parameters, timeout=10)
            response.raise_for_status()
            time.sleep(0.5)  # To avoid hitting universalis rate limit
        except Exception as e:
            st.error("No response from Universalis.app - please try again")
            st.stop()

        # Unpivot and unnest json data
        data = response.json()["items"]
        df = pl.DataFrame(data).unpivot(variable_name="id").unnest("value")
        df = df.explode("listings").unnest("listings")

        # Filter out Mannquin items (irrelevant listings)
        df = df.filter(
            (pl.col("onMannequin") == False) | pl.col("onMannequin").is_null()
        )
        
        # Add worldname if missing (for single world queries)
        if region in world_list:
            df = df.with_columns((pl.lit(region)).alias("worldName"))

        # Find minimum price for each group, then merge NQ/HQ data together

        df = df.group_by("id").min()
        
        if hq:
            hq_df = df.select(
                pl.col("id"),
                pl.col("pricePerUnit").alias("hq_price"),
                pl.col("hqSaleVelocity").round(2).alias("hq_velocity"),
                pl.col("worldName").alias("hq_world")
            )
        elif not hq:
            nq_df = df.select(
                pl.col("id"),
                pl.col("pricePerUnit").alias("nq_price"),
                pl.col("nqSaleVelocity").round(2).alias("nq_velocity"),
                pl.col("worldName").alias("nq_world")
            )
        

    prices_df = nq_df.join(hq_df, on="id").sort("id").rename({"id":"item_id"})
    prices_df = prices_df.with_columns(pl.col("item_id").cast(pl.Int64))
    return prices_df

@st.fragment
def print_result(buy_result_df: pl.DataFrame, sell_result_df: pl.DataFrame, craft_cost_total: int) -> int:

    # Initialise variables from result df
    name = buy_result_df.item(0, "item_name")
    id = buy_result_df.item(0, "item_id")
    amount = buy_result_df.item(0, "item_amount")
    shop_price_each = buy_result_df.item(0, "shop_price")
    if shop_price_each is not None:
        shop_price_total = shop_price_each * amount

    nq_buy_price_each = buy_result_df.item(0, "nq_price")
    if nq_buy_price_each is not None:
        nq_buy_price_total = nq_buy_price_each * amount
    nq_buy_velocity = buy_result_df.item(0, "nq_velocity")
    nq_buy_world = buy_result_df.item(0, "nq_world")

    hq_buy_price_each = buy_result_df.item(0, "hq_price")
    if hq_buy_price_each is not None:
        hq_buy_price_total = hq_buy_price_each * amount
    hq_buy_velocity = buy_result_df.item(0, "hq_velocity")
    hq_buy_world = buy_result_df.item(0, "hq_world")

    nq_sell_price_each = sell_result_df.item(0, "nq_price")
    if nq_sell_price_each is not None:
        nq_sell_price_total = nq_sell_price_each * amount
    nq_sell_velocity = sell_result_df.item(0, "nq_velocity")
    nq_sell_world = sell_result_df.item(0, "nq_world")

    hq_sell_price_each = sell_result_df.item(0, "hq_price")
    if hq_sell_price_each is not None:
        hq_sell_price_total = hq_sell_price_each * amount
    hq_sell_velocity = sell_result_df.item(0, "hq_velocity")
    hq_sell_world = sell_result_df.item(0, "hq_world")

    icon_url = make_icon_url(buy_result_df.item(0, "item_icon"))
    craft_cost_each = int(craft_cost_total / amount)
    cheapest_buy_source = buy_result_df.item(0, "cheapest_source")
    cheapest_buy_source_print = buy_result_df.with_columns(pl.col("cheapest_source").replace({"shop_price":"Shop","nq_price":"NQ","hq_price":"HQ"}).alias("cheapest_source")).item(0, "cheapest_source")

    


    
    # cheapest_buy_source_print = buy_result_df.with_columns(pl.col("cheapest_source").replace({"shop_price":"Shop","nq_price":"NQ","hq_price":"HQ"}).alias("cheapest_source"))
    
    st.markdown("## Craft Details")
    st.space(size="small")
    st.markdown(f"### ![{name}]({icon_url}) {name} ({str(id)}): {amount} per craft")
    st.space(size="medium")
    
    ## Create grid for result item

    if amount == 1:
        st.metric(f"Craft Cost (sum of ingredient costs)", f"{craft_cost_each:,} gil")
    elif amount > 1:
        st.metric(
            f"Craft Cost (sum of ingredient costs)", f"{craft_cost_total:,} gil ({craft_cost_each:,} gil each)"
        )
    st.space(size="small")

    # if cheapest_buy_source == "shop_price":
    #     st.markdown("#### :green[Shop Buy Price]")
    # else:
    #     st.markdown("#### Shop Buy Price")
    # if shop_price_each is None:
    #     st.write(":red[N/A  \n(Not sold in shop)]")
    # else:    
    #     if amount == 1:
    #         st.write(f"{shop_price_each:,} gil")
    #     elif amount > 1:
    #         st.write(f"Shop Buy", f"{shop_price_each * amount:,} gil ({shop_price_each:,} gil each)")

    # Create grid for data
    result_grid = {}
    for x in range(4):
        y = st.columns(2, border=True, gap=None)
        for y, col in enumerate(y):
            coord = (x, y)
            tile = col.container()
            result_grid[coord] = tile
    

    # Populate header row with column names

    # result_grid[(row, 2)].markdown("#### NQ Sell Price")
    # result_grid[(row, 4)].markdown("#### HQ Sell Price")

    # Populate second row with result item data
    row = 3
    with result_grid[(row, 0)]:
        # if cheapest_buy_source == "nq_price":
        #     st.markdown("#### :green[NQ Buy Price]")
        # else:
        st.markdown("#### NQ Buy Price")
        if nq_buy_price_each is None:
            st.write(":red[N/A  \n(No NQ available)]")
        else:
            if amount == 1:
                st.write(f"{nq_buy_price_each:,} gil @ {nq_buy_world}")
            elif amount > 1:
                st.write(f"{nq_buy_price_each * amount:,} gil ({nq_buy_price_each:,} gil each @ {nq_buy_world})")
            st.write(f"Velocity: {nq_buy_velocity:,.2f}/day")
    with result_grid[(row, 1)]:
        with st.container():
            if amount == 1:
                savings = nq_buy_price_each - craft_cost_each
                savings_perc = savings / nq_buy_price_each
            elif amount > 1:
                savings = nq_buy_price_total - craft_cost_total
                savings_perc = savings / craft_cost_each
            st.metric(
            f"Amount saved crafting vs buying NQ",
            f"{savings:,} gil",
            f"{savings_perc:.2%}"
            )

    
    # st.markdown(f"#### Cheapest buying option: :green[{cheapest_buy_source_print}]")


    row = 1

    with result_grid[(row, 0)]:
        # if cheapest_buy_source == "hq_price":
        #     st.markdown("#### :green[HQ Buy Price]")
        # else:
        st.markdown("#### HQ Buy Price")
        if hq_buy_price_each is None:
            st.write(":red[N/A  \n(No HQ available)]")
        else:
            if amount == 1:
                st.write(f"{hq_buy_price_each:,} gil @ {hq_buy_world}")
            elif amount > 1:
                st.write(f"{hq_buy_price_each * amount:,} gil ({hq_buy_price_each:,} gil each @ {hq_buy_world})")
            st.write(f"Velocity: {hq_buy_velocity:,.2f}/day")
    with result_grid[(row, 1)]:
        with st.container():
            if amount == 1:
                savings = hq_buy_price_each - craft_cost_each
                savings_perc = savings / hq_buy_price_each
            elif amount > 1:
                savings = hq_buy_price_total - craft_cost_total
                savings_perc = savings / craft_cost_each
            st.metric(
            f"Amount saved crafting vs buying HQ",
            f"{savings:,} gil",
            f"{savings_perc:.2%}"
            )
    
    
    row = 2
    with result_grid[(row, 0)]:
        st.markdown("#### NQ Sell Price")
        if nq_sell_price_each is None:
            st.write(":red[N/A  \n(No NQ available)]")
        else:
            if amount == 1:
                st.write(f"{nq_sell_price_each:,} gil @ {nq_sell_world}")
            elif amount > 1:
                st.write(f"{nq_sell_price_each * amount:,} gil ({nq_sell_price_each:,} gil each @ {nq_sell_world})")
            st.write(f"Velocity: {nq_sell_velocity:,.2f}/day")
    with result_grid[(row, 1)]:
        if nq_sell_price_each is None:
            st.metric(
            f"Profit made by crafting and selling HQ",
            f"N/A"
            )
        else:
            if amount == 1:
                profit = nq_sell_price_each - craft_cost_each
                profit_perc = profit / craft_cost_each
            elif amount > 1:
                profit = nq_sell_price_total - craft_cost_total
                profit_perc = profit / craft_cost_total
            st.metric(
                f"Profit made by crafting and selling NQ",
                f"{profit:,} gil",
                f"{profit_perc:.2%}"
                )

    row = 0
    with result_grid[(row, 0)]:
        st.markdown("#### HQ Sell Price")
        if hq_sell_price_each is None:
            st.write(":red[N/A  \n(No HQ available)]")
        else:
            if amount == 1:
                st.write(f"{hq_sell_price_each:,} gil @ {hq_sell_world}")
            elif amount > 1:
                st.write(f"{hq_sell_price_each * amount:,} gil ({hq_sell_price_each:,} gil each @ {hq_sell_world})")
            st.write(f"Velocity: {hq_sell_velocity:,.2f}/day")
    with result_grid[(row, 1)]:
        if hq_sell_price_each is None:
            st.metric(
            f"Profit made by crafting and selling HQ",
            f"N/A"
            )
        else:
            if amount == 1:
                profit = hq_sell_price_each - craft_cost_each
                profit_perc = profit / craft_cost_each
            elif amount > 1:
                profit = hq_sell_price_total - craft_cost_total
                profit_perc = profit / craft_cost_total
            st.metric(
                f"Profit made by crafting and selling HQ",
                f"{profit:,} gil",
                f"{profit_perc:.2%}"
                )



@st.fragment
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

@st.fragment
def print_velocity_warning(velocity: int) -> None:
    ## Display advice based on velocity
    if velocity is None:
        return
    elif velocity < 15:
        st.error(f"&nbsp; Item won't sell: average {velocity:,.2f} sold/day", icon="🔥")
    elif velocity < 99:
        st.warning(f"&nbsp; Item will sell really slowly: average {velocity:,.2f} sold/day", icon="🚨")
    else:
        st.success(f"&nbsp; Item will sell: average {velocity:,.2f} sold/day", icon="🥳")


@st.fragment
def print_ingredients(buy_price_df: pl.DataFrame, sell_price_df: pl.DataFrame):
    ## List ingredient data in a grid for clarity
    
    # Split data into result & ingredient dfs
    buy_result_df = buy_price_df.filter(pl.col("recipe_part") == "result")
    buy_ingr_df = buy_price_df.filter(pl.col("recipe_part").str.contains("ingredient"))
    sell_result_df = sell_price_df.filter(pl.col("recipe_part") == "result")
    
    # Create header for section
    st.markdown("")
    st.markdown("# Ingredients")
    st.text(
        "All items are set to cheapest source by default, but can be adjusted; cost will update dynamically.\n"
        "When increasing amounts, make sure to decrease other columns as this is not automatic."
    )
    st.space(size="small")

    ### Create blank grid for items
    ingr_grid = {}
    for x in range(len(buy_ingr_df) + 1):
        y = st.columns(6, border=True, gap=None)
        for y, col in enumerate(y):
            coord = (x, y)
            tile = col.container()
            ingr_grid[coord] = tile

    
    # Populate grid header row with column names
    row = 0
    ingr_grid[(row, 0)].markdown("#### Ingredient",
                                 help="Number below item name is item ID; click link to lookup item profit/loss of subcraft (only for craftable items)")
    ingr_grid[(row, 1)].markdown("#### Required")
    ingr_grid[(row, 2)].markdown("#### Shop price")
    ingr_grid[(row, 3)].markdown("#### NQ")
    ingr_grid[(row, 4)].markdown("#### HQ")
    ingr_grid[(row, 5)].markdown("#### Cost")

    ## Populate grid using data from ingredient df

    # Initialise variables from ingredient df
    craft_cost_total = 0
    
    for row in range(1, len(buy_ingr_df) + 1):
        row_cost = 0
        index = row - 1
        name = buy_ingr_df.item(index, "item_name")
        id = buy_ingr_df.item(index, "item_id")
        amount = buy_ingr_df.item(index, "item_amount")
        shop_price = buy_ingr_df.item(index, "shop_price")
        nq_price = buy_ingr_df.item(index, "nq_price")
        nq_velocity = buy_ingr_df.item(index, "nq_velocity")
        nq_world = buy_ingr_df.item(index, "nq_world")
        hq_price = buy_ingr_df.item(index, "hq_price")
        hq_velocity = buy_ingr_df.item(index, "hq_velocity")
        hq_world = buy_ingr_df.item(index, "hq_world")
        
        shop_amount, nq_amount, hq_amount = 0, 0, 0
        match buy_ingr_df.item(index, "cheapest_source"):
            case "shop_price":
                shop_amount = amount
            case "nq_price":
                nq_amount = amount
            case "hq_price":
                hq_amount = amount
        icon_url = make_icon_url(buy_price_df.item(index, "item_icon"))


        # Ingredient name column
        with ingr_grid[(row, 0)]:
            if int(id) in results_df["item_id"]:
                st.markdown(f"![{name}]({icon_url}) {name} ([{id}](/?dc={st.session_state.dc}&world={st.session_state.world}id={id}))")
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
                    value=shop_amount,
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
                    value=nq_amount,
                    key=f"{id}_nq_qty",
                    label_visibility="hidden",
                )
                nq_total = nq_price * nq_qty
                st.write(f"{nq_total:,} gil ({nq_price:,} gil each @ {nq_world})")
                st.write(f"Velocity: {nq_velocity:,.2f}/day")
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
                    value=hq_amount,
                    key=f"{id}_hq_qty",
                    label_visibility="hidden",
                )
                hq_total = hq_price * hq_qty
                st.write(f"{hq_total:,} gil ({hq_price:,} gil each @ {hq_world})")
                st.write(f"Velocity: {hq_velocity:,.2f}/day")
                row_cost += hq_total

        # Ingredient total cost (per ingredient) column
        ingr_grid[(row, 5)].write(f"{row_cost:,}")
        


        # Calculate and display ingredient total cost (all ingredients) row
        craft_cost_total += row_cost
    result_amount = buy_result_df.item(0, "item_amount")
    nq_velocity = buy_result_df.item(0, "nq_velocity")
    hq_velocity = buy_result_df.item(0, "hq_velocity")

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
        with cont_result:
            print_result(buy_result_df, sell_result_df, craft_cost_total)
        # with cont_pl_warning:
        #     print_pl_warning(pl_perc)
        with cont_velocity_warning:
            print_velocity_warning(total_velocity)

    return


def make_icon_url(icon: int) -> str:
    # GET icon image from XIVAPI using icon ID
    folder = f"{icon:0>6}"
    folder = folder[:3] + "000"
    icon_url = f"https://v2.xivapi.com/api/asset?path=ui/icon/{folder}/{icon:0>6}.tex&format=png"

    return icon_url



def update_params():
    if not st.session_state.dc == dc_selectbox:
        st.session_state.dc = dc_selectbox
    if not st.session_state.world == world_selectbox:
        st.session_state.world = world_selectbox
    if not st.session_state.item == item_id:
        st.session_state.item = item_id


def initialize_params():
    # Initialise page params
    if "dc" not in st.session_state:
        try:
            st.session_state.dc = st.query_params.dc
        except:
            st.session_state.dc = "Mana"
    if "world" not in st.session_state:
        try:
            st.session_state.world = st.query_params.world
        except:
            st.session_state.world = None
    if "item" not in st.session_state:
        try:
            st.session_state.item = st.query_params.item
        except:
            st.session_state.item = None

    for param, value in st.session_state.items():
        try:
            if value.lower() == "none":
                st.session_state[param] = None
        except:
            pass


def join_item_price_dfs(items_to_lookup_df, dc_market_prices_df):
    df = items_to_lookup_df.join(dc_market_prices_df, on="item_id", how="left").with_row_index()        
    df = df.with_columns(cheapest=pl.min_horizontal(
"shop_price",
        "nq_price",
        "hq_price"
    ))
    
    cheapest_source_df = df.select(pl.col("item_id", "shop_price","nq_price","nq_world","hq_price","hq_world"))
    # Prioritise HQ > Shop > NQ if multiple sources have the same price; order of this unpivot matters
    cheapest_source_df = cheapest_source_df.unpivot(on=["hq_price","shop_price","nq_price",],index="item_id",variable_name="source", value_name="price")
    cheapest_source_df = cheapest_source_df.sort(["item_id", "price"]).drop_nulls().unique("item_id", keep="first")
    df = df.join(cheapest_source_df.select(pl.col("item_id", "source")), on="item_id", how="left")
    df = df.rename({"source":"cheapest_source"})
    
    return df

if __name__ == "__main__":
    st.set_page_config(layout="wide", page_title="FFXIV Crafting Profit Calculator")

    
    

    

    # Initialise data centre and world dfs/lists
    initialize_params()
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
            label="Select datacenter where buying ingredients", options=dc_list,
            index=[dc.lower() for dc in dc_list].index(st.session_state.dc.lower()))
        
        world_list = filter_world(world_list, dc_selectbox)

        
        def world_selectbox_index() -> int | None:
        # Converts "world" query parameter to index used in selectbox
            try:
                index = [world.lower() for world in world_list].index(st.session_state.world.lower())
            except:
                index = None
            return index

        world_selectbox = st.selectbox(
            "Select world where selling items (optional)", world_list, index=world_selectbox_index(),
            help="Will calculate based on cheapest prices in datacentre if not selected", width=200)

        if st.session_state.world is None:
            st.session_state.same_world_buy = False
        else:
            st.checkbox("Buy ingredients on same world (no world travel)", value=False, key="same_world_buy")

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

    def item_selectbox_index() -> int | None:
    # Converts "item" query parameter to index used in selectbox
        try:
            item_id = int(st.session_state.item)
            index = recipe_selectbox_df["item_id"].to_list().index(item_id)
        except:
            index = None
        return index

    item_selectbox = st.selectbox(
        label="Select recipe (number in parentheses is item id)",
        options=recipe_selectbox_df["selectbox_label"],
        index=item_selectbox_index())
    


    # Create elements and manipulate data that are loaded once item has been selected
    if item_selectbox:
        # with st.spinner("Fetching data from Universalis"):
        item_id = recipe_selectbox_df.filter(
            pl.col("selectbox_label") == item_selectbox
        ).select("item_id").item()

        recipe_id = recipe_selectbox_df.filter(
            pl.col("selectbox_label") == item_selectbox
        ).select("recipe_id").item()

        
        st.set_page_config(layout="wide", page_title=item_selectbox)


        # Create empy containers that will display information
        
        cont_analysis = st.empty()
        cont_pl_warning = st.empty()
        cont_velocity_warning = st.empty()
        cont_result = st.empty()
        cont_ingr = st.container()


        # Prepare data needed for Universalis API GET
        items_to_lookup_df = all_recipes_df.filter(pl.col("recipe_id") == recipe_id)
        
        # Buy from datacentre if travel is allowed (i.e. same world buy = False), otherwise limit buy to same world
        if not st.session_state.same_world_buy:
            buy_price_df = get_prices_from_universalis(items_to_lookup_df, st.session_state.dc)
        else:
            buy_price_df = get_prices_from_universalis(items_to_lookup_df, st.session_state.world)
        # Combine item data with market data
        buy_price_df = join_item_price_dfs(items_to_lookup_df, buy_price_df)
        
        # Sell only from specified world if selected, otherwise sell on whole datacenter
        if st.session_state.world is not None:
            sell_price_df = get_prices_from_universalis(items_to_lookup_df, st.session_state.world)
            sell_price_df = join_item_price_dfs(items_to_lookup_df, sell_price_df)
        else:
            sell_price_df = buy_price_df

        # Fill containers with content from output_df; output of several containers nested inside print_ingredients()
        with cont_ingr:
            print_ingredients(buy_price_df, sell_price_df)


# Update params
    try:
        _ = item_id
    except:
        item_id = None
    # # Update session state, page URL and page title after dc is selected
    if not st.session_state.dc == dc_selectbox:
        update_params()
        st.switch_page(home_page, query_params={"dc":st.session_state.dc, "world": st.session_state.world, "item":st.session_state.item})
    if not st.session_state.world == world_selectbox:
        update_params()
        st.switch_page(home_page, query_params={"dc":st.session_state.dc, "world": st.session_state.world, "item":st.session_state.item})
    if not st.session_state.item == item_id:
        update_params()
        st.switch_page(home_page, query_params={"dc":st.session_state.dc, "world": st.session_state.world, "item":st.session_state.item})
