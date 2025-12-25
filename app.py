"""
TODO
- Make thresholds adjustable in UI
- Add item source, e.g. currency if vendor; SpecialShop.csv; nontrivial effort
- Add Japanese language support; not sure where source is
- Support recursive crafts (subcrafts); not sure how to implement
"""

import duckdb
import requests
import polars as pl
import streamlit as st
import time
from dataclasses import dataclass

### Configuration variables
DB_NAME = "ffxiv_price.duckdb"
home_page = st.Page("app.py", default=True)
<<<<<<< HEAD
default_profit_goal = 0.25  # Minimum profit % to show "good profit" message
=======
default_profit_goal = 25  # Minimum profit % to show "good profit" message
>>>>>>> 9dc21862c226370a797634af5e99851482bec7ba
default_velocity_warning = 15  # Minimum velocity to show "good sell" message
default_velocity_goal = 40  # Minimum velocity to show "good sell" message


@st.cache_resource(show_spinner=False)
def get_worlds_dc() -> pl.DataFrame:
    # Read world & dc data from local duckdb
    with duckdb.connect(DB_NAME) as con:
        query = """SELECT * from  world_dc"""
        df = con.sql(query).pl()
    return df


@st.cache_resource(show_spinner=False)
def get_all_recipes() -> pl.DataFrame:
    # Read recipe data from local duckdb
    with duckdb.connect(DB_NAME) as con:
        query = """SELECT * from  recipe_price"""
        df = con.sql(query).pl()
    results_df = df.filter(pl.col("recipe_part") == "result")

    # Concat item_id to the end of item_name to make selectbox easily searchable
    # Some items can be crafted by two jobs (ARM/BSM) with slightly different recipes, so appending job name to the end as well
    two_job_craftable = results_df.filter(pl.col("item_id").is_duplicated())

    df = df.lazy().with_columns(
        pl.when(pl.col("recipe_id").is_in(two_job_craftable["recipe_id"].implode()))
        .then(pl.concat_str([pl.col("item_name"), pl.lit(" ("), pl.col("item_id"), pl.lit(")"), pl.lit(" ("), pl.col("job"), pl.lit(")")]))
        .otherwise(pl.concat_str([pl.col("item_name"), pl.lit(" ("), pl.col("item_id"), pl.lit(")")]))
        .alias("selectbox_label")
    ).collect()

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
        
        # Use shared requests session with retries/backoff
        session = get_requests_session()
        try:
            response_json = fetch_universalis(session, url, parameters)
        except Exception:
            st.error("No response from Universalis.app - please try again")
            st.stop()

        # Unpivot and unnest json data
        data = response_json["items"]
        df = pl.DataFrame(data).lazy().unpivot(variable_name="id").unnest("value")
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
    prices_df = prices_df.with_columns(pl.col("item_id").cast(pl.Int64)).collect()


    # Join data from universalis lookup onto exist data from local duckdb
    df = lookup_items_df.lazy().join(prices_df.lazy(), on="item_id", how="left")
    df = df.with_columns(pl.min_horizontal("shop_price", "nq_price", "hq_price").alias("cheapest"))

    # Aggregate to find cheapest source for each item
    cheapest_source_df = (
        df.select(pl.col("item_id", "shop_price", "nq_price", "nq_world", "hq_price", "hq_world"))
        .unpivot(on=["hq_price", "shop_price", "nq_price"], index="item_id", variable_name="source", value_name="price")
        .sort(["item_id", "price"]).drop_nulls().unique("item_id", keep="first")
    )

    df = df.join(cheapest_source_df.select(pl.col("item_id", "source")), on="item_id", how="left")
    df = df.collect()
    if "source" in df.columns:
        df = df.rename({"source": "cheapest_source"})

    return df


@st.cache_resource(show_spinner=False)
def get_requests_session() -> requests.Session:
    session = requests.Session()
    try:
        from urllib3.util import Retry
        from requests.adapters import HTTPAdapter
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504))
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
    except Exception:
        pass
    return session


def fetch_universalis(session: requests.Session, url: str, params: dict) -> dict:
    resp = session.get(url, params=params, timeout=10)
    resp.raise_for_status()
    time.sleep(0.2)
    return resp.json()


def format_gil(price: int | float) -> str:
    return f"{price:,} gil"
    
def format_velocity(velocity: int | float) -> str:
    return f"Velocity: {velocity:,.2f}/day"


@dataclass
class Item:
    name: str
    item_id: int
    amount: int
    shop_price: float | None
    nq_price: float | None
    nq_velocity: float | None
    nq_world: str | None
    hq_price: float | None
    hq_velocity: float | None
    hq_world: str | None
    cheapest_source: str | None
    icon_url: str


def extract_Item_from_df(df: pl.DataFrame, index: int) -> Item:
    name = df.item(index, "item_name")
    item_id = df.item(index, "item_id")
    amount = df.item(index, "item_amount")
    shop_price = df.item(index, "shop_price")
    nq_price = df.item(index, "nq_price")
    nq_velocity = df.item(index, "nq_velocity")
    nq_world = df.item(index, "nq_world")
    hq_price = df.item(index, "hq_price")
    hq_velocity = df.item(index, "hq_velocity")
    hq_world = df.item(index, "hq_world")
    cheapest = df.item(index, "cheapest_source") if "cheapest_source" in df.columns else None
    icon_url = make_icon_url(df.item(index, "item_icon"))
    return Item(name, item_id, amount, shop_price, nq_price, nq_velocity, nq_world, hq_price, hq_velocity, hq_world, cheapest, icon_url)

@st.fragment
def print_result(buy_result_df: pl.DataFrame, sell_result_df: pl.DataFrame, craft_cost_total: int) -> int:

    # Extract fields using helper dataclasses
    buy = extract_Item_from_df(buy_result_df, 0)
    sell = extract_Item_from_df(sell_result_df, 0)

    name = buy.name
    id = buy.item_id
    amount = buy.amount

    if st.session_state.nq_craft == False:
        type = "HQ"
        buy_price_each = buy.hq_price
        buy_velocity = buy.hq_velocity
        buy_world = buy.hq_world    
        sell_price_each = sell.hq_price
        sell_velocity = sell.hq_velocity
        sell_world = sell.hq_world
    else:
        type = "NQ"
        buy_price_each = buy.nq_price
        buy_velocity = buy.nq_velocity
        buy_world = buy.nq_world
        sell_price_each = sell.nq_price
        sell_velocity = sell.nq_velocity
        sell_world = sell.nq_world

    icon_url = buy.icon_url
    craft_cost_each = int(craft_cost_total / amount) if amount else 0
    
    st.markdown("## Craft Details")
    st.space(size="small")
    st.markdown(f"### ![{name}]({icon_url}) {name} ({str(id)}): {amount} per craft")
    st.space(size="medium")
    
    ## Create grid for result item

    if amount == 1:
        st.metric(f"Craft Cost (sum of ingredient costs)", f"{format_gil(craft_cost_each)}")
    elif amount > 1:
        st.metric(
            f"Craft Cost (sum of ingredient costs)", f"{format_gil(craft_cost_total)} ({format_gil(craft_cost_each)} each)"
        )
    st.space(size="small")

    # Create grid for data
    result_grid = create_grid(2, 3)


    row = 0
    with result_grid[(row, 0)]:
        print_result_price(title=f"{type} Sell Price", type = type, amount=amount, 
                         price=sell_price_each, velocity=sell_velocity, world=sell_world)
    with result_grid[(row, 1)]:
        profit_perc = print_result_metric(title = "Profit made by crafting and selling HQ", craft_cost_total=craft_cost_total, amount=amount,
                        price_each=sell_price_each)
    with result_grid[(row, 2)]:
        sell_recommend(profit_perc, sell_velocity)
        
            
    row = 1

    with result_grid[(row, 0)]:
        print_result_price(title=f"{type} Buy Price", type = type,amount=amount, 
                         price=buy_price_each, velocity=buy_velocity, world=buy_world)

    with result_grid[(row, 1)]:
        with st.container():
            profit_perc = print_result_metric(title = "Amount saved crafting vs buying HQ", craft_cost_total=craft_cost_total, amount=amount,
                                    price_each=buy_price_each)
    with result_grid[(row, 2)]:
        buy_recommend(profit_perc)
    


def create_grid(rows,cols) -> dict:
    result_grid = {}
    for x in range(rows):
        y = st.columns(cols, border=True, gap=None)
        for y, col in enumerate(y):
            coord = (x, y)
            tile = col.container()
            result_grid[coord] = tile
    return result_grid

def print_result_metric(title, craft_cost_total: int, amount: int, price_each: int) -> int:
    
    craft_cost_each = int(craft_cost_total / amount)
    if price_each is None:
        st.metric(
            f"{title}",
            f"N/A"
            )
        profit_perc = None
    else:
        if amount == 1:
            profit = price_each - craft_cost_each
            profit_perc = profit / craft_cost_each
        elif amount > 1:
            price_total = price_each * amount
            profit = price_total - craft_cost_total
            profit_perc = profit / craft_cost_total
        st.metric(
                f"{title}",
                f"{format_gil(profit)}",
                f"{profit_perc:.2%}"
                )
    return profit_perc

def print_result_price(title: str, type: str, amount: int, price: int, velocity: int, world: str):
    st.markdown(f"#### {title}")
    if price is None:
        st.write(f":red[N/A  \n(No {type} available)]")
    else:
        if amount == 1:
            st.write(f"{format_gil(price)} @ {world}")
        elif amount > 1:
            st.write(f"{format_gil(price * amount)} {format_gil(price)}) each @ {world})")
        st.write(format_velocity(velocity))

def sell_recommend(profit_perc, sell_velocity):
    if profit_perc is None:
        st.markdown("### :red[Don't craft to sell!]")
        st.error(f"&nbsp; Unable to calculate profit as no data", icon="🔥")
        return
    if profit_perc > st.session_state.get("profit_goal") and sell_velocity > st.session_state.get("velocity_goal"):
        st.markdown("### :green[Craft to sell!]")
    else:
        st.markdown("### :red[Don't craft to sell!]")
    if profit_perc < 0:
        st.error(
                f"&nbsp; Crafting to sell will result in a loss: {profit_perc:,.2%}",
                icon="🔥",
            )
    elif profit_perc < st.session_state.get("profit_goal"):
<<<<<<< HEAD
        st.warning(f"&nbsp; Low profit margin (below {st.session_state.get("profit_goal"):,.2%}): {profit_perc:,.2%}", icon="🚨")
=======
        st.warning(f"&nbsp; Low profit margin (below {st.session_state.get("profit_goal")}%): {profit_perc:,.2%}", icon="🚨")
>>>>>>> 9dc21862c226370a797634af5e99851482bec7ba
    else:
        st.success(f"&nbsp; Profit above {st.session_state.get("profit_goal"):,.2%}): {profit_perc:,.2%}", icon="🥳")
    if sell_velocity is None:
        return
    elif sell_velocity < default_velocity_warning:
        st.error(f"&nbsp; Item won't sell: average {sell_velocity:,.2f} sold/day", icon="🔥")
    elif sell_velocity < st.session_state.get("velocity_goal"):
        st.warning(f"&nbsp; Item will sell slowly: average {sell_velocity:,.2f} sold/day", icon="🚨")
    else:
        st.success(f"&nbsp; Item will sell: average {sell_velocity:,.2f} sold/day", icon="🥳")

def buy_recommend(profit_perc):
    if profit_perc is None:
        st.markdown("### :red[Don't craft to use!]")
        st.error(f"&nbsp; Unable to calculate savings as no data", icon="🔥")
        return
    if profit_perc > st.session_state.get("profit_goal"):
        st.markdown("### :green[Craft to use!]")
    else:
        st.markdown("### :red[Don't craft to use!]")
    if profit_perc < 0:
        st.error(
                f"&nbsp; Crafting to sell will result in a loss: {profit_perc:,.2%}",
                icon="🔥",
            )
    elif profit_perc < st.session_state.get("profit_goal"):
<<<<<<< HEAD
        st.warning(f"&nbsp; Low profit margin (below {st.session_state.get("profit_goal"):,.2%}): {profit_perc:,.2%}", icon="🚨")
=======
        st.warning(f"&nbsp; Low profit margin (below {st.session_state.get("profit_goal")}%): {profit_perc:,.2%}", icon="🚨")
>>>>>>> 9dc21862c226370a797634af5e99851482bec7ba
    else:
        st.success(f"&nbsp; Profit above {st.session_state.get("profit_goal"):,.2%}): {profit_perc:,.2%}", icon="🥳")

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
    ingr_grid = create_grid(len(buy_ingr_df) + 1, 6)

    
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
        ingr = extract_Item_from_df(buy_ingr_df, index)
        name = ingr.name
        id = ingr.item_id
        amount = ingr.amount
        shop_price = ingr.shop_price
        nq_price = ingr.nq_price
        nq_velocity = ingr.nq_velocity
        nq_world = ingr.nq_world
        hq_price = ingr.hq_price
        hq_velocity = ingr.hq_velocity
        hq_world = ingr.hq_world

        shop_amount, nq_amount, hq_amount = 0, 0, 0
        match ingr.cheapest_source:
            case "shop_price":
                shop_amount = amount
            case "nq_price":
                nq_amount = amount
            case "hq_price":
                hq_amount = amount
        icon_url = ingr.icon_url


        # Ingredient name column
        with ingr_grid[(row, 0)]:
            if int(id) in results_df["item_id"]:
                st.markdown(f"![{name}]({icon_url}) {name} ([{id}](/?dc={st.session_state.dc}&world={st.session_state.world}&item={id}))")
            else:
                st.markdown(f"![{name}]({icon_url}) {name} ({id})")

        # Ingredient amount column
        ingr_grid[(row, 1)].markdown(str(amount))

        # Ingredient shop quantity/price column
        with ingr_grid[(row, 2)]:
            cell_total = print_ingr_amount_input(id=id, source="shop", amount=amount, price=shop_price, default_value=shop_amount)
            row_cost += cell_total
        # Ingredient market NQ quantity/price column
        with ingr_grid[(row, 3)]:
            cell_total = print_ingr_amount_input(id=id, source="nq", world=nq_world, amount=amount, price=nq_price, default_value=nq_amount, velocity=nq_velocity)
            row_cost += cell_total
        
        # Ingredient market HQ quantity/price column
        with ingr_grid[(row, 4)]:
            cell_total = print_ingr_amount_input(id=id, source="hq", world=hq_world, amount=amount, price=hq_price, default_value=hq_amount, velocity=hq_velocity)
            row_cost += cell_total

        # Ingredient total cost (per ingredient) column
        ingr_grid[(row, 5)].write(f"{format_gil(row_cost)}")
        


        # Calculate and display ingredient total cost (all ingredients) row
        craft_cost_total += row_cost
    result_amount = buy_result_df.item(0, "item_amount")
    nq_velocity = buy_result_df.item(0, "nq_velocity")
    hq_velocity = buy_result_df.item(0, "hq_velocity")

    total_velocity = nq_velocity + hq_velocity
    craft_cost_each = int(craft_cost_total / result_amount)

    if result_amount > 1:
        st.write(
            f"#### Total ingredient cost per craftable amount ({result_amount}): :red[{format_gil(craft_cost_total)}]"
        )
    st.write(f"#### Total ingredient cost: :red[{format_gil(craft_cost_each)} each]")


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

    return

def print_ingr_amount_input(id: int, source: str,  amount: int, price: int, default_value: int, velocity: float =0, world: str|None = None) -> int:
    if price is None:
        match source:
            case "shop":
                st.write(":red[N/A  \n(Not sold in shop)]")
            case "nq":
                st.write(":red[N/A  \n(No NQ available)]")
            case "hq":
                st.write(":red[N/A  \n(No HQ available)]")
        return 0
    else:
        shop_qty = st.number_input(
                    label=f"num_{source}",
                    min_value=0,
                    max_value=amount,
                    value=default_value,
                    key=f"{id}_{source}_qty",
                    label_visibility="hidden",
                )
        cost = price * shop_qty
        if source == "shop":
            st.write(f"{format_gil(cost)} ({format_gil(price)} each)")
        else:
            st.write(f"{format_gil(cost)} ({format_gil(price)} each @ {world})")
        if source != "shop":
            st.write(format_velocity(velocity))
        return cost
        


def make_icon_url(icon: int) -> str:
    # GET icon image from XIVAPI using icon ID
    folder = f"{icon:0>6}"
    folder = folder[:3] + "000"
    icon_url = f"https://v2.xivapi.com/api/asset?path=ui/icon/{folder}/{icon:0>6}.tex&format=png"

    return icon_url



def sync_params_and_redirect(changed: bool = False):
    # Synchronize `session_state with current UI selections and redirect if changed.
    
    if st.session_state.get("dc") != dc_selectbox:
        st.session_state["dc"] = dc_selectbox
        st.switch_page(home_page, query_params={"dc": st.session_state.get("dc"), "world": st.session_state.get("world"), "item": st.session_state.get("item")})
        changed = True
    if st.session_state.get("world") != world_selectbox:
        st.session_state["world"] = world_selectbox
        changed = True
    
    # Already marked changed = True outside the function for item; this is just a placeholder
    if st.session_state.get("item"):
        pass 
    
    if changed:
        st.switch_page(home_page, query_params={"dc": st.session_state.get("dc"), "world": st.session_state.get("world"), "item": st.session_state.get("item")})


def initialize_params():
    params = st.query_params
    # Initialise page params
    if "dc" not in st.session_state:
        st.session_state["dc"] = params.get("dc") or "Mana"
    if "world" not in st.session_state:
        st.session_state["world"] = params.get("world")
    if "item" not in st.session_state:
        st.session_state["item"] = params.get("item")
    if "profit_goal" not in st.session_state:
        st.session_state["profit_goal"] = default_profit_goal
    
    # Change "none" string values -> None
    for param in ("dc", "world", "item"):
        val = st.session_state.get(param)
        if isinstance(val, str) and val.lower() == "none":
            st.session_state[param] = None

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
        st.write(st.session_state.get("dc"))
        dc_selectbox = st.selectbox(
            label="Select datacenter where buying ingredients", options=dc_list,
            index=[dc.lower() for dc in dc_list].index(st.session_state.get("dc").lower()))
        
        world_list = filter_world(world_list, dc_selectbox)

        
        def world_selectbox_index() -> int | None:
        # Converts "world" query parameter to index used in selectbox
            try:
                index = [world.lower() for world in world_list].index(st.session_state.get("world").lower())
            except:
                index = None
            return index

        with st.container():
            world_selectbox = st.selectbox(
                "Select world where selling items (optional)", world_list, index=world_selectbox_index(),
                help="Will calculate based on cheapest prices in datacentre if not selected", width=200)

            if st.session_state.world is None:
                st.session_state.same_world_buy = False
            else:
                st.checkbox("Buy ingredients on same world (no world travel)", value=False, key="same_world_buy")
            st.space("stretch")
        st.checkbox("Only craft NQ items", value=False, help="Default setting assume crafters will always aim for HQ crafts. Check this if you are bulk crafting NQ items instead.", key="nq_craft")
<<<<<<< HEAD


        profit_goal_input = st.number_input("Low profit % warning threshold", min_value=0, value=int(default_profit_goal*100), step=1, help="Set this to determine what threshold low profit will flag at", format="%0f")
        if profit_goal_input:
            try:
                st.session_state.profit_goal = profit_goal_input / 100
            except:
                st.session_state.profit_goal = default_profit_goal
=======

        st.number_input("Low profit % warning threshold", min_value=0, value=int(default_profit_goal), key ="profit_goal", help="Set this to determine what threshold low profit will flag at")
>>>>>>> 9dc21862c226370a797634af5e99851482bec7ba
        st.number_input("Low velocity warning threshold", min_value=0, value=int(default_velocity_goal), key ="velocity_goal", help="Set this to determine what threshold low velocity will flag at")
        sync_params_and_redirect()

        

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
        item_id = st.session_state.get("item")
        index = recipe_selectbox_df["item_id"].to_list().index(int(item_id)) if item_id is not None else None
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
        
        if st.session_state["item"] != item_id:
            st.session_state["item"] = item_id
            sync_params_and_redirect(changed=True)

        recipe_id = recipe_selectbox_df.filter(
            pl.col("selectbox_label") == item_selectbox
        ).select("recipe_id").item()


        # Update page title with selected item name
        st.set_page_config(layout="wide", page_title=item_selectbox)


        # Create empy containers that will hold display information
        cont_result = st.empty()
        cont_ingr = st.empty()

        # Prepare data needed for Universalis API GET
        lookup_items_df = all_recipes_df.filter(pl.col("recipe_id") == recipe_id)
        
        # Buy from datacentre if travel is allowed (i.e. same world buy = False), otherwise limit buy to same world
        if not st.session_state.same_world_buy:
            buy_price_df = get_prices_from_universalis(lookup_items_df, st.session_state.dc)
        else:
            buy_price_df = get_prices_from_universalis(lookup_items_df, st.session_state.world)
        
        # Sell only from specified world if selected, otherwise sell on whole datacenter
        if st.session_state.get("world"):
            sell_price_df = get_prices_from_universalis(lookup_items_df, st.session_state.world)
        else:
            sell_price_df = buy_price_df

        # Fill containers with content from output_df; output of several containers nested inside print_ingredients()
        with cont_ingr:
            print_ingredients(buy_price_df, sell_price_df)