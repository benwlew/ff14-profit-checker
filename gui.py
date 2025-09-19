"""
TODO
- Handle multijob crafting
- Handle multiple servers
"""

from typing import List, Optional, Dict, Union
import duckdb
import requests
import polars as pl
import streamlit as st
import json

import config


def get_all_recipes() -> pl.DataFrame:
    with duckdb.connect(config.DB_NAME) as db:
        query = """SELECT *, CONCAT(result_name, ' (', result_id, ')') as result_text from  recipe_price"""
        df = db.sql(query).pl()
    return df


def get_recipe_items(item_id: int) -> list:
    # Get all item IDs to be passed to API request
    lookup_item_ids = []
    
    with duckdb.connect(config.DB_NAME) as db:
        query = f"""SELECT * from recipe_price where result_id = '{item_id}' """
        df = db.sql(query).pl()
        ingr_data = df.to_dicts()
        # st.write(ingr_data[0].items())

    ### TODO Make recursive
    for k, v in ingr_data[0].items():
        if 'id' in k and v != 0:
            lookup_item_ids.append(str(v))
    # lookup_item_ids
    return lookup_item_ids


def fetch_universalis(item_ids: list, region: str = "Japan") -> Optional[dict]:
    ###TODO Change logic to top 100 nq and top 100
    url = f"https://universalis.app/api/v2/{region}/{','.join(item_ids)}"

    raw_market_data={}
    
    
    for hq in [False,True]:
        parameters = {"hq": hq,
                "listings": 200,
                "fields": "items.nqSaleVelocity,items.hqSaleVelocity,items.listings.pricePerUnit,items.listings.onMannequin,listings.worldName"}
        response = requests.get(url, params = parameters)
        response.raise_for_status()
        if hq:
            raw_market_data["hq"] = response.json()
        else:
            raw_market_data["nq"] = response.json()
    
    # st.write(market_data["hq"])
    
    # Calculate cheapest listing, but skip mannequin listings
    # raw_market_data
    market_data = {"id":[],"nq_cheapest":[],"hq_cheapest":[],"nq_velocity": [], "hq_velocity":[]}
    for quality in raw_market_data:
        for item in raw_market_data[quality]["items"]:
            if item not in market_data["id"]:
                market_data["id"].append(item)
                market_data["nq_velocity"].append(int(raw_market_data[quality]["items"][item]["nqSaleVelocity"]))
                market_data["hq_velocity"].append(int(raw_market_data[quality]["items"][item]["hqSaleVelocity"]))
            
            min_listing = float('inf')
            for listing in raw_market_data[quality]["items"][item]["listings"]:
                if not listing["onMannequin"]: 
                    min_listing = min(min_listing, listing.get("pricePerUnit"))
            if min_listing == float('inf'):
                min_listing = None

            if quality == "nq":
                market_data["nq_cheapest"].append(min_listing)
            elif quality == "hq":
                market_data["hq_cheapest"].append(min_listing)
            
            
    market_data
    ### TODO LEFT JOIN market_data onto table
    

    # for item in market_data:
    #     # item_data = pl.from_dict(market_data[item])
    #     item_data = pl.json_normalize(market_data[item])
    #     item_data

    
    
    
    all_listings = [
        listing for listing in item_market.get('listings', [])
        if not listing.get('onMannequin', False)
    ]


    

    # Process market data        

        
        
    # Calculate market stats separating HQ and NQ
    market_stats = calculate_market_stats(all_listings)
    
    for i in ["nq","hq"]:
        if i in market_stats:
            details[i] = {
                'medianPrice': market_stats[i]['medianPrice'],
                'minPrice': market_stats[i]['minPrice'],
                'velocity': item_market.get(f'{i}SaleVelocity'),
            }


    
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
    


### TODO Make recursive
### TODO Add item images?
### Add item source, e.g. currency if vendor

def print_result(item_data: dict):
    v = item_data["result"]

    st.markdown("##")
    st.markdown("## Craft Details")

    prices = {"Shop": v.get("shop_price"),
            "Marketboard (NQ)": v.get("nq", {}).get("minPrice"),
            "Marketboard (HQ)": v.get("hq", {}).get("minPrice")}    
    prices = {source: price for source, price in prices.items() if price is not None}
    
    result_min_source = min(prices, key=prices.get) 
    result_min_price = prices[result_min_source]
    
    
    metric_col1, metric_col2, _, _, _, _ = st.columns(6)
    
    with metric_col1:
        with st.container(border=True):
            if v["amount"] == 1:
                st.metric(f"Cheapest: :blue[{result_min_source}]", f"{result_min_price}")
            elif v["amount"] > 1:
                st.metric(f"Cheapest: :blue[{result_min_source}]", f"{result_min_price} ({result_min_price * v["amount"]} total)")
    with metric_col2:
        with st.container(border=True):
            if v["hq"]:
                if v["amount"] == 1:
                    st.metric(f"Cheapest HQ", v["hq"]["minPrice"])
                if v["amount"] > 1:
                    st.metric(f"Cheapest HQ", f"{v["hq"]["minPrice"]} ({v["hq"]["minPrice"] * v["amount"]} total)")



    result_grid={}
    for x in range(2):
        y = st.columns(6)
        for y, col in enumerate(y):
            coord = (x,y)
            tile = col.container()
            result_grid[coord] = tile

    row = 0
    result_grid[(row,0)].markdown("##### Item")
    result_grid[(row,1)].markdown("##### ID:blue")
    result_grid[(row,2)].markdown("##### Number per craft:blue")
    result_grid[(row,3)].markdown("##### Shop price:blue")
    result_grid[(row,4)].markdown("##### NQ Price:blue")
    result_grid[(row,5)].markdown("##### HQ Price:blue")
    
    result_grid[(1,0)].write(v["name"])
    result_grid[(1,1)].write(f"{v["id"]}")
    with result_grid[(1,2)]:
        st.write(f"{v["amount"]}")
    with result_grid[(1,3)]:
        if v["shop_price"]:
            if v["amount"] == 1:
                st.write(f"{v["shop_price"]}")
            elif v["amount"] > 1:
                st.write(f"{v["shop_price"] * v["amount"]} ({v["shop_price"]} each)")
        else:
            st.write(":red[No Shop]")
    with result_grid[(1,4)]:
        if v["nq"]:
            if v["amount"] == 1:
                st.write(f"{v["nq"]["minPrice"]}")
            elif v["amount"] > 1:
                st.write(f"{v["nq"]["minPrice"] * v["amount"]} ({v["nq"]["minPrice"]} each)")
        else:
            st.write(":red[No NQ on Marketboard]")
    with result_grid[(1,5)]:
        if v["hq"]:
            if v["amount"] == 1:
                st.write(f"{v["hq"]["minPrice"]}")
            elif v["amount"] > 1:
                st.write(f"{v["hq"]["minPrice"] * v["amount"]} ({v["hq"]["minPrice"]} each)")
        else:
            st.write(":red[No HQ on Marketboard]")

    

    
    return(result_min_source, result_min_price)




@st.fragment
def print_ingredients(item_data: dict):
    st.markdown("#")
    st.markdown("# Ingredients")
    

    ingr_grid={}

    for x in range(len(item_data)):
        y = st.columns(7)
        for y, col in enumerate(y):
            coord = (x,y)
            tile = col.container()
            # tile.write(f"{x}_{y}")
            ingr_grid[coord] = tile
    
    
    ### TODO: Not sure determining max bounds of grid is needed
    ingr_coords = list(ingr_grid.keys())
    x,y = [r for r, c in ingr_coords],[c for r, c in ingr_coords]
    ing_rows, ing_cols = max(x)+1, max(y)+1 
    # print(ing_rows)
    # print(ing_cols)

    row = 0
    ingr_grid[(row,0)].markdown("##### Ingredient")
    ingr_grid[(row,1)].markdown("##### ID")
    ingr_grid[(row,2)].markdown("##### Number Needed")
    ingr_grid[(row,3)].markdown("##### Shop price")
    ingr_grid[(row,4)].button("**NQ**", help="Click to set all to NQ", type="tertiary")
    ingr_grid[(row,5)].button("**NQ**", help="Click to set all to HQ", type="tertiary")
    ingr_grid[(row,6)].markdown("##### Cost")



    craft_cost = 0
    for row, (k,v) in enumerate(item_data.items()):
        if k.startswith("ingredient"):
            cost = 0
            ingr_grid[(row,0)].write(v["name"])
            ingr_grid[(row,1)].write(f"{v["id"]}")
            ingr_grid[(row,2)].write(f"{v["amount"]}")
            with ingr_grid[(row,3)]:
                if v["shop_price"]:
                    shop_qty = st.number_input("no_shop", min_value = 0, max_value = v["amount"], key=f"{ingr_grid[(row,3)]}_shop_qty", label_visibility="hidden")
                    st.write(f"Price each: {v["shop_price"]}")
                    shop_total = v["shop_price"] * shop_qty 
                    st.write(f"{shop_total}")
                    cost += shop_total
                else:
                    st.write(":red[No Shop]")
            with ingr_grid[(row,4)]:
                nq_qty = st.number_input("no_nq", min_value = 0, max_value = v["amount"], value=v["amount"],  key=f"{ingr_grid[(row,3)]}_nq_qty", label_visibility="hidden")
                st.write(f"Price each: {v["nq"]["minPrice"]}")
                nq_total = v["nq"]["minPrice"] * nq_qty
                st.write(f"{nq_total}")
                cost += nq_total
            with ingr_grid[(row,5)]:
                if v.get("hq"):
                    hq_qty = st.number_input("no_hq", min_value = 0, max_value = v["amount"], key=f"{ingr_grid[(row,3)]}_hq_qty", label_visibility="hidden")
                    st.write(f"Price each: {v["hq"]["minPrice"]}")
                    hq_total = v["hq"]["minPrice"] * hq_qty
                    st.write(f"{hq_total}")
                    cost += hq_total
                else:
                    st.write(":red[No HQ item]")
            ingr_grid[(row,6)].write(f"{cost}")
            craft_cost += cost
    
    st.markdown(f"#### Cheapest buyable from is :red[{craft_cost} gil]")
    # if v["amount"] > 1:
    #     st.write(f"Price per craftable amount ({v["amount"]}: {v["amount"] * {craft_cost}}")




    # for k,v in item_data.items():
    #     if k.startswith("ingredient"):
    #         cols = st.columns(4)
    #         row.append(cols)
    #         # ingredients_col, nq_col, hq_col, total_col = st.columns(4)
    #         # ingredients_container = st.container(border=True)
    #         for col in row:# with ingredients_col:
    #             st.write(f'### {v["name"]} ({v["id"]})')
    #             st.write(f'Number required: {v["amount"]}')
    #             st.write(f'Shop price: {v["shop_price"]}') ## Conditional format red if None
    #             # with nq_col:
    #             st.write(f'NQ price: {v["nq"]["minPrice"]}')
    #             st.write(f'NQ velocity: ({v["nq"]["velocity"]:.0f}/day)')
    #             # with hq_col:
    #             try:
    #                 st.write(f'HQ price: {v["hq"]["minPrice"]}')
    #                 st.write(f'HQ velocity: ({v["hq"]["velocity"]:.0f}/day)')
    #             except:
    #                 pass




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


def print_analysis(item_data: dict):
    nq_craft_cost = 0
    hq_craft_cost = 0
    buy_hq_total = 0
    
    for k, v in item_data.items():
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

        if k.startswith('result'):
            ###TODO: Add source/quality of each item
            buy_hq_total = hq_total
    
    for k, v in item_data.items():
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

        if k.startswith('ingredient'):
            ###TODO: Add source/quality of each item
            nq_craft_cost += min(x for x in [shop_total, nq_total, hq_total] if x is not None)
            if hq_total is not None:
                hq_craft_cost += min(x for x in [shop_total, hq_total] if x is not None and hq_total)
            else:
                hq_craft_cost += min(x for x in [shop_total, nq_total, hq_total] if x is not None)
            # st.write(f"Cumulative Crafting Cost buying NQ mats: {nq_craft_cost:,.0f}")
            # st.write(f"Cumulative Crafting Cost buying HQ mats: {hq_craft_cost:,.0f}")

    nq_craft_pl = buy_hq_total - nq_craft_cost
    hq_craft_pl = buy_hq_total - hq_craft_cost
    nq_craft_pl_perc =  nq_craft_pl / buy_hq_total
    hq_craft_pl_perc =  hq_craft_pl / buy_hq_total

    with cont_analysis.container():
        st.write(f"### Profit Analysis")
        # analysis_container.write(f"Buy completed NQ cost: {buy_nq_total:,.0f}") if buy_nq_total else None
        st.write(f"Buy completed HQ cost: {buy_hq_total:,.0f}") if buy_hq_total else None
        st.write(f"Craft HQ from buying NQ items cost: {nq_craft_cost:,.0f}") if nq_craft_cost else None
        st.write(f"Craft HQ from buying HQ items cost: {hq_craft_cost:,.0f}") if hq_craft_cost else None
        st.write("")
        st.write(f"Craft HQ from buying NQ items P/L: {buy_hq_total:,.0f} - {nq_craft_cost:,.0f} = {nq_craft_pl:,.0f} ({nq_craft_pl_perc:,.2%})")
        if nq_craft_pl_perc <= 0:
            st.error("Warning: Crafting this item will result in a loss!")
        elif nq_craft_pl_perc < 0.2:
            st.error("Note: Low profit margin (below 20%)!")
                
        st.write(f"\nCraft HQ from buying HQ items P/L: {buy_hq_total:,.0f} - {hq_craft_cost:,.0f} = {hq_craft_pl:,.0f} ({hq_craft_pl_perc:,.2%})")
        if hq_craft_pl_perc <= 0:
            st.error("Warning: Crafting this item will result in a loss!")
        elif hq_craft_pl_perc < 0.2:
            st.error("Note: Low profit margin (below 20%)!")

if __name__ == "__main__":
    st.set_page_config(layout="wide")
    
    recipe_list = get_all_recipes()  ### List of recipes for all craftable items in game
    selectbox_recipe_list = duckdb.sql("SELECT result_text, result_id from recipe_list").pl()
    
    st.title("FFXIV Craft or Buy Checker")
    st.markdown("")
    st.text("""Select recipe to check if better value to craft from ingredients or buy from marketboard.\nNumber in parentheses is item id.""")
    item_selectbox = st.selectbox("label", options=selectbox_recipe_list ,index=0,label_visibility="hidden")
    ###TODO Change index back to None

    if not st.session_state:
        st.session_state["loading"] = False
    
    if item_selectbox:
        with st.spinner(text="In progress..."):
            st.session_state.loading = True
            
            item_id = duckdb.sql(f"""SELECT result_id from selectbox_recipe_list where result_text = '{item_selectbox.replace(r"'",r"''")}'""").pl()
            item_id = item_id[0,0]
            
            cont_analysis = st.empty()
            cont_result = st.empty()
            cont_ingr = st.empty()
            
            lookup_item_ids = get_recipe_items(item_id)
            calc = fetch_universalis(lookup_item_ids)
            
            
            print_result(calc)
            print_ingredients(calc)
            print_analysis(calc)
        
            
            
            # write_formatted_price(test)
    
    # with st.form("form"):
    #     st.write("stuff inside form")
    #     slider_val = st.slider("form slider")
    #     checkbox_val = st.checkbox("form checkbox")

    #     submitted = st.form_submit_button("submit")
    #     if submitted:
    #         st.write("slider", slider_val, "checkbox", checkbox_val)


    # tab1, tab2, tab3 = st.tabs(["1","2","3"])

    # with tab1:
    #     st.header("1")
    #     st.markdown("""wow!!!!!!!!""")

    # st.container()
    # st.toggle("yes","no")
    # st.download_button("yes","no")