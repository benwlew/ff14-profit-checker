# FFXIV Crafting Profit/Loss Checker

App for FFXIV that calculates whether items are cheaper to buy directly, or buy ingredients & craft.\
Hosted at: https://xivcraftprofit.streamlit.app/

- Crafting recipes, item data, shop data, etc. are loaded from [ffxiv-datamining](https://github.com/xivapi/ffxiv-datamining) GitHub repo and saved to local duckdb database.\
Databases are checked for updates daily at 8PM JST (3AM PDT), but will not change unless a new patch has been released with new items.
- Item prices are updated dynamically from the [Universalis](https://universalis.app/) REST API on user request.

Built using python, polars, duckdb and streamlit.