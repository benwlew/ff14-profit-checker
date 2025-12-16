# FFXIV Crafting Profit/Loss Checker

App for FFXIV that calculates whether items are cheaper to buy directly, or buy ingredients & craft.\
Hosted at: https://ff14-profit-check.streamlit.app/

- Crafting recipes are loaded from [ffxiv-datamining](https://github.com/xivapi/ffxiv-datamining) GitHub repo and saved to local duckdb database - recipes are manually updated each patch (will eventually be updated to check for updates daily).
- Item prices are updated dynamically from the [Universalis](https://universalis.app/) REST API on user request.

