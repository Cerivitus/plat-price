# plat-price
The goal of this project is to scrape Playstation Store Prices from https://platprices.com/

Part 1: Loading data into BQ

Part 2: Setting up BQ for dbt

Notes: 

- 2 GCP projects (Analytics, Raw)
- 1 dbt service account (BQ Jobs, BQ Data Editor | BQ Viewer)
- Create a Github Repo (do not tick any additional fields after specifying name)
- Each developer should have their own sandbox (within analytics project) in GCP dbt_firstinitiallastname
- While repo empty, initialize with dbt init
- In a dbt project, you can setup folders by maturity (source,staging,mart) or by domain (finance,sales,marketing)
- if you create an empty folder, create a .gitkeep file so git tracks it

