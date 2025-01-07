# Financial Data Engineering
A Finance Data Engineering project leveraging Azure Cloud involves downloading S&P 500 companies' financial data using the yfinance package in Databricks, 
storing it in Azure Data Lake Gen2, and orchestrating workflows with Azure Data Factory. The data is processed and analyzed in Azure Synapse Analytics for insights.

## Architecture
![Project Architecture](AzureDataEngineer_FinancialData/Data%20Architecture.jpeg)
1. Programming Language - Python
2. Scripting Language - SQL
3. Microsoft Azure
   - Azure Data Factory
   - Databricks
   - Data Lake Gen2
   - Azure Synapse Analytics
     

## Dataset
The dataset includes stock prices, dividend data, and fundamental company information fetched from Yahoo Finance. Total Shareholder Return (TSR) and volatility metrics were calculated to provide deeper insights into the performance of S&P 500 companies.
[Here is the dataset.](https://github.com/polyecskoeva/AzureDataEngineer_FinancialData/tree/main/Data_Raw)

** Data model


** Data Extract
Here is the Python script fetching the data using yfinance package: [Extract data](Data%20Extract/FETCHING%20YFINANCE%20DATA.py)

