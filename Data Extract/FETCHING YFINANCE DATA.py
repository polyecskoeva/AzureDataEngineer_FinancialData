# Databricks notebook source
# MAGIC %md
# MAGIC # **Fetching S&P 500 companies' Financial Data from Yahoo Finance and Saving it in the Data Lake Bronze Container**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Azure Data Lake Service Principal Authentication

# COMMAND ----------

## Service Princial to allow Databricks access Data Lake

# COMMAND ----------

# MAGIC %md
# MAGIC ## S&P 500 companies financial data download and save to Datalake

# COMMAND ----------

import yfinance as yf
import pandas as pd
import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime

# S&P 500 companies scrape
def scrape_sp500_companies():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find('table', {'class': 'wikitable'})
    if not table:
        raise ValueError("S&P 500 table not found.")
    
    symbols, entities, sectors, subinds = [], [], [], []
    for row in table.find_all('tr')[1:]:  # Skip header row
        cols = row.find_all('td')
        symbol = cols[0].text.strip()
        symbols.append(symbol.replace('.', '-'))
        entities.append(cols[1].text.strip())
        sectors.append(cols[2].text.strip())
        subinds.append(cols[3].text.strip())
    
    return pd.DataFrame({
        'Symbol': symbols,
        'Entities': entities,
        'GICS_Sector': sectors,
        'GICS_Industry': subinds
    })
#Stock Data fetching
def fetch_stock_data(symbols):
    data = yf.download(
        symbols, 
        start="2023-01-01", 
        end=pd.to_datetime("today").strftime('%Y-%m-%d'), 
        group_by='ticker', 
        interval="1d"
    )
    all_data = []
    for symbol in symbols:
        df = data[symbol][['Open', 'Close', 'Volume']].reset_index()
        df['Ticker'] = symbol
        all_data.append(df)
    return pd.concat(all_data, ignore_index=True)

# Basic company information fetching
def fetch_company_data(symbols):
    company_data = []
    for company in symbols:
        try:
            info = yf.Ticker(company).info
            company_data.append({
                'Symbol': company,
                'Name': info.get('shortName', 'N/A'),
                'Sector': info.get('sector', 'N/A'),
                'Industry': info.get('industry', 'N/A'),
                'Market_Cap': info.get('marketCap', 'N/A'),
                'Profit': info.get('profitMargins', 'N/A')
            })
            time.sleep(1)  # 1 sec waiting as yfinance has a limit
        except Exception as e:
            print(f"Error fetching data for {company}: {e}")
            continue
    df = pd.DataFrame(company_data)
    df.replace({'N/A': pd.NA}, inplace=True)
    df['Profit'] = pd.to_numeric(df['Profit'], errors='coerce')
    return df

# Data Quality Check - Addressing potential gaps in company names or industry information.
# This step utilizes the scraped data, which contains complete names and industries, to fill in any missing values.
def merge_company_data(company_data, basics_df):
    merged_df = pd.merge(company_data, basics_df, on='Symbol', how='left')
    merged_df['Name'] = merged_df['Name'].fillna(merged_df['Entities'])
    merged_df['Sector'] = merged_df['Sector'].fillna(merged_df['GICS_Sector'])
    merged_df['Industry'] = merged_df['Industry'].fillna(merged_df['GICS_Industry'])
    return merged_df

# Dividend data
def fetch_dividend_data(symbols, start_date, end_date):
    dividend_data = []
    for company in symbols:
        try:
            div_history = yf.Ticker(company).dividends
            if div_history.empty:
                continue
            div_history = div_history[(div_history.index >= start_date) & (div_history.index <= end_date)]
            for date, amount in div_history.items():
                dividend_data.append({
                    'Symbol': company,
                    'Dividend Date': date.strftime('%Y-%m-%d'),
                    'Dividend Amount': amount
                })
        except Exception as e:
            print(f"Error fetching dividends for {company}: {e}")
            continue
    return pd.DataFrame(dividend_data)

# S&P 500 companies set up
scraped_basics_df = scrape_sp500_companies()

# 1. Stock Data
stock_data = fetch_stock_data(scraped_basics_df['Symbol'].tolist())
spark_df_stock = spark.createDataFrame(stock_data)
spark_df_stock.write.mode("overwrite").parquet("abfss://bronze@compstockdatalake.dfs.core.windows.net/parquet_files/stock_data")

# 2. Company Data
company_basic_data = fetch_company_data(scraped_basics_df['Symbol'].tolist())
final_company_data = merge_company_data(company_basic_data, scraped_basics_df)
spark_df_company = spark.createDataFrame(final_company_data)
spark_df_company.write.mode("overwrite").parquet("abfss://bronze@compstockdatalake.dfs.core.windows.net/parquet_files/final_company_data")

# 3. Dividend Data
start_date = pd.to_datetime('2023-01-01').tz_localize('America/New_York')
end_date = pd.to_datetime(datetime.today()).tz_localize('America/New_York')
dividend_df = fetch_dividend_data(scraped_basics_df['Symbol'].tolist(), start_date, end_date)
spark_df_dividend = spark.createDataFrame(dividend_df)
spark_df_dividend.write.mode("overwrite").parquet("abfss://bronze@compstockdatalake.dfs.core.windows.net/parquet_files/dividend_data")
