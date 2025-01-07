# Databricks notebook source
# MAGIC %md
# MAGIC #  Data Transformation and Analysis

# Service Principal credentials

# MAGIC %md
# MAGIC ## Data Retrieval

# COMMAND ----------

from pyspark.sql.functions import col, to_date

df_stock = spark.read.parquet("abfss://bronze@compstockdatalake.dfs.core.windows.net/parquet_files/stock_data/part*.parquet")
df_dividend=spark.read.parquet("abfss://bronze@compstockdatalake.dfs.core.windows.net/parquet_files/dividend_data/part*.parquet")
df_basic_company_data=spark.read.parquet("abfss://bronze@compstockdatalake.dfs.core.windows.net/parquet_files/final_company_data/part*.parquet")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation

# COMMAND ----------


# Rename some columns
df_dividend = df_dividend.withColumnRenamed("Dividend Date", "Dividend_Date") \
               .withColumnRenamed("Dividend Amount", "Dividend_Amount")
df_dividend = df_dividend.withColumn("Dividend_Date", to_date(col("Dividend_Date"), "yyyy-MM-dd"))

df_basic_company_data=df_basic_company_data.withColumnRenamed("Market Cap", "Market_Cap") \
                                           .withColumnRenamed("Industry", "Sub_Sector")


# Drop columns
df_basic_company_data=df_basic_company_data.drop("Entities","GICS_Sector", "GICS_Industry")  




# Create SQL -View Tables

df_stock.createOrReplaceTempView("stock_data_table")
df_dividend.createOrReplaceTempView("dividend_data_table")
df_basic_company_data.createOrReplaceTempView("basic_company_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact table creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Volatility Calculation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Calculating logarithmic returns
# MAGIC CREATE OR REPLACE TEMPORARY VIEW log_returns AS
# MAGIC SELECT 
# MAGIC     Ticker,
# MAGIC     Date,
# MAGIC     Close,
# MAGIC     LOG(Close / LAG(CLose) OVER (PARTITION BY Ticker ORDER BY Date)) AS log_return
# MAGIC FROM 
# MAGIC     stock_data_table;
# MAGIC
# MAGIC -- 2. Removing the first rows where log_return is null (for the first day’s price)
# MAGIC CREATE OR REPLACE TEMPORARY VIEW valid_log_returns AS
# MAGIC SELECT *
# MAGIC FROM log_returns
# MAGIC WHERE log_return IS NOT NULL;
# MAGIC
# MAGIC -- 3. Calculating volatility (standard deviation) by company and year
# MAGIC CREATE OR REPLACE TEMPORARY VIEW volatility AS
# MAGIC SELECT 
# MAGIC     ticker,
# MAGIC     YEAR(date) AS year,
# MAGIC     STDDEV(log_return) AS volatility
# MAGIC FROM valid_log_returns
# MAGIC GROUP BY ticker, YEAR(date);
# MAGIC
# MAGIC -- 4. Calculating annualized volatility, show only full year data
# MAGIC CREATE OR REPLACE TEMPORARY VIEW annual_volatility AS
# MAGIC SELECT 
# MAGIC     ticker,
# MAGIC     year,
# MAGIC     volatility * SQRT(252) AS annual_volatility
# MAGIC FROM volatility
# MAGIC WHERE year < (select MAX(year) from volatility)
# MAGIC
# MAGIC

# COMMAND ----------


df_annual_volatility = spark.sql("SELECT * FROM annual_volatility")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Yearly TSR plan Calculation with 1 million USD investment

# COMMAND ----------

df_annual_tsr = spark.sql("""
WITH PriceData AS (
    SELECT ticker, 
           YEAR(date) AS year,
           FIRST_VALUE(Close) OVER (PARTITION BY ticker, YEAR(date) ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS beginning_price,
           LAST_VALUE(Close) OVER (PARTITION BY ticker, YEAR(date) ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS ending_price
    FROM stock_data_table
),
PivotedPriceData AS (
    SELECT 
        ticker,
        year,
        MAX(beginning_price) AS beginning_price,
        MAX(ending_price) AS ending_price
    FROM PriceData
    GROUP BY ticker, year
),
DividendData AS (
    SELECT Symbol as ticker, 
           YEAR(Dividend_Date) AS year,
           SUM(Dividend_Amount) AS total_dividends
    FROM dividend_data_table
    GROUP BY Symbol, YEAR(Dividend_Date)
)
SELECT pd.ticker,
       pd.year,
       pd.beginning_price,
       pd.ending_price,
       dd.total_dividends,
       ROUND(((pd.ending_price + dd.total_dividends) / pd.beginning_price) - 1,3) AS TSR,
       ROUND(1000000 * ((pd.ending_price + dd.total_dividends) / pd.beginning_price),2) AS Return_Value
FROM PivotedPriceData pd
JOIN DividendData dd ON pd.ticker = dd.ticker AND pd.year = dd.year
ORDER BY pd.ticker, pd.year
""")
df_annual_tsr.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension tables creation

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Sector and Sub_Sector tables creation with adding primary keys

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Create a combined table that handles the Sector and Sub_Sector combinations
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Sector_Sub_industry_Combination AS
# MAGIC SELECT 
# MAGIC     DENSE_RANK() OVER (ORDER BY Sector)  AS Sector_ID,
# MAGIC     Sector,
# MAGIC     DENSE_RANK() OVER (ORDER BY Sub_Sector) +99  AS Sub_Sector_ID,
# MAGIC     Sub_Sector
# MAGIC      
# MAGIC FROM 
# MAGIC     basic_company_data
# MAGIC GROUP BY sector,Sub_Sector;
# MAGIC
# MAGIC -- 2. Create the Sector table that contains the Sector_ID and Sector pair
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Sector AS
# MAGIC SELECT DISTINCT
# MAGIC     Sector_ID,
# MAGIC     Sector
# MAGIC FROM 
# MAGIC     Sector_Sub_industry_Combination;
# MAGIC
# MAGIC -- 3. Create the Sub_Sector table that contains the Sub_Sector_ID and Sub_Sector pair
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Sub_Sector AS
# MAGIC SELECT DISTINCT
# MAGIC     Sub_Sector_ID,
# MAGIC     Sub_Sector
# MAGIC FROM 
# MAGIC     Sector_Sub_industry_Combination;
# MAGIC
# MAGIC -- 4. Create the final table that contains only the Sector_ID and Sub_Sector_ID pairs
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Sector_Sub_Sector_ID_Combination AS
# MAGIC SELECT 
# MAGIC     Sector_ID,
# MAGIC     Sub_Sector_ID     
# MAGIC FROM 
# MAGIC     Sector_Sub_industry_Combination;
# MAGIC select * from Sector_Sub_Sector_ID_Combination
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Store them in DataFrames

# COMMAND ----------

df_Sector = spark.sql("SELECT * FROM Sector")
df_Sub_Sector = spark.sql("SELECT * FROM Sub_Sector")
df_Sector_Sub_Sector_ID_Combination=spark.sql("SELECT * FROM Sector_Sub_Sector_ID_Combination")
df_Sector_Sub_Sector_ID_Combination.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Transform the basic_company_data table so that the foreign keys for Sector and Sub_Sector are included.

# COMMAND ----------

df_basic_company_data_transformed = spark.sql("""select Symbol, 
      Name, 
      s.Sector_ID,  
      ss.Sub_Sector_ID,
      Market_Cap,
      Profit
from basic_company_data bcd
left join Sector s on s.Sector=bcd.Sector
left join Sub_Sector ss on ss.Sub_Sector=bcd.Sub_Sector""")



# COMMAND ----------

# MAGIC %md
# MAGIC ###  3. Create a Calendar Table for easier joins in the future

# COMMAND ----------

df_calendar = spark.sql("""SELECT DISTINCT
    Date,
    DATE_FORMAT(TO_TIMESTAMP(Date, 'yyyy-MM-dd''T''HH:mm:ss.SSSXXX'), 'yyyy-MM-dd') AS Formatted_Date,
    YEAR(Date) AS Year,
    MONTH(Date) AS Month,
    DAY(Date) AS Day,
    QUARTER(Date) AS Quarter, 
    DAYOFWEEK(Date) AS Weekday,
    WEEKOFYEAR(Date) AS Week_Number, 
    CASE WHEN DAYOFWEEK(Date) IN (1, 7) THEN 'Yes' ELSE 'No' END AS Is_Weekend, 
    DAYOFYEAR(Date) AS Day_of_Year
FROM 
    stock_data_table
order by 2""")

df_calendar.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transfer the dataframes to Datalake silver container

# COMMAND ----------



# Write the DataFrames to the Silver container Fact folder
df_annual_volatility.write.parquet("abfss://silver@compstockdatalake.dfs.core.windows.net/Facts/annual_volatility_fact.parquet", mode="overwrite")
df_stock.write.parquet("abfss://silver@compstockdatalake.dfs.core.windows.net/Facts/stock_data_fact.parquet", mode="overwrite")
df_annual_tsr.write.parquet("abfss://silver@compstockdatalake.dfs.core.windows.net/Facts/annual_tsr_fact.parquet", mode="overwrite")
df_dividend.write.parquet("abfss://silver@compstockdatalake.dfs.core.windows.net/Facts/dividend_data_fact.parquet", mode="overwrite")

# Write the DataFrames to the Silver container Dimension folder

df_Sector.write.parquet("abfss://silver@compstockdatalake.dfs.core.windows.net/Dimensions/Sector_dimension.parquet", mode="overwrite") 
df_Sub_Sector.write.parquet("abfss://silver@compstockdatalake.dfs.core.windows.net/Dimensions/Sub_Sector_dimension.parquet", mode="overwrite") 
df_Sector_Sub_Sector_ID_Combination.write.parquet("abfss://silver@compstockdatalake.dfs.core.windows.net/Dimensions/Sector_Sub_Sector_ID_Combination_dimension.parquet", mode="overwrite")  
df_basic_company_data_transformed.write.parquet("abfss://silver@compstockdatalake.dfs.core.windows.net/Dimensions/basic_company_data_transformed_dimension.parquet", mode="overwrite")  
df_calendar.write.parquet("abfss://silver@compstockdatalake.dfs.core.windows.net/Dimensions/calendar_dimension.parquet", mode="overwrite") if 'df_calendar' in locals() else None
