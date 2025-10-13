-- ==================================================================
-- Mirror of History (历史之镜) - Dataset Creation and Data Ingestion
-- ==================================================================
-- This file contains all data ingestion code for the Mirror of History project
-- Core concept: 历史之镜 (Mirror of History) - AI agent for historical analogy analysis

-- Step 1: Create a dedicated dataset to store all project resources
-- Dataset: historical_mirror_mvp = "历史之镜" (Mirror of History) main dataset
-- Naming: historical(历史) + mirror(镜子) + mvp(最小可行产品)
-- Purpose: Store raw data, feature tables, AI models, vector indexes, etc.
CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT_ID.historical_mirror_mvp`
OPTIONS(
  description = 'Dataset for the Mirror of History project, containing raw data, feature tables, and AI models.',
  location = 'US' -- Create dataset in US multi-region for better query performance with public datasets
);

-- Step 2: Create raw data tables
-- 2.1: S&P 500 daily price data (标普500日线价格表)
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.historical_mirror_mvp.raw_prices`
(
  date DATE NOT NULL,
  sp500_close FLOAT64,
  volume FLOAT64
)
OPTIONS(
  description="Daily S&P 500 closing prices and trading volume"
);

-- 2.2: VIX volatility index data (VIX波动率指数表)
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.historical_mirror_mvp.raw_vix_fred`
(
  date DATE NOT NULL,
  value FLOAT64
)
OPTIONS(
  description="VIX volatility index data from FRED"
);

-- 2.3: Federal Funds Rate data (联邦基金利率表)
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.historical_mirror_mvp.raw_ffr_fred`
(
  date DATE NOT NULL,
  value FLOAT64
)
OPTIONS(
  description="Federal Funds Rate data from FRED"
);

-- 2.4: CPI inflation data (CPI通胀数据表)
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.historical_mirror_mvp.raw_cpi_fred`
(
  date DATE NOT NULL,
  value FLOAT64
)
OPTIONS(
  description="Consumer Price Index (CPI) data from FRED"
);

-- 2.5: Financial news data (财经新闻表)
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.historical_mirror_mvp.raw_news_whitelisted`
(
  date DATE,
  url STRING,
  themes STRING,
  organizations STRING,
  persons STRING,
  tone STRING,
  title STRING
)
OPTIONS(
  description="Financial news data from whitelisted domains"
);

-- Step 3: Data loading instructions
-- Note: Actual data is loaded through the following methods:
-- 1. Use bq load command to load from CSV files
-- 2. Use Python scripts to fetch data from APIs
-- 3. Query GDELT public datasets

-- Example: Load data from CSV files
-- bq load --source_format=CSV --skip_leading_rows=1 \
--   YOUR_PROJECT_ID:historical_mirror_mvp.raw_prices \
--   data/raw_prices.csv \
--   date:DATE,sp500_close:FLOAT,volume:FLOAT

-- Step 4: Create domain whitelist for financial news
-- Whitelist: Common English financial/mainstream media domains (可按需增删)
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.historical_mirror_mvp.domain_whitelist`
AS SELECT domain FROM UNNEST([
  'cnbc.com',
  'reuters.com',
  'bloomberg.com',
  'ft.com',
  'wsj.com',
  'nytimes.com',
  'finance.yahoo.com',
  'markets.businessinsider.com',
  'marketwatch.com',
  'thestreet.com'
]) AS domain;