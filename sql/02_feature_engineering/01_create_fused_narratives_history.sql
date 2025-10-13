-- ==================================================================
-- Mirror of History (历史之镜) - Feature Engineering: Fused Narrative Fingerprints
-- ==================================================================
-- Core concept: 历史之镜 (Mirror of History) - AI agent for historical analogy analysis
-- Based on your actual code implementation

CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.historical_mirror_mvp.fused_narratives_history` AS

WITH
-- CTE 1: Filter high-quality news data sources (过滤高质量新闻数据源)
filtered_news AS (
  SELECT date, themes, title
  FROM `YOUR_PROJECT_ID.historical_mirror_mvp.raw_news_whitelisted`
  WHERE REGEXP_CONTAINS(url, r'(cnbc.com|reuters.com|bloomberg.com|wsj.com|ft.com|marketwatch.com)')
),

-- CTE 2: Calculate S&P 500 features (计算S&P500特征)
sp500_features AS (
  SELECT
    date,
    sp500_close,
    (sp500_close / MAX(sp500_close) OVER (
        ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    )) - 1 AS pct_from_52w_high
  FROM `YOUR_PROJECT_ID.historical_mirror_mvp.raw_prices`
),

-- CTE 3: Macro data table (VIX and FFR) (宏观数据表 (VIX和FFR))
macro_daily_pivoted AS (
  SELECT
    COALESCE(vix.date, ffr.date) AS date,
    vix.value AS vix_close,
    ffr.value AS ffr_close
  FROM `YOUR_PROJECT_ID.historical_mirror_mvp.raw_vix_fred` vix
  FULL OUTER JOIN `YOUR_PROJECT_ID.historical_mirror_mvp.raw_ffr_fred` ffr
  ON vix.date = ffr.date
),

-- CTE 4: CPI 月度前向填充
cpi_daily_filled AS (
  SELECT
    calendar_date AS date,
    LAST_VALUE(cpi.value IGNORE NULLS) OVER (ORDER BY calendar_date) AS cpi_value
  FROM UNNEST(GENERATE_DATE_ARRAY('2015-01-01', '2023-12-31', INTERVAL 1 DAY)) AS calendar_date
  LEFT JOIN `YOUR_PROJECT_ID.historical_mirror_mvp.raw_cpi_fred` cpi
  ON calendar_date = cpi.date
),

-- CTE 5: 周级别特征聚合，新增 weekly_titles 聚合
weekly_features AS (
  SELECT
    DATE_TRUNC(p.date, WEEK(MONDAY)) AS week_start_date,
    STRING_AGG(DISTINCT fn.themes, '; ') AS weekly_themes,
    STRING_AGG(DISTINCT fn.title, '; ') AS weekly_titles,  -- 新增weekly_titles
    AVG(p.sp500_close) AS avg_weekly_price,
    AVG(p.pct_from_52w_high) AS avg_pct_from_52w_high,
    AVG(m.vix_close) AS avg_weekly_vix,
    AVG(m.ffr_close) AS avg_weekly_ffr,
    AVG(c.cpi_value) AS avg_weekly_cpi
  FROM sp500_features p
  LEFT JOIN filtered_news fn ON p.date = fn.date
  LEFT JOIN macro_daily_pivoted m ON p.date = m.date
  LEFT JOIN cpi_daily_filled c ON p.date = c.date
  WHERE p.date <= '2022-12-31'
  GROUP BY week_start_date
),

-- CTE 6: 计算未来4周和8周价格，用于回报计算
weekly_features_with_returns AS (
  SELECT
    *,
    LEAD(avg_weekly_price, 4) OVER (ORDER BY week_start_date) AS price_4_weeks_later,
    LEAD(avg_weekly_price, 8) OVER (ORDER BY week_start_date) AS price_8_weeks_later
  FROM weekly_features
)

-- 最终查询整理（包含新增weekly_titles）
SELECT
  week_start_date,
  weekly_themes,
  weekly_titles, -- 新增字段
  avg_pct_from_52w_high,
  avg_weekly_vix,
  avg_weekly_ffr,
  avg_weekly_cpi,
  SAFE_DIVIDE(price_4_weeks_later - avg_weekly_price, avg_weekly_price) AS return_30d,
  SAFE_DIVIDE(price_8_weeks_later - avg_weekly_price, avg_weekly_price) AS return_60d
FROM weekly_features_with_returns
WHERE weekly_themes IS NOT NULL
  AND price_8_weeks_later IS NOT NULL
ORDER BY week_start_date;