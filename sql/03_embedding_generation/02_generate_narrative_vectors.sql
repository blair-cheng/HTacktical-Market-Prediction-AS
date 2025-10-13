-- ==================================================================
-- File: 02_generate_narrative_vectors.sql (Final MVP Version)
-- Purpose: Generate high-quality vector library using weekly_titles core content with minimal cost
-- ==================================================================
-- Core concept: 历史之镜 (Mirror of History) - AI agent for historical analogy analysis

-- Dataset: historical_mirror_mvp = "历史之镜" (Mirror of History) main dataset
-- Contains: Raw data tables, feature engineering tables, AI models, vector indexes, etc.
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.historical_mirror_mvp.narrative_vectors_history` AS
WITH src AS (
  SELECT
    week_start_date,
    -- Generate content column for embedding (最终优化版)
    CONCAT(
      'Weekly News Titles: ',
      -- Core optimization: Truncate weekly_titles to ensure all text fits within model's 2048 token limit (2048 * 4 ≈ 8192)
      -- This ensures minimal cost while providing complete information to the model
      SUBSTR(IFNULL(weekly_titles, ''), 1, 8000),
      '. Market Context: S&P 500 is ', CAST(ROUND(avg_pct_from_52w_high * 100, 2) AS STRING), '% from its 52-week high. ',
      'Macro Context: VIX=', CAST(ROUND(avg_weekly_vix, 2) AS STRING),
      ', FFR=',  CAST(ROUND(avg_weekly_ffr, 2) AS STRING),
      ', CPI=',  CAST(ROUND(avg_weekly_cpi, 2) AS STRING)
    ) AS content                -- Must be named 'content' for ML.GENERATE_EMBEDDING
  FROM `YOUR_PROJECT_ID.historical_mirror_mvp.fused_narratives_history`
)

SELECT
  week_start_date,
  content AS narrative_text,
  ml_generate_embedding_result AS embedding
FROM ML.GENERATE_EMBEDDING(
  MODEL  `YOUR_PROJECT_ID.historical_mirror_mvp.embedding_model`,
  TABLE  src,
  STRUCT('RETRIEVAL_DOCUMENT' AS task_type, TRUE AS flatten_json_output)
);