-- ==================================================================
-- File: sql/04_inference/01_master_inference_query.sql (V3 - Enhanced Decision Context Version)
-- Purpose: Core inference engine of the AI agent. Combines quantitative metrics with narrative analysis to output structured five-part reports.
-- Output: Contains both input prompt (input_prompt) and LLM output (counsellor_report).
-- ==================================================================
-- Core concept: 历史之镜 (Mirror of History) - AI agent for historical analogy analysis

-- Dataset: historical_mirror_mvp = "历史之镜" (Mirror of History) main dataset
-- Contains: Raw data tables, feature engineering tables, AI models, vector indexes, etc.

-- Target week (Monday as start point)
-- 可用的2023年target date列表（周一日期）：
-- 2023-01-02, 2023-01-09, 2023-01-16, 2023-01-23, 2023-01-30, 2023-02-06, 2023-02-13, 2023-02-20, 2023-02-27,
-- 2023-03-06, 2023-03-13, 2023-03-20, 2023-03-27, 2023-04-03, 2023-04-10, 2023-04-17, 2023-04-24, 2023-05-01,
-- 2023-05-08, 2023-05-15, 2023-05-22, 2023-05-29, 2023-06-05, 2023-06-12, 2023-06-19, 2023-06-26, 2023-07-03,
-- 2023-07-10, 2023-07-17, 2023-07-24, 2023-07-31, 2023-08-07, 2023-08-14, 2023-08-21, 2023-08-28, 2023-09-04,
-- 2023-09-11, 2023-09-18, 2023-09-25, 2023-10-02, 2023-10-09, 2023-10-16, 2023-10-23, 2023-10-30, 2023-11-06,
-- 2023-11-13, 2023-11-20, 2023-11-27, 2023-12-04, 2023-12-11, 2023-12-18, 2023-12-25
-- 用户可以将下面的日期替换为上述列表中的任意一个日期
DECLARE target_date_param DATE DEFAULT '2023-01-30';

WITH
-- Step 1: Build "current" narrative fingerprint (FP-L) for target date in real-time
-- FP-L = Figure Print Left (当前叙事指纹) - Initial market narrative state
current_narrative_and_embedding AS (
  SELECT
    ml_generate_embedding_result AS embedding,
    content AS narrative_text,
    CAST(ROUND(avg_vix, 2) AS STRING) AS avg_vix,
    CAST(ROUND(avg_ffr, 2) AS STRING) AS avg_ffr,
    CAST(ROUND(avg_cpi, 2) AS STRING) AS avg_cpi
  FROM
    ML.GENERATE_EMBEDDING(
      MODEL `YOUR_PROJECT_ID.historical_mirror_mvp.embedding_model`,
      (
        WITH
        cpi_daily_filled AS (
          SELECT
            calendar.date,
            LAST_VALUE(cpi.value IGNORE NULLS) OVER (ORDER BY calendar.date) AS cpi_value
          FROM
            (SELECT date FROM UNNEST(GENERATE_DATE_ARRAY('2015-01-01', '2023-12-31', INTERVAL 1 DAY)) AS date) AS calendar
          LEFT JOIN `YOUR_PROJECT_ID.historical_mirror_mvp.raw_cpi_fred` AS cpi ON calendar.date = cpi.date
        ),
        -- Pre-aggregate randomized news titles (truncated to prevent excessive length)
        randomized_titles AS (
          SELECT
            (SELECT SUBSTR(STRING_AGG(title, '; '), 1, 8000)
             FROM (
               SELECT DISTINCT fn.title
               FROM `YOUR_PROJECT_ID.historical_mirror_mvp.raw_prices` p
               JOIN `YOUR_PROJECT_ID.historical_mirror_mvp.raw_news_whitelisted` fn ON p.date = fn.date
               WHERE DATE_TRUNC(p.date, WEEK(MONDAY)) = DATE_TRUNC(target_date_param, WEEK(MONDAY))
               ORDER BY RAND()
             )
            ) AS title_string
        )
        SELECT
          CONCAT(
            'Weekly News Titles: ', (SELECT title_string FROM randomized_titles),
            '. Market Context: S&P 500 is ', CAST(ROUND(AVG(p.pct_from_52w_high) * 100, 2) AS STRING), '% from its 52-week high. ',
            'Macro Context: VIX=', CAST(ROUND(AVG(vix.value), 2) AS STRING),
            ', FFR=', CAST(ROUND(AVG(ffr.value), 2) AS STRING),
            ', CPI=', CAST(ROUND(AVG(c.cpi_value), 2) AS STRING)
          ) AS content,
          AVG(vix.value) AS avg_vix,
          AVG(ffr.value) AS avg_ffr,
          AVG(c.cpi_value) AS avg_cpi
        FROM
          (SELECT date, sp500_close,
                  (sp500_close / MAX(sp500_close) OVER (ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW)) - 1 AS pct_from_52w_high
           FROM `YOUR_PROJECT_ID.historical_mirror_mvp.raw_prices`) p
        LEFT JOIN `YOUR_PROJECT_ID.historical_mirror_mvp.raw_vix_fred` vix ON p.date = vix.date
        LEFT JOIN `YOUR_PROJECT_ID.historical_mirror_mvp.raw_ffr_fred` ffr ON p.date = ffr.date
        LEFT JOIN cpi_daily_filled c ON p.date = c.date
        WHERE DATE_TRUNC(p.date, WEEK(MONDAY)) = DATE_TRUNC(target_date_param, WEEK(MONDAY))
      ),
      STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output)
    )
),

-- Step 2: Retrieve most similar historical mirrors (检索最相似的历史镜像)
historical_mirrors AS (
  SELECT
    base.week_start_date,
    distance
  FROM VECTOR_SEARCH(
    TABLE `YOUR_PROJECT_ID.historical_mirror_mvp.narrative_vectors_history`,
    'embedding',
    (SELECT embedding FROM current_narrative_and_embedding),
    top_k => 5,
    distance_type => 'COSINE'
  )
),

-- Step 3: Join historical outcomes (连接历史结局)
mirror_outcomes AS (
  SELECT
    h.week_start_date,
    h.distance,
    (1 - h.distance) AS similarity,
    f.return_30d
  FROM historical_mirrors h
  JOIN `YOUR_PROJECT_ID.historical_mirror_mvp.fused_narratives_history` f
  ON h.week_start_date = f.week_start_date
),

-- Step 3.1: Quantitative metrics (仅保留加权30日回报；不展示概率)
-- Only keep weighted 30-day return; do not display probabilities
quantitative_metrics AS (
  SELECT
    SAFE_DIVIDE(SUM(similarity * return_30d),
                SUM(IF(return_30d IS NOT NULL, similarity, 0))) AS weighted_expected_return
  FROM mirror_outcomes
),

-- Step 3.2: Input dashboard Markdown (输入仪表盘 Markdown)
input_summary_table AS (
  SELECT
    CONCAT(
      '| Metric (Weekly Avg) | Value |\n',
      '|---|---|\n',
      '| VIX (Fear Index) | ', avg_vix, ' |\n',
      '| FFR (Fed Funds Rate) | ', avg_ffr, ' |\n',
      '| CPI (Inflation) | ', avg_cpi, ' |'
    ) AS table_string
  FROM current_narrative_and_embedding
),

-- Step 4: Generate final prompt (生成最终 Prompt)
-- Five-part structure; no scenario probabilities; mirrors sorted by good→neutral→bad (五部分结构；无情景概率；镜像按好→中→坏)
final_prompt AS (
  SELECT
    REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(
              '''You are the "Mirror of History," an AI Quantitative Strategist. Produce a five-part historical-analogy & scenario report for the **Target Week**. Write in clear US English for finance professionals and follow the exact section order.

---

# Part 1 — Target Week Summary (FP-L → historical FP-R) with One-line Scenes
- **Target Week FP-L start:** {target_date_param}
- **FP-L one-liner (≤9 words):** A vivid sensory line for the Target Week setup.
- **FP-R one-liner (≤9 words):** A vivid sensory line for the likely reassessment path.

**Write exactly 3 short paragraphs:**
1) The Target Week (FP-L) dominant drivers & conflicts (3–4 sentences).
2) What the historical analogs imply for the subsequent (historical) FP-R window and why (2–3 sentences).
3) The single biggest FP-R risk and mechanism (1–2 sentences).

---

# Part 2 — Quantitative Dashboard & Current Market Situation (with dates)

## 2.1 Quantitative Dashboard (inputs only)
{input_summary_table}

**Historical-based Weighted Expected 30d Return:** {quantitative_summary}
*(Do not show scenario probabilities.)*

## 2.2 Current Market Situation (with dates)
From this week''s inputs/titles, **cite 1–3 concrete dates (YYYY-MM-DD)** for key events (e.g., FOMC, mega-cap earnings, Adani-style episodes).
If a date cannot be inferred from inputs but is a well-known public event within FP-L, you may supply the **public record date** and add "(public record)".
If still unknown, write: "(no explicit date available in source)".
Current Input: "{current_context_text}"

---

# Part 3 — Historical Mirrors (sorted by 30d Return: Good → Neutral → Bad), then Multi-Scenario Projection

## 3.1 Historical Mirrors Table (FP-L vs FP-R narratives)
Using the analog list below:
{historical_mirror_details}

Construct a **5-row Markdown table** with columns:
`Week Start | Similarity | 30d Return | FP-L (Historical Initial Narrative) | FP-R (Historical Outcome Narrative)`
**Order rows by return bucket:** Positive (>2%) first, then Neutral ([-2%, 2%]), then Negative (<-2%).
Fill FP-L/FP-R cells with concise one-liners (no long prose).

## 3.2 Multi-Scenario Projection (strictly historical-based, not a forecast)
For **each** scenario, you MUST include **one explicit line**: *FP-L: <initial setup> → FP-R: <subsequent outcome>*, then 2–3 sentences on mechanism.
- **Negative Scenario:** cite refs; include the FP-L→FP-R line; then mechanism (2–3 sentences).
- **Neutral Scenario:** cite refs; include the FP-L→FP-R line; then mechanism (2–3 sentences).
- **Positive Scenario:** cite refs; include the FP-L→FP-R line; then mechanism (2–3 sentences).

---

# Part 4 — Socratic Strategy Questions (why they matter)
We ask these because history shows they are decisive in similar regimes. Answering them clarifies assumptions and improves decisions:
1) **Fed stance:** nearer to late-1994 data-dependent pause or late-2018 autopilot hawkishness? *(Historically dictates major shifts.)*
2) **Optimism quality:** genuine macro strength (e.g., mid-2005) or premature pivot-bet vulnerability (e.g., late-2007)? *(Misjudged optimism often precedes drawdowns.)*
3) **Single decisive indicator:** labor strength, inflation trajectory, or credit conditions? *(Each historically correlates with the next regime.)*

---

# Part 5 — Strategy & Final Takeaways
- **Most significant tail risk:** one-sentence definition + one-sentence trigger mechanism.
- **Actionable, historically justified points (numbered):**
  1) Beware narrow late-cycle rallies (tech/AI-led) before repricing.
  2) Hedge explicitly for hawkish surprises.
  3) Favor strong balance sheets under restrictive regimes.
  4) Use low VIX to add hedges; low-vol often underprices near-term risk.

**Complete all sections. Refer to FP-L/FP-R explicitly where relevant.**''',
              '{target_date_param}', CAST(target_date_param AS STRING)
            ),
            '{fp_l_end_date}', CAST(DATE_ADD(target_date_param, INTERVAL 6 DAY) AS STRING)
          ),
          '{input_summary_table}', (SELECT table_string FROM input_summary_table)
        ),
        '{quantitative_summary}', (SELECT CONCAT('- ', CAST(ROUND(weighted_expected_return * 100, 2) AS STRING), '%') FROM quantitative_metrics)
      ),
      '{current_context_text}', (SELECT narrative_text FROM current_narrative_and_embedding)
    ) AS prompt,
    -- 供 LLM 使用的“要点列表”，并按好→中→坏排序
    (SELECT STRING_AGG(
              CONCAT('On the week of ', CAST(week_start_date AS STRING),
                     ' (Similarity: ', CAST(ROUND(similarity, 3) AS STRING),
                     '), 30d return = ', CAST(ROUND(return_30d * 100, 2) AS STRING), '%.'),
              '\n'
              ORDER BY
                CASE
                  WHEN return_30d >  0.02 THEN 1
                  WHEN return_30d BETWEEN -0.02 AND 0.02 THEN 2
                  ELSE 3
                END,
                return_30d DESC
            )
     FROM mirror_outcomes) AS historical_mirror_details_text
  FROM (SELECT 1), quantitative_metrics
)

-- 步骤 5: 生成可读版 prompt + 调用 LLM
SELECT
  -- 可读版：把占位符替换为 Markdown 表（同样好→中→坏排序）
  REPLACE(
    p.prompt,
    '{historical_mirror_details}',
    (SELECT
       CONCAT(
         '| Week Start Date | Similarity Score | Subsequent 30d Return |\n',
         '|---|---|---|\n',
         STRING_AGG(
           CONCAT('| ', CAST(week_start_date AS STRING),
                  ' | ', CAST(ROUND(similarity, 3) AS STRING),
                  ' | ', CAST(ROUND(return_30d * 100, 2) AS STRING), '% |'),
           '\n'
           ORDER BY
             CASE
               WHEN return_30d >  0.02 THEN 1
               WHEN return_30d BETWEEN -0.02 AND 0.02 THEN 2
               ELSE 3
             END,
             return_30d DESC
         ),
         '\n'
       )
     FROM mirror_outcomes)
  ) AS input_prompt,
  llm_output.ml_generate_text_llm_result AS counsellor_report
FROM
  final_prompt p,
  ML.GENERATE_TEXT(
    MODEL `YOUR_PROJECT_ID.historical_mirror_mvp.gemini_model`,
    -- Version for LLM: Replace placeholder with "key points list" (提供给 LLM 的版本：用"要点列表"替换占位符)
    (SELECT REPLACE(
       p.prompt,
       '{historical_mirror_details}',
       p.historical_mirror_details_text
     ) AS prompt FROM final_prompt p),
    STRUCT(0.3 AS temperature, 8192 AS max_output_tokens, TRUE AS flatten_json_output)
  ) AS llm_output;
