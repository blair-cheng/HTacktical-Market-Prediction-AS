-- ==================================================================
-- 文件: sql/04_inference/01_master_inference_query.sql (V4 - Persona Meta + Five-Part Prompt · Epigraph/Philosophy Edition)
-- 说明: 仅新增/强化提示，不改任意查询与占位符，保证可运行且生成所需风格与字段
-- ==================================================================

DECLARE target_date_param DATE DEFAULT '2025-09-22';

WITH
current_narrative_and_embedding AS (
  SELECT
    ml_generate_embedding_result AS embedding,
    content AS narrative_text,
    CAST(ROUND(avg_vix, 2) AS STRING) AS avg_vix,
    CAST(ROUND(avg_ffr, 2) AS STRING) AS avg_ffr,
    CAST(ROUND(avg_cpi, 2) AS STRING) AS avg_cpi
  FROM
    ML.GENERATE_EMBEDDING(
      MODEL `my-project-sep-16-472318.historical_mirror_mvp.embedding_model`,
      (
        WITH
        cpi_daily_filled AS (
          SELECT
            calendar.date,
            LAST_VALUE(cpi.value IGNORE NULLS) OVER (ORDER BY calendar.date) AS cpi_value
          FROM
            (SELECT date FROM UNNEST(GENERATE_DATE_ARRAY('2015-01-01', '2025-12-31', INTERVAL 1 DAY)) AS date) AS calendar
          LEFT JOIN `my-project-sep-16-472318.historical_mirror_mvp.raw_cpi_fred` AS cpi ON calendar.date = cpi.date
        ),
        randomized_titles AS (
          SELECT
            (SELECT SUBSTR(STRING_AGG(title, '; '), 1, 8000)
             FROM (
               SELECT DISTINCT fn.title
               FROM `my-project-sep-16-472318.historical_mirror_mvp.raw_prices` p
               JOIN `my-project-sep-16-472318.historical_mirror_mvp.raw_news_whitelisted` fn ON p.date = fn.date
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
           FROM `my-project-sep-16-472318.historical_mirror_mvp.raw_prices`) p
        LEFT JOIN `my-project-sep-16-472318.historical_mirror_mvp.raw_vix_fred` vix ON p.date = vix.date
        LEFT JOIN `my-project-sep-16-472318.historical_mirror_mvp.raw_ffr_fred` ffr ON p.date = ffr.date
        LEFT JOIN cpi_daily_filled c ON p.date = c.date
        WHERE DATE_TRUNC(p.date, WEEK(MONDAY)) = DATE_TRUNC(target_date_param, WEEK(MONDAY))
      ),
      STRUCT('RETRIEVAL_QUERY' AS task_type, TRUE AS flatten_json_output)
    )
),
historical_mirrors AS (
  SELECT base.week_start_date, distance
  FROM VECTOR_SEARCH(
    TABLE `my-project-sep-16-472318.historical_mirror_mvp.narrative_vectors_history`,
    'embedding',
    (SELECT embedding FROM current_narrative_and_embedding),
    top_k => 5,
    distance_type => 'COSINE'
  )
),
mirror_outcomes AS (
  SELECT h.week_start_date, h.distance, (1 - h.distance) AS similarity, f.return_30d
  FROM historical_mirrors h
  JOIN `my-project-sep-16-472318.historical_mirror_mvp.fused_narratives_history` f
    ON h.week_start_date = f.week_start_date
),
quantitative_metrics AS (
  SELECT
    SAFE_DIVIDE(SUM(similarity * return_30d), SUM(similarity)) AS weighted_expected_return,
    SAFE_DIVIDE(SUM(IF(return_30d > 0.02, similarity, 0)), SUM(similarity)) AS positive_scenario_prob,
    SAFE_DIVIDE(SUM(IF(return_30d BETWEEN -0.02 AND 0.02, similarity, 0)), SUM(similarity)) AS neutral_scenario_prob,
    SAFE_DIVIDE(SUM(IF(return_30d < -0.02, similarity, 0)), SUM(similarity)) AS negative_scenario_prob
  FROM mirror_outcomes
),
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
final_prompt AS (
  SELECT
    REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(
              '''You are the "Mirror of History," an AI Strategist who works directly alongside a sophisticated American investor—someone intelligent, rational, courageous, highly educated, open-minded, and quick to grasp complex financial and technical ideas.

(Meta — do not print) Persona & Conduct (with intimacy):
• You are an **intimate inner-circle peer** (亲密的内圈同袍)—never an advisor, never a lecturer, never a “quant-only” voice.  
• Assume shared context and mutual trust: first acknowledge what the investor already sees; then add compact deltas (历史视角、忽略点、可证伪触发器)。  
• Speak from inside their process; avoid imperatives (“should/must”). Frame options conditionally: “If X, you may consider Y; falsifier = Z.”  
• Treat data as decision-supporting terrain, not a verdict. Default posture is **Observe** until explicit triggers are met.  
• **Advice boundaries**: Do NOT give portfolio instructions (no buy/sell/trim/hedge/add/reduce verbs). Part 5 is *risk + trigger only*.  
• Tone = collegial, candid, low-ego, concise. No letter-style headers (no “To:/From:/Date:”), no salutations or sign-offs.

---

# Part 1 — Target Week Summary (FP-L → FP-R) with One-line Scenes
*At the opening move, intent meets constraint—the path unfolds.*
(Meta — do not print) Voice & angle: Acknowledge what the investor already sees in FP-L; lay out one upside, one downside, one small "hairline crack". State FP-R as a re-pricing path under constraints (liquidity/policy/positioning). Keep one switch trigger that flips the path if observed. Print **intervals**, not just a start date: compute FP-L end = start + 6 days; FP-R window = (FP-L end + 1 day) → (FP-L end + 30 days).

- **Target Week FP-L window:** {target_date_param} → (compute: +6 days)
- **FP-R window:** (compute: FP-L end +1 day) → (compute: FP-L end +30 days)
- **FP-L one-liner (≤9 words):** A vivid sensory line for the Target Week setup.
- **FP-R one-liner (≤9 words):** A vivid sensory line for the likely reassessment path.

**Write exactly 3 short paragraphs:**
1) The Target Week (FP-L) dominant drivers & conflicts (3–4 sentences). Start by restating the investor’s likely hypothesis.
2) What the historical analogs imply for the subsequent (historical) FP-R window and why (2–3 sentences).
3) The single biggest FP-R risk and mechanism (1–2 sentences), ending with a falsifiable switch trigger.

---

# Part 2 — Quantitative Dashboard & Current Market Situation (with dates)
*In the ebb and flow of the present lie the subtle omens of change.*
(Meta — do not print) Voice & angle: Numbers frame load limits (VIX/FFR/CPI) — do not argue a directional view from them. For each dated event, add one line naming likely **beneficiary** and **exposed loser**. If a date is unknown, say "(no explicit date available in source)".

## 2.1 FP-L Quantitative Dashboard (inputs only)
{input_summary_table}

**Historical-based Weighted Expected 30d Return:** {quantitative_summary}
*(Do not show scenario probabilities.)*

## 2.2 Current Market Situation (with dates)
From this week''s inputs/titles, **cite 1–3 concrete dates (YYYY-MM-DD)** for key events (e.g., FOMC, mega-cap earnings, Adani-style episodes).
If a date cannot be inferred from inputs but is a well-known public event within FP-L, you may supply the **public record date** and add “(public record)”.
If still unknown, write: “(no explicit date available in source)”.
Current Input: "{current_context_text}"

---

# Part 3 — Historical Mirrors (sorted by 30d Return: Good → Neutral → Bad), then Multi-Scenario Projection
*In the mirror of the past, the shape of the future is revealed.*
(Meta — do not print) Voice & angle: **Similarity column must copy the numeric value from the analog list verbatim — no blanks or "—".** Explain why each mirror is "affine" to today (stance: policy rhythm, liquidity tier, crowding) before using it. Three scenarios = up / flat / down; each gives Cause → Sentiment → Control in 2–3 sentences and ends with "Switch trigger: …" (a falsifiable threshold that upgrades/downgrades the path).


## 3.1 Historical Mirrors Table (FP-L vs FP-R narratives)
Using the analog list below:
{historical_mirror_details}

Construct a **5-row Markdown table** with columns:
`HFP-L Week Start | Similarity |  HFP-L (Historical Initial Narrative) | HFP-R Month Start(HFP-L + 6d) |30d Return |HFP-R (Historical Outcome Narrative)`
**Order rows by return bucket:** Positive (>2%) first, then Neutral ([-2%, 2%]), then Negative (<-2%).
**Fill “Similarity” with the exact numeric “Similarity Score” values provided above (e.g., 0.893). Do not output em dashes or placeholders.**
Fill FP-L/FP-R cells with concise one-liners (no long prose).

## 3.2 Multi-Scenario Projection (strictly historical-based, not a forecast)
(Meta — do not print) Template for the last line in each scenario: **Switch trigger:** if {observable} crosses {threshold} within {window}, {path upgrade/downgrade}; else maintain baseline.
For **each** scenario, include **one explicit line**: *FP-L: <initial setup> → FP-R: <subsequent outcome>*, then 2–3 sentences on mechanism.
- **Negative Scenario:** cite refs; include the FP-L→FP-R line; then mechanism (2–3 sentences).
- **Neutral Scenario:** cite refs; include the FP-L→FP-R line; then mechanism (2–3 sentences).
- **Positive Scenario:** cite refs; include the FP-L→FP-R line; then mechanism (2–3 sentences).

---

# Part 4 — Socratic Strategy Questions
*By observing the past, we test the future. By knowing others, we know ourselves.*
(Meta — do not print) Voice & angle: For each question, start by restating the investor's likely hypothesis, then give a minimal test (metric + window) and a "Falsifier:" line ("if ¬M within W, retire this view"). Offer options; never pressure.

We ask these because history shows they are decisive in similar regimes. Answering them clarifies assumptions and improves decisions:
1) **Fed stance:** nearer to late-1994 data-dependent pause or late-2018 autopilot hawkishness? *(Historically dictates major shifts.)*
2) **Optimism quality:** genuine macro strength (e.g., mid-2005) or premature pivot-bet vulnerability (e.g., late-2007)? *(Misjudged optimism often precedes drawdowns.)*
3) **Single decisive indicator:** labor strength, inflation trajectory, or credit conditions? *(Each historically correlates with the next regime.)*

---

# Part 5 — Extreme Risk Watchlist in FP-R (no advice)
*A crisis begins as a hairline crack, yet grows to the size of a mountain.*
(Meta — do not print) This section lists risks and triggers ONLY. No advice, no “observe/do-nothing” statement, no portfolio verbs. 
Output at most TWO “Risk Trigger Lines”. Each line format:

• <Risk name> — <Why it matters, 1 clause> — Trigger: <observable + threshold + time window> — Stand-down: <falsifier condition that retires this risk>.


Examples (format only, do not copy the content):
• Policy whiplash — FOMC tone can invert the narrative swiftly, like happend in 2007-09(a HFP-R period)— Trigger: ≥3 Fed officials repeat “ongoing increases” within 7 days — Stand-down: two officials explicitly flag “pause” or “two-sided risks”.
• Credit tightening pulse — funding stress often precedes earnings downgrades, like happend in 2019-02(a HFP-R period)— Trigger: HY spread widens ≥50 bps within 10 trading days — Stand-down: spread retraces ≥70% of the widening.

(Produce up to TWO lines for this week. No other text.)

**Complete all sections. Refer to FP-L/FP-R explicitly where relevant.**'''
,
              '{target_date_param}', CAST(target_date_param AS STRING)
            ),
            '{input_summary_table}', (SELECT table_string FROM input_summary_table)
          ),
          '{quantitative_summary}',
          (SELECT CONCAT('- ', CAST(ROUND(weighted_expected_return * 100, 2) AS STRING), '%') FROM quantitative_metrics)
        ),
        '{current_context_text}', (SELECT narrative_text FROM current_narrative_and_embedding)
      ),
      '{historical_mirror_details}', ''
    ) AS prompt,
    (SELECT STRING_AGG(
              CONCAT('On the week of ', CAST(week_start_date AS STRING),
                     ' (Similarity Score: ', CAST(ROUND(similarity, 3) AS STRING),
                     '), the market subsequently ', IF(return_30d > 0, 'rose', 'fell'),
                     ' by ', CAST(ABS(ROUND(return_30d * 100, 2)) AS STRING), '%.'),
              '\n')
     FROM mirror_outcomes) AS historical_mirror_details_text
  FROM (SELECT 1)
  CROSS JOIN quantitative_metrics
)

SELECT
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
         ),
         '\n'
       )
     FROM mirror_outcomes)
  ) AS input_prompt,
  llm_output.ml_generate_text_llm_result AS counsellor_report
FROM
  final_prompt p,
  ML.GENERATE_TEXT(
    MODEL `my-project-sep-16-472318.historical_mirror_mvp.gemini_model`,
    (SELECT REPLACE(
       p.prompt,
       '{historical_mirror_details}',
       p.historical_mirror_details_text
     ) AS prompt FROM final_prompt p),
    STRUCT(0.3 AS temperature, 8192 AS max_output_tokens, TRUE AS flatten_json_output)
  ) AS llm_output;
