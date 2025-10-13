-- ==================================================================
-- 历史之镜 (Mirror of History) - 中间输出表设计
-- 存储历史镜像分析的结构化数据
-- ==================================================================
-- 核心概念：将LLM生成的报告中的历史镜像数据提取为结构化表格

-- 创建历史镜像中间输出表
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.historical_mirror_mvp.historical_mirrors_output` (
  -- 目标预测日期
  target_date DATE NOT NULL,
  
  -- FP-L 窗口 (Forward Prediction - Left)
  fp_l_start_date DATE NOT NULL,
  fp_l_end_date DATE NOT NULL,
  
  -- FP-R 窗口 (Forward Prediction - Right) 
  fp_r_start_date DATE NOT NULL,
  fp_r_end_date DATE NOT NULL,
  
  -- 历史镜像1 (Historical Forward Prediction - Right 1)
  hfp_r1_start_date DATE,
  hfp_r1_end_date DATE,
  hfp_r1_similarity FLOAT64,
  hfp_r1_return_30d FLOAT64,
  hfp_r1_signal INT64, -- 1=positive, -1=negative, 0=neutral
  
  -- 历史镜像2
  hfp_r2_start_date DATE,
  hfp_r2_end_date DATE,
  hfp_r2_similarity FLOAT64,
  hfp_r2_return_30d FLOAT64,
  hfp_r2_signal INT64,
  
  -- 历史镜像3
  hfp_r3_start_date DATE,
  hfp_r3_end_date DATE,
  hfp_r3_similarity FLOAT64,
  hfp_r3_return_30d FLOAT64,
  hfp_r3_signal INT64,
  
  -- 历史镜像4
  hfp_r4_start_date DATE,
  hfp_r4_end_date DATE,
  hfp_r4_similarity FLOAT64,
  hfp_r4_return_30d FLOAT64,
  hfp_r4_signal INT64,
  
  -- 历史镜像5
  hfp_r5_start_date DATE,
  hfp_r5_end_date DATE,
  hfp_r5_similarity FLOAT64,
  hfp_r5_return_30d FLOAT64,
  hfp_r5_signal INT64,
  
  -- 汇总统计
  weighted_expected_return FLOAT64,
  positive_scenario_count INT64,
  negative_scenario_count INT64,
  neutral_scenario_count INT64,
  
  -- 元数据
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  report_source STRING -- 来源报告文件名
  
)
OPTIONS(
  description="Historical mirrors analysis intermediate output table - stores structured data from LLM reports"
);

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_historical_mirrors_target_date 
ON `YOUR_PROJECT_ID.historical_mirror_mvp.historical_mirrors_output` (target_date);

CREATE INDEX IF NOT EXISTS idx_historical_mirrors_fp_l_start 
ON `YOUR_PROJECT_ID.historical_mirror_mvp.historical_mirrors_output` (fp_l_start_date);

-- 示例数据插入 (基于2023-02-20报告)
INSERT INTO `YOUR_PROJECT_ID.historical_mirror_mvp.historical_mirrors_output` (
  target_date,
  fp_l_start_date, fp_l_end_date,
  fp_r_start_date, fp_r_end_date,
  hfp_r1_start_date, hfp_r1_end_date, hfp_r1_similarity, hfp_r1_return_30d, hfp_r1_signal,
  hfp_r2_start_date, hfp_r2_end_date, hfp_r2_similarity, hfp_r2_return_30d, hfp_r2_signal,
  hfp_r3_start_date, hfp_r3_end_date, hfp_r3_similarity, hfp_r3_return_30d, hfp_r3_signal,
  hfp_r4_start_date, hfp_r4_end_date, hfp_r4_similarity, hfp_r4_return_30d, hfp_r4_signal,
  hfp_r5_start_date, hfp_r5_end_date, hfp_r5_similarity, hfp_r5_return_30d, hfp_r5_signal,
  weighted_expected_return, positive_scenario_count, negative_scenario_count, neutral_scenario_count,
  report_source
) VALUES (
  '2023-02-20',
  '2023-02-20', '2023-02-26',
  '2023-02-27', '2023-03-28',
  -- HFP-R1: 2004-05-17 (Positive)
  '2004-05-17', '2004-06-14', 0.893, 0.035, 1,
  -- HFP-R2: 1994-11-14 (Neutral) 
  '1994-11-14', '1994-12-12', 0.912, 0.012, 0,
  -- HFP-R3: 2018-10-29 (Negative)
  '2018-10-29', '2018-11-26', 0.885, -0.015, -1,
  -- HFP-R4: 2007-07-23 (Negative)
  '2007-07-23', '2007-08-20', 0.901, -0.048, -1,
  -- HFP-R5: 2000-09-25 (Negative)
  '2000-09-25', '2000-10-23', 0.879, -0.072, -1,
  -- 汇总统计
  -0.004, 1, 3, 1,
  'mirror_report_2023-02-20.md'
);

-- 查询示例：获取所有历史镜像数据
SELECT 
  target_date,
  fp_l_start_date, fp_l_end_date,
  fp_r_start_date, fp_r_end_date,
  hfp_r1_start_date, hfp_r1_similarity, hfp_r1_return_30d, hfp_r1_signal,
  hfp_r2_start_date, hfp_r2_similarity, hfp_r2_return_30d, hfp_r2_signal,
  hfp_r3_start_date, hfp_r3_similarity, hfp_r3_return_30d, hfp_r3_signal,
  hfp_r4_start_date, hfp_r4_similarity, hfp_r4_return_30d, hfp_r4_signal,
  hfp_r5_start_date, hfp_r5_similarity, hfp_r5_return_30d, hfp_r5_signal,
  weighted_expected_return,
  positive_scenario_count,
  negative_scenario_count,
  neutral_scenario_count
FROM `YOUR_PROJECT_ID.historical_mirror_mvp.historical_mirrors_output`
ORDER BY target_date;

-- 查询示例：按信号类型分组统计
SELECT 
  target_date,
  SUM(CASE WHEN hfp_r1_signal = 1 THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_r2_signal = 1 THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_r3_signal = 1 THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_r4_signal = 1 THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_r5_signal = 1 THEN 1 ELSE 0 END) AS total_positive_signals,
  
  SUM(CASE WHEN hfp_r1_signal = -1 THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_r2_signal = -1 THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_r3_signal = -1 THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_r4_signal = -1 THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_r5_signal = -1 THEN 1 ELSE 0 END) AS total_negative_signals,
  
  SUM(CASE WHEN hfp_r1_signal = 0 THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_r2_signal = 0 THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_r3_signal = 0 THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_r4_signal = 0 THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_r5_signal = 0 THEN 1 ELSE 0 END) AS total_neutral_signals
FROM `YOUR_PROJECT_ID.historical_mirror_mvp.historical_mirrors_output`
GROUP BY target_date
ORDER BY target_date;


