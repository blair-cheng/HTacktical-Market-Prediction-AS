-- ==================================================================
-- 历史之镜 (Mirror of History) - 增强版历史镜像中间输出表设计
-- 存储包含HFP-L和is_pre_black_swan标识的结构化数据
-- ==================================================================
-- 核心概念：将LLM生成的报告中的历史镜像数据提取为结构化表格，包含前后时间窗口分析

-- 创建增强版历史镜像中间输出表
CREATE OR REPLACE TABLE `YOUR_PROJECT_ID.historical_mirror_mvp.historical_mirrors_enhanced` (
  -- 目标预测日期
  target_date DATE NOT NULL,
  
  -- FP-L 窗口 (Forward Prediction - Left)
  fp_l_start_date DATE NOT NULL,
  fp_l_end_date DATE NOT NULL,
  
  -- FP-R 窗口 (Forward Prediction - Right) 
  fp_r_start_date DATE NOT NULL,
  fp_r_end_date DATE NOT NULL,
  
  -- 加权预期收益率
  weighted_expected_return FLOAT64,
  
  -- 报告来源
  report_source STRING,
  
  -- 历史镜像1 (Historical Forward Prediction - Left & Right 1)
  hfp_l1_start_date DATE,
  hfp_l1_end_date DATE,
  hfp_l1_is_pre_black_swan BOOL,
  hfp_r1_start_date DATE,
  hfp_r1_end_date DATE,
  hfp_r1_similarity FLOAT64,
  hfp_r1_return_30d FLOAT64,
  hfp_r1_signal INT64, -- 1=positive, -1=negative, 0=neutral
  
  -- 历史镜像2
  hfp_l2_start_date DATE,
  hfp_l2_end_date DATE,
  hfp_l2_is_pre_black_swan BOOL,
  hfp_r2_start_date DATE,
  hfp_r2_end_date DATE,
  hfp_r2_similarity FLOAT64,
  hfp_r2_return_30d FLOAT64,
  hfp_r2_signal INT64,
  
  -- 历史镜像3
  hfp_l3_start_date DATE,
  hfp_l3_end_date DATE,
  hfp_l3_is_pre_black_swan BOOL,
  hfp_r3_start_date DATE,
  hfp_r3_end_date DATE,
  hfp_r3_similarity FLOAT64,
  hfp_r3_return_30d FLOAT64,
  hfp_r3_signal INT64,
  
  -- 历史镜像4
  hfp_l4_start_date DATE,
  hfp_l4_end_date DATE,
  hfp_l4_is_pre_black_swan BOOL,
  hfp_r4_start_date DATE,
  hfp_r4_end_date DATE,
  hfp_r4_similarity FLOAT64,
  hfp_r4_return_30d FLOAT64,
  hfp_r4_signal INT64,
  
  -- 历史镜像5
  hfp_l5_start_date DATE,
  hfp_l5_end_date DATE,
  hfp_l5_is_pre_black_swan BOOL,
  hfp_r5_start_date DATE,
  hfp_r5_end_date DATE,
  hfp_r5_similarity FLOAT64,
  hfp_r5_return_30d FLOAT64,
  hfp_r5_signal INT64,
  
  -- 汇总统计
  positive_scenario_count INT64,
  negative_scenario_count INT64,
  neutral_scenario_count INT64,
  
  -- 元数据
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
  
)
OPTIONS(
  description="Enhanced historical mirrors analysis intermediate output table - stores structured data with HFP-L and black swan pre-indicators"
);

-- 创建索引以提高查询性能
CREATE INDEX IF NOT EXISTS idx_enhanced_historical_mirrors_target_date 
ON `YOUR_PROJECT_ID.historical_mirror_mvp.historical_mirrors_enhanced` (target_date);

CREATE INDEX IF NOT EXISTS idx_enhanced_historical_mirrors_fp_l_start 
ON `YOUR_PROJECT_ID.historical_mirror_mvp.historical_mirrors_enhanced` (fp_l_start_date);

CREATE INDEX IF NOT EXISTS idx_enhanced_historical_mirrors_black_swan 
ON `YOUR_PROJECT_ID.historical_mirror_mvp.historical_mirrors_enhanced` (hfp_l1_is_pre_black_swan, hfp_l2_is_pre_black_swan, hfp_l3_is_pre_black_swan, hfp_l4_is_pre_black_swan, hfp_l5_is_pre_black_swan);

-- 示例数据插入 (基于2023-02-20报告)
INSERT INTO `YOUR_PROJECT_ID.historical_mirror_mvp.historical_mirrors_enhanced` (
  target_date,
  fp_l_start_date, fp_l_end_date,
  fp_r_start_date, fp_r_end_date,
  weighted_expected_return, report_source,
  hfp_l1_start_date, hfp_l1_end_date, hfp_l1_is_pre_black_swan,
  hfp_r1_start_date, hfp_r1_end_date, hfp_r1_similarity, hfp_r1_return_30d, hfp_r1_signal,
  hfp_l2_start_date, hfp_l2_end_date, hfp_l2_is_pre_black_swan,
  hfp_r2_start_date, hfp_r2_end_date, hfp_r2_similarity, hfp_r2_return_30d, hfp_r2_signal,
  hfp_l3_start_date, hfp_l3_end_date, hfp_l3_is_pre_black_swan,
  hfp_r3_start_date, hfp_r3_end_date, hfp_r3_similarity, hfp_r3_return_30d, hfp_r3_signal,
  hfp_l4_start_date, hfp_l4_end_date, hfp_l4_is_pre_black_swan,
  hfp_r4_start_date, hfp_r4_end_date, hfp_r4_similarity, hfp_r4_return_30d, hfp_r4_signal,
  hfp_l5_start_date, hfp_l5_end_date, hfp_l5_is_pre_black_swan,
  hfp_r5_start_date, hfp_r5_end_date, hfp_r5_similarity, hfp_r5_return_30d, hfp_r5_signal,
  positive_scenario_count, negative_scenario_count, neutral_scenario_count
) VALUES (
  '2023-02-20',
  '2023-02-20', '2023-02-26',
  '2023-02-27', '2023-03-28',
  -0.004, 'mirror_report_2023-02-20.md',
  -- HFP-L1 & HFP-R1: 2004-05-17 (Positive)
  '2004-04-17', '2004-05-16', FALSE,
  '2004-05-17', '2004-06-16', 0.893, 0.035, 1,
  -- HFP-L2 & HFP-R2: 1994-11-14 (Neutral) 
  '1994-10-15', '1994-11-13', FALSE,
  '1994-11-14', '1994-12-14', 0.912, 0.012, 0,
  -- HFP-L3 & HFP-R3: 2018-10-29 (Negative)
  '2018-09-29', '2018-10-28', FALSE,
  '2018-10-29', '2018-11-28', 0.885, -0.015, -1,
  -- HFP-L4 & HFP-R4: 2007-07-23 (Negative)
  '2007-06-23', '2007-07-22', FALSE,
  '2007-07-23', '2007-08-22', 0.901, -0.048, -1,
  -- HFP-L5 & HFP-R5: 2000-09-25 (Negative)
  '2000-08-26', '2000-09-24', FALSE,
  '2000-09-25', '2000-10-25', 0.879, -0.072, -1,
  -- 汇总统计
  1, 3, 1
);

-- 查询示例：获取所有增强版历史镜像数据
SELECT 
  target_date,
  fp_l_start_date, fp_l_end_date,
  fp_r_start_date, fp_r_end_date,
  weighted_expected_return,
  hfp_l1_start_date, hfp_l1_end_date, hfp_l1_is_pre_black_swan,
  hfp_r1_start_date, hfp_r1_similarity, hfp_r1_return_30d, hfp_r1_signal,
  hfp_l2_start_date, hfp_l2_end_date, hfp_l2_is_pre_black_swan,
  hfp_r2_start_date, hfp_r2_similarity, hfp_r2_return_30d, hfp_r2_signal,
  hfp_l3_start_date, hfp_l3_end_date, hfp_l3_is_pre_black_swan,
  hfp_r3_start_date, hfp_r3_similarity, hfp_r3_return_30d, hfp_r3_signal,
  hfp_l4_start_date, hfp_l4_end_date, hfp_l4_is_pre_black_swan,
  hfp_r4_start_date, hfp_r4_similarity, hfp_r4_return_30d, hfp_r4_signal,
  hfp_l5_start_date, hfp_l5_end_date, hfp_l5_is_pre_black_swan,
  hfp_r5_start_date, hfp_r5_similarity, hfp_r5_return_30d, hfp_r5_signal,
  positive_scenario_count, negative_scenario_count, neutral_scenario_count
FROM `YOUR_PROJECT_ID.historical_mirror_mvp.historical_mirrors_enhanced`
ORDER BY target_date;

-- 查询示例：按黑天鹅前标识分组统计
SELECT 
  target_date,
  SUM(CASE WHEN hfp_l1_is_pre_black_swan THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_l2_is_pre_black_swan THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_l3_is_pre_black_swan THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_l4_is_pre_black_swan THEN 1 ELSE 0 END) +
  SUM(CASE WHEN hfp_l5_is_pre_black_swan THEN 1 ELSE 0 END) AS total_pre_black_swan_count,
  
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
FROM `YOUR_PROJECT_ID.historical_mirror_mvp.historical_mirrors_enhanced`
GROUP BY target_date
ORDER BY target_date;

-- 查询示例：黑天鹅前标识与信号关联分析
SELECT 
  CASE 
    WHEN (hfp_l1_is_pre_black_swan OR hfp_l2_is_pre_black_swan OR hfp_l3_is_pre_black_swan OR hfp_l4_is_pre_black_swan OR hfp_l5_is_pre_black_swan) 
    THEN 'Has Pre-Black Swan' 
    ELSE 'No Pre-Black Swan' 
  END AS black_swan_status,
  
  AVG(weighted_expected_return) as avg_expected_return,
  AVG(positive_scenario_count) as avg_positive_signals,
  AVG(negative_scenario_count) as avg_negative_signals,
  AVG(neutral_scenario_count) as avg_neutral_signals,
  COUNT(*) as report_count
FROM `YOUR_PROJECT_ID.historical_mirror_mvp.historical_mirrors_enhanced`
GROUP BY black_swan_status;

-- 查询示例：相似度与黑天鹅前标识的关系
SELECT 
  hfp_l1_is_pre_black_swan,
  AVG(hfp_r1_similarity) as avg_similarity_r1,
  AVG(hfp_r1_return_30d) as avg_return_r1,
  COUNT(*) as count
FROM `YOUR_PROJECT_ID.historical_mirror_mvp.historical_mirrors_enhanced`
WHERE hfp_r1_similarity IS NOT NULL
GROUP BY hfp_l1_is_pre_black_swan;


