# 历史之镜 (Mirror of History)

> BigQuery 原生的历史类比分析 AI 代理，通过向量检索 + 生成模型为金融决策者提供多情景参谋报告。

## 项目定位
- 将市场情境映射为「整体情境指纹」，在历史数据库中寻找高度相似的周度片段。
- 生成包含正/中/负三类历史情景的战略简报，帮助训练决策直觉，而非直接给出买卖信号。
- 所有核心流水线（特征工程、向量生成、推理）均以 SQL 在 BigQuery 中实现，并可批量回测。

更多背景理念见 `docs/philosophy.md`（本次整理后提供）。

## 系统架构
1. **数据摄取**：加载标准化的市场、宏观、新闻数据到 `historical_mirror_mvp` 数据集中。
2. **特征工程**：`sql/02_feature_engineering/01_create_fused_narratives_history.sql` 聚合周度指标并拼接叙事标题，形成「整体情境指纹」。
3. **向量生成**：`sql/03_embedding_generation/*.sql` 调用 `ML.GENERATE_EMBEDDING` 构建向量库 `narrative_vectors_history`。
4. **历史镜像检索与推理**：`sql/04_inference/01_master_inference_query Epigraph/Philosophy Edition).sql` / `..._specialist.sql`
   - 为当前周生成向量 → `VECTOR_SEARCH` 检索最相似历史 → 拼接上下文 → `ML.GENERATE_TEXT` 输出五部分参谋报告。
5. **回测与评估**：`backtesting/` 下脚本批量运行推理、分析结果，以「类比质量与情景多样性」为核心指标。

架构详解与流程图见 `docs/setup.md`。

## 仓库结构
```
mirror-of-history/
├─ README.md
├─ config/
│  ├─ database.yaml.template
│  └─ model_params.yaml
├─ backtesting/
│  ├─ run_backtest.py
│  ├─ analyze_results.py
│  ├─ compare_strategies.py
│  └─ README.md
├─ data/                  # 经过清洗的示例数据与向量，详见 data/README.md
                        # 大型数据集 (raw_news_whitelisted, fused_narratives_history) 
                        # 已上传至公共 GCS: gs://dataset_for_mirror_of_history/
├─ reports/               # 示例参谋报告与可视化
├─ sql/
│  ├─ 01_raw_ingest/
│  │  └─ 01_load_raw_prices.sql
│  ├─ 02_feature_engineering/
│  │  └─ 01_create_fused_narratives_history.sql
│  ├─ 03_embedding_generation/
│  │  ├─ 01_create_embedding_model.sql
│  │  └─ 02_generate_narrative_vectors.sql
│  ├─ 04_inference/
│  │  ├─ 01_master_inference_query Epigraph/
│  │  │  └─ Philosophy Edition).sql
│  │  └─ 01_master_inference_query_specialist.sql
│  └─ 05_output/
│     └─ 01_export_mirror_results.py
├─ requirements.txt
├─ results.json (示例输出)
└─ v3_results.json 等示例结果文件
```

## 快速开始

### 环境配置
```bash
# 1. 激活conda环境
export PATH="$HOME/anaconda3/bin:$PATH"
source ~/.zshrc
conda activate mirror-history

# 2. 安装依赖（如果尚未安装）
pip install google-cloud-bigquery pandas db-dtypes

# 3. 验证环境
python -c "import google.cloud.bigquery; print('环境配置成功')"
```

### 数据准备
项目包含两部分数据：
- **本地数据** (`data/` 文件夹): 市场数据、向量库、回测结果 (~11MB)
- **云端数据** (GCS): 大型数据集已上传至公共bucket
  ```bash
  # 大型数据集位置
  gs://dataset_for_mirror_of_history/raw_news_whitelisted/     # 8.95 GiB
  gs://dataset_for_mirror_of_history/fused_narratives_history/ # 3.37 GiB
  gs://dataset_for_mirror_of_history/raw_prices                # 市场数据
  gs://dataset_for_mirror_of_history/domain_whitelist          # 域名白名单
  ```

### 下载BigQuery数据到本地
```bash
# 下载已创建的表数据到data文件夹
python scripts/download_bigquery_tables.py

# 下载的表包括：
# - raw_news_20250922_28_cleaned.csv (财经新闻)
# - fin_macro_20250922_28.csv (宏观数据)
# - fused_narratives_history_20250922_28.csv (融合叙事)
# - narrative_vectors_20250922_28.csv (向量数据)
```

### 运行4.1推理查询

#### 方法1: 使用GCS外部表 (推荐)
```sql
-- 1. 创建外部表指向GCS数据
CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT_ID.historical_mirror_mvp.raw_news_whitelisted`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dataset_for_mirror_of_history/raw_news_whitelisted/*']
);

CREATE OR REPLACE EXTERNAL TABLE `YOUR_PROJECT_ID.historical_mirror_mvp.fused_narratives_history`
OPTIONS (
  format = 'CSV', 
  uris = ['gs://dataset_for_mirror_of_history/fused_narratives_history/*']
);

-- 2. 运行4.1推理查询
-- 直接使用 sql/04_inference/01_master_inference_query Epigraph/Philosophy Edition).sql
```

#### 方法2: 直接查询GCS (无需创建表)
```sql
-- 修改4.1查询中的表引用
-- 将 YOUR_PROJECT_ID.historical_mirror_mvp.raw_news_whitelisted 
-- 改为 gs://dataset_for_mirror_of_history/raw_news_whitelisted/*
```

### 环境配置
- **BigQuery设置**: 需要创建embedding_model和gemini_model连接
- **权限**: 需要访问GCS公共bucket的权限
- **详细步骤**: 参考 `docs/setup.md`

## 评估框架
在新范式下，我们关注「类比质量」而非收益曲线：
- **叙事连贯性与情境相关性**：人工评审（1–5 分量表）。
- **情景多样性指数**：正/中/负情景分布是否均衡。
- **异常事件识别率**：能否捕捉危机或黑天鹅样本。

详细流程与统计指标见 `backtesting/README.md`。

## 数据与模型
- 嵌入模型默认使用 `text-embedding-004`，生成模型使用 `gemini-2.5-pro`，参数在 `config/model_params.yaml`。
- 成本提示集中在 `docs/setup.md` 的「费用与预算」章节，避免多处重复。

## 输出示例
`reports/` 目录提供单周报告（Markdown）和对比图（PNG），可作为模板验证生成格式。

## 免责声明
本项目面向研究与教学用途，不构成任何投资建议。

---

*历史之镜 —— 让 AI 成为你的历史参谋。*
