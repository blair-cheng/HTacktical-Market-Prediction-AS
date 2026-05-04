# 数据源计划

## 原则

- 本地优先。
- 原始数据可缓存。
- 每个字段有来源。
- 免费源先跑通，关键价格源未来可替换成付费/券商源。
- 不依赖 BigQuery。

## P0 数据

| 模块 | 第一选择 | 用途 | 风险 |
|---|---|---|---|
| ETF/资产价格 | Tiingo EOD 稳定版；Yahoo chart 开发备选 | `SPY`, `QQQ`, `IWM`, `TLT`, `IEF`, `SHY`, `GLD`, `UUP` | Yahoo 非正式且会限流；Stooq 现在需要 apikey；生产最好换 Tiingo/券商 |
| 通胀 | BLS CPI | `cpi` | 月频数据 forward-fill 到交易日 |
| 利率 | U.S. Treasury FiscalData | P0 只用一个 Treasury yield：`yield_10y` | 原始数据可缓存更多期限，但 embedding 不需要整条曲线 |
| 波动率 | Cboe VIX historical data | `VIX`, `VVIX`, `VIX9D` 等 | Cboe 声明不保证完全准确；仍是官方主源 |
| 恐贪指数 | CNN Fear & Greed Index | 先代替新闻标题，作为市场情绪/风险温度计 | 不是正式稳定 API；生产版最好重建底层组件 |
| 新闻情绪 | GDELT GKG | tone, themes, source, url, news volume | GKG 不一定有干净标题；标题可用 DOC/RSS 或 URL slug 补 |
| 公司事件 | SEC EDGAR | 个股版再用：10-K/10-Q/8-K/XBRL | ETF tactical 第一版不需要 |

## 第一版字段

每日一行 `market_state`：

```text
date
SPY, QQQ, IWM, TLT, IEF, SHY, GLD, UUP
VIX
yield_10y
cpi
fear_greed_score
fear_greed_delta
```

## 新闻方案

P0 可以先不用新闻标题，改用 Fear & Greed Index。

原因：

- 数据工程更轻。
- 日频更新。
- 已经包含市场动量、VIX、期权、宽度、避险需求等情绪 proxy。
- 适合先判断 regime 是否处在极端恐惧 / 极端贪婪。

限制：

- 它不是新闻语义。
- 它更像市场技术情绪合成指标。
- CNN 没有正式承诺的公开稳定 API。
- 后续需要用底层组件重建，避免依赖单一网页/内部接口。

Fear & Greed P0 字段：

```text
fear_greed_score
fear_greed_delta
fear_greed_zscore
fear_greed_is_extreme_fear
fear_greed_is_extreme_greed
```

P1 再接新闻标题。

新闻标题阶段不抓正文。

先取：

- date
- source/domain
- url
- title 或 URL-derived title
- tone
- themes

每日聚合：

- `news_count`
- `avg_tone`
- `tone_delta`
- `negative_count`
- `source_weighted_tone`
- `risk_theme_count`

白名单先用：

```text
reuters.com
bloomberg.com
cnbc.com
wsj.com
ft.com
marketwatch.com
```

## 推荐执行顺序

1. 价格：先让 portfolio return / fpR 跑起来。
2. CPI + Treasury 10Y + VIX：补真实 regime 特征。
3. Fear & Greed：先替代新闻标题。
4. GDELT 新闻：加 tone/volume/theme。
5. Treasury liquidity：再加流动性特征。
6. SEC：等扩展到个股再接。

## 价格源策略

开发阶段可以接受不完美免费源。

真钱跟踪阶段不建议只依赖 yfinance：

- 没有正式 SLA。
- 非官方。
- 容易被限流或改接口。

可行路线：

```text
dev fallback: Yahoo chart endpoint
research stable: Tiingo EOD
execution: broker / paid market data
```

Stooq 仍可作为备选，但现在 CSV 下载需要先在网页通过验证码获取 `apikey`。

脚本已支持 Tiingo：

```bash
python3 scripts/download_p0_data.py \
  --start 2005-01-01 \
  --end 2026-04-30 \
  --price-source tiingo \
  --tiingo-api-key YOUR_TOKEN
```

也可以先设置环境变量：

```bash
export TIINGO_API_KEY=YOUR_TOKEN
python3 scripts/download_p0_data.py --price-source tiingo
```
