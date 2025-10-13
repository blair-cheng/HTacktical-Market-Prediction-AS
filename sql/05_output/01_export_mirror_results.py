#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, json, re
from pathlib import Path
from datetime import datetime

PATTERN_START = re.compile(r"Target Week FP-L start:\s*(\d{4}-\d{2}-\d{2})")

def extract_start_date(text: str) -> str:
    """从文本中提取目标周开始日期"""
    m = PATTERN_START.search(text or "")
    if m:
        return m.group(1)
    m2 = re.search(r"(\d{4}-\d{2}-\d{2})", text or "")
    return m2.group(1) if m2 else datetime.today().strftime("%Y-%m-%d")

def main():
    ap = argparse.ArgumentParser(description="Save input_prompt & counsellor_report to Markdown.")
    ap.add_argument("--json", help="Path to JSON file containing input_prompt & counsellor_report", required=True)
    ap.add_argument("--outdir", default="./out", help="Output directory")
    args = ap.parse_args()

    # 读取JSON数据
    data = json.loads(Path(args.json).read_text(encoding="utf-8"))
    # 支持 [[{...}]] 或 [{...}] 或 {...}
    if isinstance(data, list) and len(data) > 0:
        if isinstance(data[0], list) and len(data[0]) > 0:
            rec = data[0][0]  # 处理 [[{...}]] 格式
        else:
            rec = data[0]     # 处理 [{...}] 格式
    elif isinstance(data, dict):
        rec = data
    else:
        print("Error: Invalid JSON format")
        return
    
    input_prompt = rec.get("input_prompt", "")
    report = rec.get("counsellor_report", "")

    # 提取目标周开始日期
    start = extract_start_date(input_prompt)
    
    # 创建输出目录
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    
    # 写入文件
    (outdir / f"mirror_input_prompt_{start}.md").write_text(input_prompt, encoding="utf-8")
    (outdir / f"mirror_report_{start}.md").write_text(f"# Mirror of History — Target Week {start}\n\n{report}", encoding="utf-8")

    print(f"✓ Wrote {outdir}/mirror_input_prompt_{start}.md")
    print(f"✓ Wrote {outdir}/mirror_report_{start}.md")

if __name__ == "__main__":
    main()
