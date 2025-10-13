#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, json, re, os, sys
from pathlib import Path
from datetime import datetime, date
from typing import Optional, Dict, Any, List

# 尝试导入BigQuery客户端
try:
    from google.cloud import bigquery
    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False

PATTERN_START = re.compile(r"Target Week FP-L start:\s*(\d{4}-\d{2}-\d{2})")

# 2023年所有周一日期列表（来自SQL文件的原始格式）
AVAILABLE_DATES_2023 = [
    "2023-01-02", "2023-01-09", "2023-01-16", "2023-01-23", "2023-01-30", 
    "2023-02-06", "2023-02-13", "2023-02-20", "2023-02-27",
    "2023-03-06", "2023-03-13", "2023-03-20", "2023-03-27", 
    "2023-04-03", "2023-04-10", "2023-04-17", "2023-04-24", 
    "2023-05-01", "2023-05-08", "2023-05-15", "2023-05-22", "2023-05-29", 
    "2023-06-05", "2023-06-12", "2023-06-19", "2023-06-26", 
    "2023-07-03", "2023-07-10", "2023-07-17", "2023-07-24", "2023-07-31", 
    "2023-08-07", "2023-08-14", "2023-08-21", "2023-08-28", 
    "2023-09-04", "2023-09-11", "2023-09-18", "2023-09-25", 
    "2023-10-02", "2023-10-09", "2023-10-16", "2023-10-23", "2023-10-30", 
    "2023-11-06", "2023-11-13", "2023-11-20", "2023-11-27", 
    "2023-12-04", "2023-12-11", "2023-12-18", "2023-12-25"
]

def extract_start_date(text: str) -> str:
    """从文本中提取目标周开始日期"""
    m = PATTERN_START.search(text or "")
    if m:
        return m.group(1)
    m2 = re.search(r"(\d{4}-\d{2}-\d{2})", text or "")
    return m2.group(1) if m2 else datetime.today().strftime("%Y-%m-%d")

def validate_date(date_str: str) -> bool:
    """验证日期是否为2023年的周一日期"""
    return date_str in AVAILABLE_DATES_2023

def parse_date_range(date_input: str) -> List[str]:
    """解析日期范围输入，支持逗号分隔的多个日期"""
    dates = [d.strip() for d in date_input.split(',')]
    valid_dates = []
    
    for date_str in dates:
        if validate_date(date_str):
            valid_dates.append(date_str)
        else:
            print(f"⚠️  Warning: {date_str} is not a valid 2023 Monday date")
    
    return valid_dates

def display_available_dates() -> None:
    """显示可用的日期列表"""
    print("\n📅 Available 2023 Monday dates:")
    for i, date_str in enumerate(AVAILABLE_DATES_2023, 1):
        marker = " ← Current" if date_str == "2023-02-20" else ""
        print(f"  {i:2d}) {date_str}{marker}")
    print()

def ask_continue() -> bool:
    """询问是否继续执行下一个日期"""
    try:
        while True:
            response = input("Continue with next date? [Y/n]: ").strip().lower()
            if response in ['', 'y', 'yes']:
                return True
            elif response in ['n', 'no']:
                return False
            else:
                print("Please enter 'y' for yes or 'n' for no.")
    except (EOFError, KeyboardInterrupt):
        # 在非交互式环境中或用户中断时，默认继续
        print(" (auto-continuing)")
        return True

def load_config() -> Dict[str, Any]:
    """加载配置文件"""
    config = {}
    
    # 从环境变量读取
    config['project_id'] = os.getenv('PROJECT_ID', 'YOUR_PROJECT_ID')
    config['sql_file'] = os.getenv('SQL_FILE', 'sql/04_inference/01_master_inference_query Epigraph/Philosophy Edition).sql')
    
    # 尝试从配置文件读取
    config_files = [
        'config/database.yaml',
        'config/model_params.yaml',
        '.env'
    ]
    
    for config_file in config_files:
        if Path(config_file).exists():
            try:
                if config_file.endswith('.yaml'):
                    import yaml
                    with open(config_file, 'r', encoding='utf-8') as f:
                        yaml_config = yaml.safe_load(f)
                        if 'project_id' in yaml_config:
                            config['project_id'] = yaml_config['project_id']
                elif config_file == '.env':
                    with open(config_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            if line.startswith('PROJECT_ID='):
                                config['project_id'] = line.split('=', 1)[1].strip()
                                break
            except Exception as e:
                print(f"Warning: Could not read config from {config_file}: {e}")
    
    return config

def execute_bigquery_query(sql_content: str, project_id: str) -> str:
    """执行BigQuery查询并返回JSON结果"""
    if not BIGQUERY_AVAILABLE:
        raise ImportError("google-cloud-bigquery not installed. Install with: pip install google-cloud-bigquery")
    
    if project_id == 'YOUR_PROJECT_ID':
        raise ValueError("Please set PROJECT_ID environment variable or update config files")
    
    print(f"🔍 Connecting to BigQuery project: {project_id}")
    
    # 创建BigQuery客户端
    client = bigquery.Client(project=project_id)
    
    # 替换SQL中的项目ID占位符
    sql_content = sql_content.replace('YOUR_PROJECT_ID', project_id)
    
    print("⚡ Executing 4.1 inference query...")
    
    # 执行查询
    query_job = client.query(sql_content)
    results = query_job.result()
    
    # 转换为JSON格式
    json_results = []
    for row in results:
        json_results.append(dict(row))
    
    print(f"✅ Query completed. Retrieved {len(json_results)} result(s)")
    
    # 返回JSON字符串
    return json.dumps(json_results, ensure_ascii=False, indent=2)

def process_json_to_markdown(json_data: str, outdir: str) -> None:
    """处理JSON数据并生成Markdown文件"""
    # 解析JSON数据
    data = json.loads(json_data)
    
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
    outdir_path = Path(outdir)
    outdir_path.mkdir(parents=True, exist_ok=True)
    
    # 写入文件
    (outdir_path / f"mirror_input_prompt_{start}.md").write_text(input_prompt, encoding="utf-8")
    (outdir_path / f"mirror_report_{start}.md").write_text(f"# Mirror of History — Target Week {start}\n\n{report}", encoding="utf-8")

    print(f"📄 Generated {outdir_path}/mirror_input_prompt_{start}.md")
    print(f"📄 Generated {outdir_path}/mirror_report_{start}.md")

def main():
    ap = argparse.ArgumentParser(description="Mirror of History - Enhanced Export Tool")
    
    # 执行模式选择
    execution_group = ap.add_mutually_exclusive_group(required=False)
    execution_group.add_argument("--execute-sql", action="store_true", 
                                help="Execute 4.1 SQL query directly")
    execution_group.add_argument("--json", 
                                help="Path to JSON file containing input_prompt & counsellor_report")
    
    # 其他参数
    ap.add_argument("--outdir", default="./reports/output", help="Output directory")
    ap.add_argument("--project-id", help="BigQuery project ID (overrides config)")
    ap.add_argument("--sql-file", help="Path to SQL file (overrides config)")
    ap.add_argument("--target-date", help="Target date for inference (format: YYYY-MM-DD)")
    ap.add_argument("--date-range", help="Comma-separated list of dates to process (e.g., 2023-01-30,2023-02-20,2023-03-06)")
    ap.add_argument("--show-dates", action="store_true", help="Show available 2023 Monday dates")
    
    args = ap.parse_args()
    
    # 如果只是显示日期列表
    if args.show_dates:
        display_available_dates()
        return
    
    # 如果没有指定执行模式，默认执行SQL
    if not args.execute_sql and not args.json:
        print("🚀 Default Mode: Executing 4.1 SQL query directly")
        args.execute_sql = True
    
    if args.execute_sql:
        # 执行SQL模式
        print("🚀 Enhanced Mode: Executing 4.1 SQL query directly")
        
        # 加载配置
        config = load_config()
        
        # 覆盖配置
        if args.project_id:
            config['project_id'] = args.project_id
        if args.sql_file:
            config['sql_file'] = args.sql_file
            
        # 检查SQL文件
        sql_file_path = Path(config['sql_file'])
        if not sql_file_path.exists():
            print(f"❌ SQL file not found: {sql_file_path}")
            print("Available SQL files:")
            sql_dir = Path("sql/04_inference")
            if sql_dir.exists():
                for f in sql_dir.glob("*.sql"):
                    print(f"  - {f}")
            return
        
        # 读取SQL文件
        print(f"📖 Reading SQL file: {sql_file_path}")
        sql_content = sql_file_path.read_text(encoding="utf-8")
        
        # 确定要处理的日期列表
        dates_to_process = []
        
        if args.date_range:
            # 处理日期范围
            dates_to_process = parse_date_range(args.date_range)
            if not dates_to_process:
                print("❌ No valid dates provided in date range")
                return
        elif args.target_date:
            # 处理单个日期
            if validate_date(args.target_date):
                dates_to_process = [args.target_date]
            else:
                print(f"❌ Invalid date: {args.target_date}")
                print("💡 Use --show-dates to see available dates")
                return
        else:
            # 默认使用当前日期
            default_date = "2023-02-20"
            dates_to_process = [default_date]
            print(f"📅 Using default date: {default_date}")
        
        print(f"📋 Processing {len(dates_to_process)} date(s): {', '.join(dates_to_process)}")
        
        # 逐个处理每个日期
        for i, target_date in enumerate(dates_to_process, 1):
            print(f"\n🔄 Processing date {i}/{len(dates_to_process)}: {target_date}")
            
            # 更新SQL中的日期参数
            current_sql = re.sub(
                r"DECLARE target_date_param DATE DEFAULT '[^']*';",
                f"DECLARE target_date_param DATE DEFAULT '{target_date}';",
                sql_content
            )
            
            try:
                # 执行BigQuery查询
                json_result = execute_bigquery_query(current_sql, config['project_id'])
                
                # 处理结果并生成Markdown
                print("📝 Processing results and generating Markdown files...")
                process_json_to_markdown(json_result, args.outdir)
                
                print(f"✅ Completed processing {target_date}")
                
                # 如果不是最后一个日期，询问是否继续
                if i < len(dates_to_process):
                    if not ask_continue():
                        print("⏹️  Stopping at user request")
                        break
                        
            except Exception as e:
                print(f"❌ Error processing {target_date}: {e}")
                if "google-cloud-bigquery not installed" in str(e):
                    print("💡 Install BigQuery client: pip install google-cloud-bigquery")
                elif "PROJECT_ID" in str(e):
                    print("💡 Set PROJECT_ID environment variable or update config files")
                
                # 询问是否继续处理下一个日期
                if i < len(dates_to_process):
                    if ask_continue():
                        continue
                    else:
                        break
        
        print("\n🎉 All processing completed!")
            
    else:
        # 处理JSON模式 (原有功能)
        print("📄 Legacy Mode: Processing existing JSON file")
        
        if not Path(args.json).exists():
            print(f"❌ JSON file not found: {args.json}")
            return
            
        # 读取JSON数据
        data = json.loads(Path(args.json).read_text(encoding="utf-8"))
        process_json_to_markdown(json.dumps(data), args.outdir)
        print("🎉 Complete! All files generated successfully.")

if __name__ == "__main__":
    main()
