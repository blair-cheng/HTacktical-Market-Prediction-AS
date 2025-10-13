#!/usr/bin/env python3
"""
运行4.1 V1版本生成2023-08-21到2023-10-16的报告
"""

import os
import sys
from datetime import datetime, timedelta
from google.cloud import bigquery
import pandas as pd

# 配置
PROJECT_ID = "my-project-sep-16-472318"
SQL_FILE_PATH = "../sql/04_inference/01_master_inference_query Epigraph/v1.Philosophy Edition) copy.sql"
OUTPUT_DIR = "v1_reports"

# 目标日期范围
START_DATE = "2023-08-21"
END_DATE = "2023-10-16"

def get_trading_days(start_date, end_date):
    """获取指定日期范围内的交易日"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    trading_days = []
    current = start
    
    while current <= end:
        # 排除周末 (Saturday=5, Sunday=6)
        if current.weekday() < 5:
            trading_days.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    
    return trading_days

def read_sql_file():
    """读取SQL文件内容"""
    try:
        with open(SQL_FILE_PATH, 'r') as file:
            sql_content = file.read()
        return sql_content
    except Exception as e:
        print(f"❌ 读取SQL文件失败: {e}")
        return None

def run_v1_query(target_date, client):
    """运行V1查询生成报告"""
    try:
        sql_content = read_sql_file()
        if not sql_content:
            return None
        
        # 替换target_date_param
        sql_content = sql_content.replace("DECLARE target_date_param DATE DEFAULT '2023-02-20';", 
                                        f"DECLARE target_date_param DATE DEFAULT '{target_date}';")
        
        # 替换项目ID
        sql_content = sql_content.replace("YOUR_PROJECT_ID", PROJECT_ID)
        
        print(f"📊 运行V1查询，目标日期: {target_date}")
        
        # 执行查询
        query_job = client.query(sql_content)
        results = query_job.result()
        
        # 获取结果
        for row in results:
            return {
                'target_date': target_date,
                'input_prompt': row.input_prompt,
                'counsellor_report': row.counsellor_report,
                'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        
        return None
        
    except Exception as e:
        print(f"❌ 查询执行失败 ({target_date}): {e}")
        return None

def save_report(report_data, output_dir):
    """保存报告到文件"""
    if not report_data:
        return False
    
    target_date = report_data['target_date']
    
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    
    # 保存报告文件
    report_file = os.path.join(output_dir, f"mirror_report_v1_{target_date}.md")
    
    try:
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(f"# Mirror Report V1 - {target_date}\n\n")
            f.write(f"**生成时间**: {report_data['timestamp']}\n\n")
            f.write(f"**目标日期**: {target_date}\n\n")
            f.write("---\n\n")
            f.write("## Input Prompt\n\n")
            f.write(f"```\n{report_data['input_prompt']}\n```\n\n")
            f.write("---\n\n")
            f.write("## Counsellor Report\n\n")
            f.write(report_data['counsellor_report'])
        
        print(f"✅ 报告已保存: {report_file}")
        return True
        
    except Exception as e:
        print(f"❌ 保存报告失败 ({target_date}): {e}")
        return False

def main():
    """主函数"""
    print("🚀 开始运行V1版本报告生成")
    print(f"📅 日期范围: {START_DATE} 到 {END_DATE}")
    print(f"📁 输出目录: {OUTPUT_DIR}")
    print("=" * 60)
    
    # 初始化BigQuery客户端
    try:
        client = bigquery.Client(project=PROJECT_ID)
        print(f"✅ 已连接到BigQuery项目: {PROJECT_ID}")
    except Exception as e:
        print(f"❌ BigQuery连接失败: {e}")
        return
    
    # 获取交易日列表
    trading_days = get_trading_days(START_DATE, END_DATE)
    print(f"📊 找到 {len(trading_days)} 个交易日")
    
    # 运行查询并生成报告
    successful_reports = 0
    failed_reports = 0
    
    for i, target_date in enumerate(trading_days, 1):
        print(f"\n[{i}/{len(trading_days)}] 处理日期: {target_date}")
        
        # 运行查询
        report_data = run_v1_query(target_date, client)
        
        if report_data:
            # 保存报告
            if save_report(report_data, OUTPUT_DIR):
                successful_reports += 1
            else:
                failed_reports += 1
        else:
            failed_reports += 1
    
    # 总结
    print("\n" + "=" * 60)
    print("📊 运行总结:")
    print(f"  总交易日: {len(trading_days)}")
    print(f"  成功生成: {successful_reports}")
    print(f"  失败数量: {failed_reports}")
    print(f"  成功率: {successful_reports/len(trading_days)*100:.1f}%")
    
    if successful_reports > 0:
        print(f"\n✅ V1报告生成完成！")
        print(f"📁 报告保存在: {OUTPUT_DIR}/")
    else:
        print(f"\n❌ 没有成功生成任何报告")

if __name__ == "__main__":
    main()
