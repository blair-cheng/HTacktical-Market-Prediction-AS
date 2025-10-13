#!/usr/bin/env python3
"""
运行V3版本生成指定日期的报告
"""

import os
from google.cloud import bigquery

# 配置
PROJECT_ID = "my-project-sep-16-472318"
SQL_FILE_PATH = "../sql/04_inference/01_master_inference_query Epigraph/v3.Philosophy Edition).sql"
OUTPUT_DIR = "v3_reports"

# 目标日期列表 - 硬编码的9个具体日期
TARGET_DATES = [
    "2023-08-21",
    "2023-08-28", 
    "2023-09-04",
    "2023-09-11",
    "2023-09-18",
    "2023-09-25",
    "2023-10-02",
    "2023-10-09",
    "2023-10-16"
]

def run_v3_query_for_date(client, target_date, sql_file_path, output_dir):
    """运行V3查询生成单个日期的报告"""
    print(f"📊 运行V3查询，目标日期: {target_date}")
    
    # 检查文件是否已存在
    output_filename = os.path.join(output_dir, f"mirror_report_v3_{target_date}.md")
    if os.path.exists(output_filename):
        print(f"⏭️  报告已存在，跳过: {target_date}")
        return True
    
    try:
        with open(sql_file_path, 'r') as file:
            sql_content = file.read()
        
        # 替换项目ID和target_date_param
        sql_content = sql_content.replace("YOUR_PROJECT_ID", PROJECT_ID)
        sql_content = sql_content.replace("DECLARE target_date_param DATE DEFAULT '2023-02-20';", 
                                        f"DECLARE target_date_param DATE DEFAULT '{target_date}';")

        print("⏳ 查询可能需要几分钟...")
        query_job = client.query(sql_content)
        results = query_job.result()

        report_content = ""
        for row in results:
            report_content = row.counsellor_report
            break # 只取第一行报告

        with open(output_filename, 'w') as f:
            f.write(f"# Mirror Report V3 - {target_date}\n\n")
            f.write(report_content)
        print(f"✅ 报告已保存: {output_filename}")
        return True
        
    except Exception as e:
        print(f"❌ 查询执行失败，日期 {target_date}: {e}")
        return False

def main():
    print("🚀 开始运行V3版本报告生成")
    print(f"📅 目标日期: {TARGET_DATES}")
    print(f"📁 输出目录: {OUTPUT_DIR}")
    print("=" * 60)
    
    client = bigquery.Client(project=PROJECT_ID)
    print(f"✅ 已连接到BigQuery项目: {PROJECT_ID}")

    # 创建输出目录
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    successful_generations = 0
    for i, date_str in enumerate(TARGET_DATES):
        print(f"\n[{i+1}/{len(TARGET_DATES)}] 处理日期: {date_str}")
        if run_v3_query_for_date(client, date_str, SQL_FILE_PATH, OUTPUT_DIR):
            successful_generations += 1
    
    print("\n" + "=" * 60)
    print(f"📊 运行总结:")
    print(f"  目标日期: {len(TARGET_DATES)}")
    print(f"  成功生成: {successful_generations}")
    print(f"  失败数量: {len(TARGET_DATES) - successful_generations}")
    print(f"  成功率: {successful_generations / len(TARGET_DATES) * 100:.1f}%")
    
    if successful_generations == len(TARGET_DATES):
        print("\n🎉 V3报告生成完成！")
        print(f"📁 报告保存在: {OUTPUT_DIR}/")
    else:
        print(f"\n⚠️  有 {len(TARGET_DATES) - successful_generations} 个报告生成失败")

if __name__ == "__main__":
    main()
