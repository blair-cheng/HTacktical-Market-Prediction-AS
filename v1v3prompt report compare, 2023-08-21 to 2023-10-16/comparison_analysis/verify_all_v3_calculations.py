#!/usr/bin/env python3
"""
验证所有V3报告的加权回报率计算
"""

import pandas as pd

def verify_all_v3_calculations():
    # 读取完整表格
    df = pd.read_csv("key_content_extraction_table_complete.csv")
    v3_data = df[df['Report_Version'] == 'v3']
    
    print("=== 验证所有V3报告的加权回报率计算 ===\n")
    
    for idx, row in v3_data.iterrows():
        date = row['Report_Date']
        print(f"=== {date} (V3) ===")
        
        # 收集数据
        data = []
        for i in range(1, 6):
            date_col = f'HFP_L_{i}'
            sim_col = f'HFP_L_{i}_Similarity'
            ret_col = f'HFP_L_{i}_Return'
            
            hfp_date = row[date_col] if pd.notna(row[date_col]) else ""
            similarity = row[sim_col] if pd.notna(row[sim_col]) else ""
            return_val = row[ret_col] if pd.notna(row[ret_col]) else ""
            
            if hfp_date and similarity and return_val:
                try:
                    sim = float(similarity)
                    ret_str = str(return_val).replace('%', '').replace('+', '')
                    ret = float(ret_str)
                    data.append({"date": hfp_date, "similarity": sim, "return": ret})
                except:
                    pass
        
        if not data:
            print("无有效数据")
            continue
            
        # 计算加权回报率
        weighted_sum = 0
        similarity_sum = 0
        
        for item in data:
            weighted_contribution = item['similarity'] * item['return']
            weighted_sum += weighted_contribution
            similarity_sum += item['similarity']
        
        if similarity_sum > 0:
            calculated_return = weighted_sum / similarity_sum
            print(f"使用{len(data)}个历史指纹")
            print(f"加权回报率: {calculated_return:.2f}%")
            
            # 显示详细计算
            print("详细计算:")
            for i, item in enumerate(data, 1):
                contribution = item['similarity'] * item['return']
                print(f"  HFP_L_{i}: {item['similarity']} × {item['return']}% = {contribution:.4f}")
            print(f"  加权和: {weighted_sum:.4f}")
            print(f"  相似度和: {similarity_sum:.4f}")
            print(f"  结果: {calculated_return:.4f}%")
        else:
            print("无法计算")
        
        print()

if __name__ == "__main__":
    verify_all_v3_calculations()

