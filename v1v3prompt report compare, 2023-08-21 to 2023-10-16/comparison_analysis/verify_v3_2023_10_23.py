#!/usr/bin/env python3
"""
验证V3 2023-10-23的加权回报率计算
"""

def verify_v3_2023_10_23():
    print("=== 验证V3 2023-10-23加权回报率计算 ===\n")
    
    # V3 2023-10-23的实际数据（从原始报告）
    data = [
        {"date": "2016-11-07", "similarity": 0.852, "return": 3.9},
        {"date": "2015-08-17", "similarity": 0.891, "return": -0.5},
        {"date": "2007-09-17", "similarity": 0.788, "return": -5.8},
        {"date": "2011-08-01", "similarity": 0.824, "return": -7.1},
        {"date": "2018-10-01", "similarity": 0.913, "return": -8.2}
    ]
    
    print("原始数据:")
    for i, item in enumerate(data, 1):
        print(f"HFP_L_{i}: {item['date']}, 相似度={item['similarity']}, 回报={item['return']}%")
    
    print("\n详细计算过程:")
    weighted_sum = 0
    similarity_sum = 0
    
    for i, item in enumerate(data, 1):
        weighted_contribution = item['similarity'] * item['return']
        weighted_sum += weighted_contribution
        similarity_sum += item['similarity']
        
        print(f"HFP_L_{i}: {item['similarity']} × {item['return']}% = {weighted_contribution:.4f}")
    
    print(f"\n加权和: {weighted_sum:.4f}")
    print(f"相似度和: {similarity_sum:.4f}")
    
    weighted_return = weighted_sum / similarity_sum
    print(f"加权回报率: {weighted_sum:.4f} ÷ {similarity_sum:.4f} = {weighted_return:.4f}%")
    
    print(f"\n最终结果: {weighted_return:.2f}%")
    
    # 检查我之前提取的数据
    print(f"\n我之前提取的数据:")
    my_data = [
        {"date": "1995-11-27", "similarity": 0.852, "return": 3.9},
        {"date": "2018-09-24", "similarity": 0.891, "return": -0.5},
        {"date": "2007-08-20", "similarity": 0.788, "return": -5.8},
        {"date": "2006-08-14", "similarity": 0.824, "return": -7.1},
        {"date": "2019-08-12", "similarity": 0.913, "return": -8.2}
    ]
    
    print("我提取的数据:")
    for i, item in enumerate(my_data, 1):
        print(f"HFP_L_{i}: {item['date']}, 相似度={item['similarity']}, 回报={item['return']}%")
    
    # 计算我提取的数据的加权回报率
    my_weighted_sum = 0
    my_similarity_sum = 0
    
    for item in my_data:
        weighted_contribution = item['similarity'] * item['return']
        my_weighted_sum += weighted_contribution
        my_similarity_sum += item['similarity']
    
    my_weighted_return = my_weighted_sum / my_similarity_sum
    print(f"\n我提取数据的加权回报率: {my_weighted_return:.2f}%")
    
    print(f"\n差异: {abs(weighted_return - my_weighted_return):.4f}%")

if __name__ == "__main__":
    verify_v3_2023_10_23()

