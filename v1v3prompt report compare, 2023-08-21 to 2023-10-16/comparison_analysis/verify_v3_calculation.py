#!/usr/bin/env python3
"""
手动验证V3加权回报率计算
"""

def verify_v3_calculation():
    print("=== 手动验证V3 2023-08-21加权回报率计算 ===\n")
    
    # V3 2023-08-21的实际数据
    data = [
        {"date": "2019-07-08", "similarity": 0.893, "return": 3.5},
        {"date": "2015-02-23", "similarity": 0.911, "return": 0.8},
        {"date": "2012-05-14", "similarity": 0.865, "return": -1.5},
        {"date": "2007-10-01", "similarity": 0.924, "return": -4.2},
        {"date": "2018-09-24", "similarity": 0.902, "return": -6.8}
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
    
    # 验证我的脚本结果
    print(f"\n我的脚本计算结果是: -1.66%")
    print(f"差异: {abs(weighted_return - (-1.66)):.4f}%")
    
    if abs(weighted_return - (-1.66)) < 0.01:
        print("✅ 计算正确！")
    else:
        print("❌ 计算有误！")

if __name__ == "__main__":
    verify_v3_calculation()

