#!/usr/bin/env python3
"""
生成fragments.csv的独立脚本
运行此脚本可以生成fragments.csv, embeddings.npy, scaler.pkl

特征与处理：
- 5个特征：E1, S1, V1, risk_free_rate, market_forward_excess_returns
- 比率计算：5日均值 / 252日均值（使用完整历史数据回溯）
- 数据预处理：全局 ffill + bfill（仅一次，不在计算中重复）

片段构建：
- 窗口：5天，步长：5天，未来窗口：20天
- 起始点：固定 date_id 1800（确保有252天历史数据）
- 输出字段：date_range_id, start_date_id, end_date_id, future_20d_excess_returns, 
  future_20d_volatility, 5个特征比率

Embedding：
- 5维向量，StandardScaler标准化
- 输出：embeddings.npy [n_fragments, 5], scaler.pkl
"""

import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.preprocessing import StandardScaler
import pickle
import sys

# 尝试导入tqdm用于显示进度，如果没有则使用哑类
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs): return iterable

class FeatureEngineer:
    def __init__(self, features=None, short_window=5, long_window=252):
        if features is None:
            features = ['E1', 'S1', 'V1', 'risk_free_rate', 'market_forward_excess_returns']
        self.features = features
        self.feature_mapping = {
            'risk_free_rate': ['risk_free_rate', 'lagged_risk_free_rate'],
            'market_forward_excess_returns': ['market_forward_excess_returns', 'lagged_market_forward_excess_returns']
        }
        self.short_window = short_window
        self.long_window = long_window
    
    def calculate_ratios(self, data, date_id, window=None, long_window=None):
        if window is None:
            window = self.short_window
        if long_window is None:
            long_window = self.long_window
        
        # 找到当前 date_id 对应的位置索引（使用位置索引以确保 iloc 正确工作）
        # 注意：这里假设 data['date_id'] 是有序的，且 data 已经 reset_index
        mask = data['date_id'] == date_id
        position_indices = np.where(mask)[0]
        if len(position_indices) == 0:
            raise ValueError(f"Date ID {date_id} not found in data")
        current_idx = int(position_indices[0])
        
        # 确定切片范围
        short_start = max(0, current_idx - window + 1)
        long_start = max(0, current_idx - long_window + 1)
        
        # 获取切片 (不再在内部进行 fillna，假设输入数据已清洗)
        short_slice = data.iloc[short_start : current_idx + 1]
        long_slice = data.iloc[long_start : current_idx + 1]
        
        ratios = {}
        # 前4个特征使用增量（差值），market_forward_excess_returns 使用比率
        delta_features = ['E1', 'S1', 'V1', 'risk_free_rate']
        
        for feature in self.features:
            feature_col = None
            if feature in data.columns:
                feature_col = feature
            elif feature in self.feature_mapping:
                for mapped_feat in self.feature_mapping[feature]:
                    if mapped_feat in data.columns:
                        feature_col = mapped_feat
                        break
            
            if feature_col is None:
                if feature in delta_features:
                    ratios[f'{feature}_delta'] = 0.0
                else:
                    ratios[f'{feature}_ratio'] = 1.0
                continue
            
            # 计算平均值 (数据应当在外部已经做过 ffill/bfill)
            short_mean = short_slice[feature_col].mean()
            long_mean = long_slice[feature_col].mean()
            
            if feature in delta_features:
                # 使用增量（差值）：5日均值 - 252日均值
                delta = short_mean - long_mean
                ratios[f'{feature}_delta'] = float(delta)
            else:
                # market_forward_excess_returns 仍然使用比率
                # 避免除零
                EPS = 1e-10
                if abs(long_mean) < EPS:
                    long_mean = EPS if long_mean >= 0 else -EPS
                
                ratio = short_mean / long_mean
                ratios[f'{feature}_ratio'] = float(ratio)
        
        return ratios
    
    def calculate_ratios_for_fragment(self, data, start_date_id, end_date_id):
        return self.calculate_ratios(data, end_date_id)


class FragmentBuilder:
    def __init__(self, feature_engineer=None, window=5, future_window=20):
        if feature_engineer is None:
            self.feature_engineer = FeatureEngineer()
        else:
            self.feature_engineer = feature_engineer
        self.window = window
        self.future_window = future_window
        self.fragments_df = None
    
    def build_fragments(self, train_data, start_date_id=1000, end_date_id=4000, step=5):
        """
        train_data: 必须包含完整的历史数据，以便计算长窗口均值
        start_date_id: 生成片段的起始日期
        end_date_id: 生成片段的结束日期
        """
        window = self.window
        future_window = self.future_window
        
        if 'date_id' not in train_data.columns:
            raise ValueError("train_data must contain 'date_id' column")
        
        # 关键修正：不要在这里过滤 train_data，否则会丢失历史数据。
        # 我们只在循环时控制索引范围。
        
        # 确保数据按日期排序
        # 注意：这里我们使用整个 train_data，外部需要保证 train_data 已经包含了 start_date_id 之前的历史数据
        
        # 找到符合日期范围的"起始索引"列表
        # 我们需要遍历的 range 是: data['date_id'] 在 [start_date_id, end_date_id] 之间
        # 且步长为 step
        
        target_mask = (train_data['date_id'] >= start_date_id) & (train_data['date_id'] <= end_date_id)
        target_indices = train_data.index[target_mask].tolist()
        
        if not target_indices:
            print("Warning: No data found in the specified date range.")
            return pd.DataFrame()

        fragments = []
        
        # 使用索引进行步进
        # target_indices[0] 是 start_date_id 对应的行索引
        # 我们从这个索引开始，每隔 step 取一个片段
        
        start_idx = target_indices[0]
        # 确保不超过数据边界 (需要保留 future_window 的空间)
        max_idx = len(train_data) - future_window
        
        # 在目标范围内循环
        # 注意：end_date_id 限制的是片段的"开始时间"还是"结束时间"？
        # 原逻辑：mask范围是片段的起始行。
        # 修正逻辑：我们遍历合法的起始行索引
        
        valid_start_indices = [idx for idx in range(start_idx, max_idx, step) 
                               if train_data.at[idx, 'date_id'] <= end_date_id]
        
        print(f"Generating {len(valid_start_indices)} fragments...")
        
        for i in tqdm(valid_start_indices):
            # i 是片段的第一天
            # 片段范围索引: [i, i + window)
            current_window_indices = range(i, i + window)
            
            # 边界检查
            if i + window + future_window > len(train_data):
                break
                
            # 获取当前片段的日期ID
            fragment_start_date = train_data.at[i, 'date_id']
            fragment_end_date = train_data.at[i + window - 1, 'date_id']
            
            # 这里的 end_date 用于 FeatureEngineer 回溯
            # train_data 是完整的，所以 FeatureEngineer 可以读取 i 之前的数据
            ratios = self.feature_engineer.calculate_ratios_for_fragment(
                train_data, fragment_start_date, fragment_end_date
            )
            
            # 获取未来收益
            # 未来窗口索引: [i + window, i + window + future_window)
            future_data = train_data.iloc[i + window : i + window + future_window]
            
            future_excess_returns = future_data['market_forward_excess_returns'].values
            # 简单的 NaN 过滤
            future_excess_returns = future_excess_returns[~np.isnan(future_excess_returns)]
            
            if len(future_excess_returns) == 0:
                continue
            
            future_20d_excess_returns = np.mean(future_excess_returns)
            future_20d_volatility = np.std(future_excess_returns)
            
            fragment = {
                'date_range_id': f"{fragment_start_date}_{fragment_end_date}",
                'start_date_id': int(fragment_start_date),
                'end_date_id': int(fragment_end_date),
                'future_20d_excess_returns': future_20d_excess_returns,
                'future_20d_volatility': future_20d_volatility,
                **ratios
            }
            
            fragments.append(fragment)
        
        self.fragments_df = pd.DataFrame(fragments)
        
        # 对异常特征进行截断处理（使用5%-95%分位数，更严格）
        if len(self.fragments_df) > 0:
            # S1_delta: 截断到5%-95%分位数
            if 'S1_delta' in self.fragments_df.columns:
                s1_lower = self.fragments_df['S1_delta'].quantile(0.05)
                s1_upper = self.fragments_df['S1_delta'].quantile(0.95)
                self.fragments_df['S1_delta'] = self.fragments_df['S1_delta'].clip(lower=s1_lower, upper=s1_upper)
            
            # market_forward_excess_returns_ratio: 截断到5%-95%分位数
            if 'market_forward_excess_returns_ratio' in self.fragments_df.columns:
                mfe_lower = self.fragments_df['market_forward_excess_returns_ratio'].quantile(0.05)
                mfe_upper = self.fragments_df['market_forward_excess_returns_ratio'].quantile(0.95)
                self.fragments_df['market_forward_excess_returns_ratio'] = self.fragments_df['market_forward_excess_returns_ratio'].clip(lower=mfe_lower, upper=mfe_upper)
        
        return self.fragments_df


class EmbeddingGenerator:
    def __init__(self, features=None, normalize=True):
        if features is None:
            features = ['E1', 'S1', 'V1', 'risk_free_rate', 'market_forward_excess_returns']
        self.features = features
        self.normalize = normalize
        self.scaler = StandardScaler() if normalize else None
        self._fitted = False
        # 前4个特征使用delta，market_forward_excess_returns使用ratio
        self.delta_features = ['E1', 'S1', 'V1', 'risk_free_rate']
    
    def _get_feature_value(self, ratios, feat):
        """根据特征类型获取值（delta或ratio）"""
        if feat in self.delta_features:
            return ratios.get(f'{feat}_delta', 0.0)
        else:
            return ratios.get(f'{feat}_ratio', 1.0)
    
    def fit(self, ratios_list):
        if self.scaler is not None and len(ratios_list) > 0:
            embeddings = np.array([
                [self._get_feature_value(ratios, feat) for feat in self.features]
                for ratios in ratios_list
            ])
            self.scaler.fit(embeddings)
            self._fitted = True
    
    def generate_embeddings_batch(self, ratios_list):
        if len(ratios_list) == 0:
            return np.array([])
        
        embeddings = np.array([
            [self._get_feature_value(ratios, feat) for feat in self.features]
            for ratios in ratios_list
        ])
        
        if self.normalize and self.scaler is not None and self._fitted:
            embeddings = self.scaler.transform(embeddings)
        elif self.normalize:
            # Fallback normalization if scaler not fitted or None
            norms = np.linalg.norm(embeddings, axis=1, keepdims=True)
            norms[norms == 0] = 1
            embeddings = embeddings / norms
        
        return embeddings
    
    def save_scaler(self, file_path):
        if self.scaler is not None:
            with open(file_path, 'wb') as f:
                pickle.dump(self.scaler, f)


def main():
    # 获取脚本所在目录，确保无论从哪里运行都能找到 train.csv
    # 兼容 IDE 和 Kaggle 环境
    try:
        script_dir = Path(__file__).parent
    except NameError:
        # 在交互式环境（如 Jupyter/Kaggle）中 __file__ 可能不存在
        script_dir = Path('.')
    
    working_dir = script_dir
    train_csv_path = working_dir / 'train.csv'
    
    # 如果脚本目录没有，尝试当前工作目录（Kaggle 环境）
    if not train_csv_path.exists():
        train_csv_path = Path('train.csv')
        if not train_csv_path.exists():
            print(f"Error: train.csv not found")
            print(f"  Searched in: {working_dir.absolute()}")
            print(f"  Current working directory: {Path.cwd()}")
            return
        else:
            working_dir = Path('.')  # 使用当前目录作为工作目录
    
    print("Loading data...")
    train_data = pd.read_csv(train_csv_path)
    
    print("Preprocessing data (Global ffill/bfill)...")
    # 数据预处理：前向填充缺失值
    train_data_preprocessed = train_data.copy().sort_values('date_id').reset_index(drop=True)
    
    required_features = ['E1', 'S1', 'V1', 'risk_free_rate', 'market_forward_excess_returns']
    available_features = [f for f in required_features if f in train_data_preprocessed.columns]
    
    # 只需要在全量数据上做一次填充
    for feat in available_features:
        train_data_preprocessed[feat] = train_data_preprocessed[feat].ffill().bfill()
    
    # 配置参数
    fragment_start_date_id = 2400
    # 保留所有数据，以便计算2400点的252日均值
    # 只要 date_id 2148 (2400-252) 存在于 train.csv 中，FeatureEngineer 就能访问到
    
    max_available_date = train_data_preprocessed['date_id'].max()
    future_window = 20
    end_date_id = max_available_date - future_window  # 确保有足够的未来数据
    
    print(f"Building fragments from date_id {fragment_start_date_id} to {end_date_id}...")
    
    # 初始化
    feature_engineer = FeatureEngineer(
        features=required_features,
        short_window=5,
        long_window=252
    )
    
    fragment_builder = FragmentBuilder(
        feature_engineer=feature_engineer,
        window=5,
        future_window=future_window
    )
    
    # 构建片段
    # 传入全量数据 train_data_preprocessed
    fragments_df = fragment_builder.build_fragments(
        train_data=train_data_preprocessed,
        start_date_id=fragment_start_date_id,
        end_date_id=end_date_id,
        step=5
    )
    
    if fragments_df.empty:
        print("Error: No fragments were generated.")
        return

    print(f"Generated {len(fragments_df)} fragments.")
    
    # 保存fragments.csv
    fragments_path = working_dir / 'fragments.csv'
    fragments_df.to_csv(fragments_path, index=False)
    print(f"Saved fragments to {fragments_path}")
    
    # 提取特征值用于 Embedding（前4个特征使用delta，market_forward_excess_returns使用ratio）
    ratios_list = []
    delta_features = ['E1', 'S1', 'V1', 'risk_free_rate']
    for _, row in fragments_df.iterrows():
        r_dict = {}
        for feat in required_features:
            if feat in delta_features:
                r_dict[f'{feat}_delta'] = row.get(f'{feat}_delta', 0.0)
            else:
                r_dict[f'{feat}_ratio'] = row.get(f'{feat}_ratio', 1.0)
        ratios_list.append(r_dict)
    
    print("Generating embeddings...")
    embedding_generator = EmbeddingGenerator(
        features=required_features,
        normalize=True
    )
    
    # 拟合scaler
    embedding_generator.fit(ratios_list)
    
    # 生成embeddings
    embeddings = embedding_generator.generate_embeddings_batch(ratios_list)
    
    # 保存embeddings
    embeddings_path = working_dir / 'embeddings.npy'
    np.save(embeddings_path, embeddings)
    print(f"Saved embeddings to {embeddings_path}")
    
    # 保存scaler
    scaler_path = working_dir / 'scaler.pkl'
    embedding_generator.save_scaler(str(scaler_path))
    print(f"Saved scaler to {scaler_path}")


if __name__ == '__main__':
    main()