import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
import psutil

# 获取最大核心数的1/2
max_cores = psutil.cpu_count(logical=True) // 2

# 文件夹路径
folder_path = r'E:\在线监测数据\水泥'

# 获取所有CSV文件的文件名
def get_file_names():
    files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    return files

# 处理单个CSV文件的函数
def process_file(file_name, index, total):
    file_path = os.path.join(folder_path, file_name)
    print(f"Processing file {index + 1}/{total}: {file_name}")
    try:
        # 加载整个CSV文件
        df = pd.read_csv(file_path, low_memory=False)

        # 确保 '工况' 列为 object 类型
        if '工况' in df.columns:
            df['工况'] = df['工况'].astype(object)

        if '是否折算' in df.columns:
            # 遍历所有行
            for condition_index, condition in df.iterrows():
                # 检查 '工况' 列是否包含 '停' 字
                if '停' in str(condition['工况']):
                    df.loc[condition_index, ['颗粒物实测浓度', '二氧化硫实测浓度', '氮氧化物实测浓度', '颗粒物折算浓度', '二氧化硫折算浓度', '氮氧化物折算浓度']] = 0

                # 处理 '是否折算' 为 '是' 的情况
                if condition['是否折算'] == '是':
                    if ((condition['氧含量'] > 20 and condition['烟温'] < 50) or
                        (condition['氧含量'] <= 0 and condition['烟温'] <= 0)):
                        df.loc[condition_index, ['颗粒物折算浓度', '二氧化硫折算浓度', '氮氧化物折算浓度', '颗粒物实测浓度', '二氧化硫实测浓度', '氮氧化物实测浓度']] = 0
                        if pd.isna(df.loc[condition_index, '工况']) or df.loc[condition_index, '工况'] == '':
                            df.loc[condition_index, '工况'] = '停运_人工标记'
       
        # 将更新后的 DataFrame 保存回CSV文件
        df.to_csv(file_path, index=False)

        print(f"Finished processing file {index + 1}/{total}: {file_name}")

    except Exception as e:
        print(f"Error processing file {file_name}: {e}")

# 主函数
def main():
    try:
        file_names = get_file_names()
        total_files = len(file_names)
        print(f"Total files to process: {total_files}")

        with ThreadPoolExecutor(max_workers=max_cores) as executor:
            futures = [executor.submit(process_file, file_name, index, total_files) for index, file_name in enumerate(file_names)]
            for future in futures:
                future.result()

    except Exception as e:
        print(f"Error in main function: {e}")

if __name__ == "__main__":
    main()
