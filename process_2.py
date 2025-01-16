import pandas as pd
import os
import numpy as np
import multiprocessing as mp

# 文件夹路径
folder_path = 'E:\\在线监测数据\\水泥'
# 用于记录列名的文件路径
missing_columns_file = 'missing_columns.xlsx'

# 获取所有CSV文件的文件名
def get_csv_files(folder_path):
    return [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# 处理单个CSV文件的函数
def process_file(file_path):
    missing_columns = []
    try:
        df = pd.read_csv(file_path, low_memory=False)

        if '是否折算' not in df.columns:
            return f"文件 {file_path} 缺少 '是否折算' 列，跳过处理"

        # 获取'是否折算'列的第一个值
        is_converted = df['是否折算'].iloc[0]

        # 获取每个排放标准列的第一个非空值
        particulate_standard = df['颗粒物排放标准'].dropna().iloc[0] if not df['颗粒物排放标准'].dropna().empty else None
        so2_standard = df['二氧化硫排放标准'].dropna().iloc[0] if not df['二氧化硫排放标准'].dropna().empty else None
        nox_standard = df['氮氧化物排放标准'].dropna().iloc[0] if not df['氮氧化物排放标准'].dropna().empty else None

        if particulate_standard is None:
            missing_columns.append('颗粒物排放标准')
        if so2_standard is None:
            missing_columns.append('二氧化硫排放标准')
        if nox_standard is None:
            missing_columns.append('氮氧化物排放标准')

        if is_converted == '是':
            if particulate_standard is not None:
                particulate_standard *= 20
            if so2_standard is not None:
                so2_standard *= 20
            if nox_standard is not None:
                nox_standard *= 20

            # 检查并替换超标或低于0的值
            if particulate_standard is not None:
                df.loc[(df['颗粒物折算浓度'] < 0) | (df['颗粒物折算浓度'] > particulate_standard), '颗粒物折算浓度'] = None
            if so2_standard is not None:
                df.loc[(df['二氧化硫折算浓度'] < 0) | (df['二氧化硫折算浓度'] > so2_standard), '二氧化硫折算浓度'] = None
            if nox_standard is not None:
                df.loc[(df['氮氧化物折算浓度'] < 0) | (df['氮氧化物折算浓度'] > nox_standard), '氮氧化物折算浓度'] = None

        elif is_converted == '否':
            if particulate_standard is not None:
                particulate_standard *= 20

            # 检查并替换超标或低于0的值
            if particulate_standard is not None:
                df.loc[(df['颗粒物实测浓度'] < 0) | (df['颗粒物实测浓度'] > particulate_standard), '颗粒物实测浓度'] = None

        # 保存处理后的文件
        df.to_csv(file_path, index=False)
        
        return f"处理完成: {file_path}", missing_columns
    except Exception as e:
        return f"处理失败: {file_path} - 错误: {str(e)}", missing_columns

def main():
    csv_files = get_csv_files(folder_path)
    all_missing_columns = []

    # 使用系统最大核心数的一半
    num_cores = mp.cpu_count() // 2
    
    # 使用Pool进行多进程处理
    with mp.Pool(num_cores) as pool:
        for result, missing_columns in pool.imap(process_file, [os.path.join(folder_path, file) for file in csv_files]):
            print(result)
            all_missing_columns.extend(missing_columns)

    # 将缺失列名保存到Excel文件
    if all_missing_columns:
        missing_columns_df = pd.DataFrame(all_missing_columns, columns=['缺失列名'])
        missing_columns_df.to_excel(missing_columns_file, index=False)
        print(f"缺失列名已保存至 {missing_columns_file}")

if __name__ == "__main__":
    main()
