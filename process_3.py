# ###看下哪里的空值最多，看看具体情况
# import pandas as pd
# import os
# from concurrent.futures import ThreadPoolExecutor
# import psutil
# from tqdm import tqdm  # 导入进度条库

# # 获取最大核心数的一半
# max_cores = psutil.cpu_count(logical=True) // 2

# # 文件夹路径
# folder_path = r'E:\在线监测数据\水泥'

# # 保存结果的字典
# results = {
#     '文件名': [],
#     '是否折算': [],
#     '颗粒物实测浓度_空值数量': [],
#     '颗粒物折算浓度_空值数量': [],
#     '二氧化硫折算浓度_空值数量': [],
#     '氮氧化物折算浓度_空值数量': []
# }

# # 获取所有CSV文件的文件名
# file_names = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# def process_file(file_name):
#     file_path = os.path.join(folder_path, file_name)
#     try:
#         # 加载整个CSV文件
#         df = pd.read_csv(file_path, low_memory=False)

#         # 获取'是否折算'列的第一个值
#         is_folded = df.at[0, '是否折算'] if '是否折算' in df.columns else '否'

#         # 初始化空值计数
#         pm_measured_na_count = 0
#         pm_folded_na_count = 0
#         so2_folded_na_count = 0
#         nox_folded_na_count = 0

#         if is_folded == '是':
#             # 统计折算浓度列的空值数量
#             pm_folded_na_count = df['颗粒物折算浓度'].isna().sum() if '颗粒物折算浓度' in df.columns else '列不存在'
#             so2_folded_na_count = df['二氧化硫折算浓度'].isna().sum() if '二氧化硫折算浓度' in df.columns else '列不存在'
#             nox_folded_na_count = df['氮氧化物折算浓度'].isna().sum() if '氮氧化物折算浓度' in df.columns else '列不存在'
#             pm_measured_na_count = df['颗粒物实测浓度'].isna().sum() if '颗粒物实测浓度' in df.columns else '列不存在'
#         else:
#             # 统计实测浓度列的空值数量
#             pm_measured_na_count = df['颗粒物实测浓度'].isna().sum() if '颗粒物实测浓度' in df.columns else '列不存在'

#         # 保存结果
#         return {
#             '文件名': file_name,
#             '是否折算': is_folded,
#             '颗粒物实测浓度_空值数量': pm_measured_na_count,
#             '颗粒物折算浓度_空值数量': pm_folded_na_count,
#             '二氧化硫折算浓度_空值数量': so2_folded_na_count,
#             '氮氧化物折算浓度_空值数量': nox_folded_na_count,
#         }

#     except Exception as e:
#         print(f"Error processing file {file_name}: {e}")
#         return None

# # 主函数
# def main():
#     try:
#         total_files = len(file_names)
#         print(f"Total files to process: {total_files}")

#         with ThreadPoolExecutor(max_workers=max_cores) as executor:
#             results_list = list(tqdm(executor.map(process_file, file_names), total=total_files))

#         # 过滤掉处理失败的文件
#         results_list = [result for result in results_list if result is not None]

#         # 将结果添加到results字典中
#         for result in results_list:
#             results['文件名'].append(result['文件名'])
#             results['是否折算'].append(result['是否折算'])
#             results['颗粒物实测浓度_空值数量'].append(result['颗粒物实测浓度_空值数量'])
#             results['颗粒物折算浓度_空值数量'].append(result['颗粒物折算浓度_空值数量'])
#             results['二氧化硫折算浓度_空值数量'].append(result['二氧化硫折算浓度_空值数量'])
#             results['氮氧化物折算浓度_空值数量'].append(result['氮氧化物折算浓度_空值数量'])

#         # 将结果保存到Excel文件
#         result_df = pd.DataFrame(results)
#         result_df.to_excel(os.path.join(folder_path, '空值统计结果.xlsx'), index=False)
#         print("结果已保存到空值统计结果.xlsx")

#     except Exception as e:
#         print(f"Error in main function: {e}")

# if __name__ == "__main__":
#     main()



# #看下是否全部为0或者为空值
# import pandas as pd
# import os
# from tqdm import tqdm  # 进度条库

# # 文件夹路径
# folder_path = r'E:\在线监测数据\水泥后'

# # 保存结果的字典
# results = {
#     '文件名': [],
#     '是否折算': [],
#     '全为空值或0的列': []
# }

# # 获取所有CSV文件的文件名
# file_names = [f for f in os.listdir(folder_path) if f.endswith('.csv')]

# def process_file(file_name):
#     file_path = os.path.join(folder_path, file_name)
#     try:
#         # 加载整个CSV文件
#         df = pd.read_csv(file_path, low_memory=False)

#         # 获取'是否折算'列的第一个值
#         is_folded = df.at[0, '是否折算']

#         # 初始化全为空值或0的列列表
#         empty_or_zero_cols = []

#         if is_folded == '是':
#             # 检查每列是否全部为空值或0
#             for col in ['颗粒物实测浓度', '二氧化硫实测浓度', '氮氧化物实测浓度']:
#                 if col in df.columns and ((df[col].isna()) | (df[col] == 0)).all():
#                     empty_or_zero_cols.append(col)
#         else:
#             # 检查'颗粒物实测浓度'列是否全部为空值或0
#             if '颗粒物实测浓度' in df.columns and ((df['颗粒物实测浓度'].isna()) | (df['颗粒物实测浓度'] == 0)).all():
#                 empty_or_zero_cols.append('颗粒物实测浓度')

#         # 如果有全为空值或0的列，保存结果
#         if empty_or_zero_cols:
#             results['文件名'].append(file_name)
#             results['是否折算'].append(is_folded)
#             results['全为空值或0的列'].append(', '.join(empty_or_zero_cols))

#     except Exception as e:
#         print(f"Error processing file {file_name}: {e}")

# # 主函数
# def main():
#     try:
#         total_files = len(file_names)
#         print(f"Total files to process: {total_files}")

#         for file_name in tqdm(file_names, total=total_files):
#             process_file(file_name)

#         # 将结果保存到Excel文件
#         result_df = pd.DataFrame(results)
#         result_df.to_excel(os.path.join(folder_path, '空值和零值统计结果.xlsx'), index=False)
#         print("结果已保存到空值和零值统计结果.xlsx")

#     except Exception as e:
#         print(f"Error in main function: {e}")

# if __name__ == "__main__":
#     main()




'''
大修部分
'''
# import os
# import pandas as pd
# from datetime import timedelta

# # 定义检索csv文件的路径
# path = "E:\\在线监测数据\\水泥"

# # 定义一个函数来找到连续的时间段
# def find_continuous_periods(time_series):
#     time_series = pd.to_datetime(time_series, format="%Y-%m-%d %H:%M:%S")
#     time_series = time_series.sort_values()
    
#     continuous_periods = []
#     current_period = []
    
#     for time in time_series:
#         if len(current_period) == 0:
#             current_period.append(time)
#         elif (time - current_period[-1]) == timedelta(hours=1):
#             current_period.append(time)
#         else:
#             continuous_periods.append(current_period)
#             current_period = [time]
    
#     if len(current_period) > 0:
#         continuous_periods.append(current_period)
    
#     return continuous_periods

# # 定义一个函数来处理每个文件
# def process_file(file_path, file_index, total_files):
#     print(f"正在处理文件 {file_index+1}/{total_files}: {file_path}")
    
#     # 加载整个表，发出警告
#     try:
#         df = pd.read_csv(file_path, low_memory=False)
#     except Exception as e:
#         print(f"加载表时遇到问题：{e}")
#         return
    
#     # 将“监测时间”列转换为Timestamp格式
#     df["监测时间"] = pd.to_datetime(df["监测时间"], format="%Y-%m-%d %H:%M:%S")
    
#     # 检索“是否折算”列的第一个内容
#     is_converted = df["是否折算"].iloc[0]
    
#     if is_converted == "是":
#         columns_to_check = ["颗粒物实测浓度", "二氧化硫实测浓度", "氮氧化物实测浓度"]
#     else:
#         columns_to_check = ["颗粒物实测浓度"]
    
#     # 新建一列来存储标记（使用数字“1”代替“大修”）
#     for col in columns_to_check:
#         repair_col = col + "_大修"
#         if repair_col not in df.columns:
#             df[repair_col] = ""  # 初始化为空字符串
#         df[repair_col] = df[repair_col].astype(str)  # 确保新列为字符串类型
    
#     # 检索每列的空值，并获取对应的监测时间
#     for col in columns_to_check:
#         null_times = df[df[col].isnull()]["监测时间"]
        
#         # 找出连续时间段
#         continuous_periods = find_continuous_periods(null_times)
        
#         # 检查连续时间段的长度
#         for period in continuous_periods:
#             if len(period) > 120:
#                 repair_col = col + "_大修"
                
#                 # 直接使用Timestamp格式进行填充
#                 df.loc[df["监测时间"].isin(period), repair_col] = "1"
    
#     # 保存修改后的数据
#     try:
#         df.to_csv(file_path, index=False)
#         print(f"文件 {file_index+1}/{total_files} 处理完成: {file_path}")
#     except Exception as e:
#         print(f"保存文件时遇到问题：{e}")

# # 遍历路径下的所有csv文件，并处理每个文件
# csv_files = [f for f in os.listdir(path) if f.endswith(".csv")]
# total_files = len(csv_files)

# for index, file_name in enumerate(csv_files):
#     process_file(os.path.join(path, file_name), index, total_files)


# import os
# import pandas as pd
# from datetime import timedelta

# # 定义检索csv文件的路径
# path = "E:\\在线监测数据\\水泥"

# # 定义一个函数来找到连续的时间段
# def find_continuous_periods(time_series):
#     time_series = pd.to_datetime(time_series, format="%Y-%m-%d %H:%M:%S")
#     time_series = time_series.sort_values()
    
#     continuous_periods = []
#     current_period = []
    
#     for time in time_series:
#         if len(current_period) == 0:
#             current_period.append(time)
#         elif (time - current_period[-1]) == timedelta(hours=1):
#             current_period.append(time)
#         else:
#             continuous_periods.append(current_period)
#             current_period = [time]
    
#     if len(current_period) > 0:
#         continuous_periods.append(current_period)
    
#     return continuous_periods

# # 定义一个函数来处理每个文件
# def process_file(file_path, file_index, total_files):
#     print(f"正在处理文件 {file_index+1}/{total_files}: {file_path}")
    
#     # 加载整个表，发出警告
#     try:
#         df = pd.read_csv(file_path, low_memory=False)
#     except Exception as e:
#         print(f"加载表时遇到问题：{e}")
#         return
    
#     # 将“监测时间”列转换为Timestamp格式
#     df["监测时间"] = pd.to_datetime(df["监测时间"], format="%Y-%m-%d %H:%M:%S")
    
#     # 检索“是否折算”列的第一个内容
#     is_converted = df["是否折算"].iloc[0]
    
#     if is_converted == "是":
#         columns_to_check = ["颗粒物实测浓度", "二氧化硫实测浓度", "氮氧化物实测浓度"]
#     else:
#         columns_to_check = ["颗粒物实测浓度"]
    
#     # 新建一列来存储标记（使用数字“1”代替“大修”）
#     for col in columns_to_check:
#         repair_col = col + "_大修"
#         if repair_col not in df.columns:
#             df[repair_col] = ""  # 初始化为空字符串
#         df[repair_col] = df[repair_col].astype(str)  # 确保新列为字符串类型
    
#     # 检索每列的空值和0值，并获取对应的监测时间
#     for col in columns_to_check:
#         null_and_zero_times = df[(df[col].isnull()) | (df[col] == 0)]["监测时间"]
        
#         # 找出连续时间段
#         continuous_periods = find_continuous_periods(null_and_zero_times)
        
#         # 检查连续时间段的长度
#         for period in continuous_periods:
#             if len(period) > 120:
#                 repair_col = col + "_大修"
                
#                 # 直接使用Timestamp格式进行填充
#                 df.loc[df["监测时间"].isin(period), repair_col] = "1"
    
#     # 保存修改后的数据
#     try:
#         df.to_csv(file_path, index=False)
#         print(f"文件 {file_index+1}/{total_files} 处理完成: {file_path}")
#     except Exception as e:
#         print(f"保存文件时遇到问题：{e}")

# # 遍历路径下的所有csv文件，并处理每个文件
# csv_files = [f for f in os.listdir(path) if f.endswith(".csv")]
# total_files = len(csv_files)

# for index, file_name in enumerate(csv_files):
#     process_file(os.path.join(path, file_name), index, total_files)





'''
大修后的处理
# '''
# import os
# import pandas as pd
# import multiprocessing
# from tqdm import tqdm

# # 定义要处理的路径
# path = "E:\\在线监测数据\\水泥"

# # 获取所有csv文件的路径
# file_list = [os.path.join(path, file) for file in os.listdir(path) if file.endswith(".csv")]

# # 定义处理每个文件的函数
# def process_file(file_path):
#     # 读取csv文件
#     df = pd.read_csv(file_path, dtype=str, on_bad_lines='warn')
    
#     # 获取"是否折算"列的第一个内容
#     is_adjusted = df['是否折算'].iloc[0]

#     if is_adjusted == "否":
#         # 筛选"颗粒物实测浓度_大修"列中所有为“1”的单元格
#         filter_condition = df['颗粒物实测浓度_大修'] == '1'
#         if filter_condition.any():
#             # 将"颗粒物实测浓度"和"颗粒物折算浓度"列中的对应行填充为0
#             df.loc[filter_condition, ['颗粒物实测浓度', '颗粒物折算浓度']] = '0'
            
#             # 检查并填充"工况"列中的空值
#             df.loc[filter_condition & df['工况'].isna(), '工况'] = '停运_连续5日缺失'

#     elif is_adjusted == "是":
#         # 筛选并处理每一列中所有为“1”的单元格
#         for col in ['颗粒物实测浓度_大修', '二氧化硫实测浓度_大修', '氮氧化物实测浓度_大修']:
#             filter_condition = df[col] == '1'
#             if filter_condition.any():
#                 # 根据当前列，选择对应的实测浓度列和折算浓度列
#                 if col == '颗粒物实测浓度_大修':
#                     target_cols = ['颗粒物实测浓度', '颗粒物折算浓度']
#                 elif col == '二氧化硫实测浓度_大修':
#                     target_cols = ['二氧化硫实测浓度', '二氧化硫折算浓度']
#                 elif col == '氮氧化物实测浓度_大修':
#                     target_cols = ['氮氧化物实测浓度', '氮氧化物折算浓度']

#                 # 将对应的实测浓度列和折算浓度列中的行填充为0
#                 df.loc[filter_condition, target_cols] = '0'

#                 # 检查并填充"工况"列中的空值
#                 df.loc[filter_condition & df['工况'].isna(), '工况'] = '停运_连续5日缺失'

#     # 保存修改后的csv文件
#     df.to_csv(file_path, index=False)
    
#     return file_path

# def main():
#     # 获取处理器核心数的一半
#     num_cores = multiprocessing.cpu_count() // 2
    
#     # 使用tqdm显示进度
#     with multiprocessing.Pool(num_cores) as pool:
#         results = list(tqdm(pool.imap(process_file, file_list), total=len(file_list)))
    
#     print(f"所有文件处理完毕，共处理了 {len(results)} 个文件。")

# if __name__ == "__main__":
#     main()

