import os
import pandas as pd
import multiprocessing
from datetime import datetime, timedelta

# 定义要处理的路径
path = "E:\\在线监测数据\\水泥\\是否为5天缺失值处理前"

# 获取所有csv文件的路径
file_list = [os.path.join(path, file) for file in os.listdir(path) if file.endswith(".csv")]

# 定义一个函数来找到连续的时间段
def find_continuous_periods(time_series):
    time_series = pd.to_datetime(time_series, errors='coerce')  # 强制转换为datetime类型
    time_series = time_series.dropna().sort_values()  # 删除无法解析的时间并排序
    
    continuous_periods = []
    current_period = []
    
    for time in time_series:
        if len(current_period) == 0:
            current_period.append(time)
        elif (time - current_period[-1]) == timedelta(hours=1):
            current_period.append(time)
        else:
            continuous_periods.append(current_period)
            current_period = [time]
    
    if len(current_period) > 0:
        continuous_periods.append(current_period)
    
    return continuous_periods

# 补值函数：处理连续空值段和单个空值
def fill_missing_values(df, column):
    df[column] = pd.to_numeric(df[column], errors='coerce')  # 将列转换为数值类型
    missing_times = df[df[column].isna()]['监测时间']
    continuous_periods = find_continuous_periods(missing_times)
   
    def calculate_monthly_mean(current_time):
        month_start = current_time.replace(day=1, hour=0, minute=0, second=0)
        month_end = (month_start + pd.DateOffset(months=1)) - timedelta(seconds=1)
        monthly_data = df[(df['监测时间'] >= month_start) & (df['监测时间'] <= month_end)][column]
        
        # 调试信息：打印该月的数据情况
        total_count = len(monthly_data)
        nan_count = monthly_data.isna().sum()
        zero_count = (monthly_data == 0).sum()
        

        if nan_count + zero_count > 0.95 * total_count:
            return 0  # 如果当月数据的NaN值和零值数量超过总数的95%

        monthly_data_non_zero = monthly_data[monthly_data != 0]
        if monthly_data_non_zero.empty or len(monthly_data_non_zero) < 24:
            return 0  # 没有有效数据点时返回0，或者有效数据少于24条时返回0

        return monthly_data_non_zero.mean()

    # 原则1：处理连续空值段
    for period in continuous_periods:
        if len(period) > 1:
            period_df = df[df['监测时间'].isin(period)]
            current_time = period_df['监测时间'].iloc[0]
            mean_value = calculate_monthly_mean(current_time)
            print(f"Filling continuous period from {period[0]} to {period[-1]} with mean value {mean_value}")
            df.loc[period_df.index, column] = mean_value
    
    # 原则2：处理单个空值
    for period in continuous_periods:
        if len(period) == 1:
            time = period[0]
            previous_time = time - timedelta(hours=1)
            next_time = time + timedelta(hours=1)

            prev_value = df[df['监测时间'] == previous_time][column]
            next_value = df[df['监测时间'] == next_time][column]

            if not prev_value.empty and not next_value.empty:
                if prev_value.iloc[0] != 0 and next_value.iloc[0] != 0:
                    new_value = (prev_value.iloc[0] + next_value.iloc[0]) / 2
                    df.loc[df['监测时间'] == time, column] = new_value
                else:
                    df.loc[df['监测时间'] == time, column] = 0
            else:
                mean_value = calculate_monthly_mean(time)
                df.loc[df['监测时间'] == time, column] = mean_value

    return df

# 定义处理每个文件的函数
def process_file(file_path):
    print(f"开始处理文件: {file_path}")
    # 读取csv文件
    df = pd.read_csv(file_path, parse_dates=['监测时间'], low_memory=False, dtype=str)
    
    # 转换监测时间为datetime类型，并处理可能的解析错误
    df['监测时间'] = pd.to_datetime(df['监测时间'], errors='coerce', format="%Y-%m-%d %H:%M:%S")
    df = df.dropna(subset=['监测时间'])  # 删除解析失败的行
    
    # 获取"是否折算"列的第一个内容
    is_adjusted = df['是否折算'].iloc[0]

    # 定义需要处理的列
    if is_adjusted == '是':
        columns_to_check = ['颗粒物折算浓度', '二氧化硫折算浓度', '氮氧化物折算浓度']
    else:
        columns_to_check = ['颗粒物实测浓度']

    # 补值处理
    for col in columns_to_check:
        df = fill_missing_values(df, col)

    # 保存修改后的csv文件
    df.to_csv(file_path, index=False)
    print(f"文件处理完成并保存: {file_path}")
    
    return file_path

def main():
    num_files = len(file_list)
    # 获取处理器核心数的一半
    num_cores = multiprocessing.cpu_count() // 2

    with multiprocessing.Pool(num_cores) as pool:
        for i, _ in enumerate(pool.imap(process_file, file_list), 1):
            print(f"已处理 {i}/{num_files} 个文件，还剩 {num_files - i} 个文件。")
    
    print("所有文件处理完毕。")

if __name__ == "__main__":
    main()
