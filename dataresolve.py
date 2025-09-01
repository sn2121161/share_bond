# coding:utf-8
from datetime import datetime
from logs import logger

# 计算溢价率
def fixed_fair_value(fair_value, bond_par_value, convert_value, bond_trans_value):
    """
    return: 债券交易价格得到的溢价率
    """
    # B_T = B_P * S_F / S_C
    convert = (fair_value * bond_par_value / convert_value)
    return 100*(bond_trans_value - convert) / convert

# 计算溢价率和表中溢价率的差
def difference(row):
    return fixed_fair_value(row['正股价'], 100, row['转股价'], row['现价'])

# 评价差
def get_target(x):
    if (x >= 0.1 and x< 1) or (x <= -0.1 and x > -1):
        return '差异>=0.1%且差异<=1%'
    elif (x >= 1 and x < 10) or (x <= -1 and x > -10):
        return '差异>1%且差异<10%'
    elif (x >= 10 or x <= -10):
        return '差异>10%'
    else:
        return ''


# 计算涨跌幅差异
def rate_diff(row):
    if (row['正股涨跌'] > 0) and (row['涨跌幅'] > 0):  # 债券的涨跌幅是大于0的，也就是说债券是往上发展的
        if (row['正股涨跌'] - row['涨跌幅'] >= 4.2) or (row['涨跌幅'] - row['正股涨跌'] >= 4.2):   # 正股比债股涨跌幅大
            return '债股差异大'
        return ''
    else:
        return ''


def calc_columns(df):
    try:
        # df = df[~df['转债名称'].str.contains('退债', na=False)]   # 
        # 转换数据类型
        df['下修次数'] = df['转股价'].str.count(r'\*')   # 统计星号数量
        df['转股价'] = df['转股价'].str.replace(r'\*+', '', regex=True).astype(float)   # 移除所有星号并转为浮点
        df['转股溢价率'] = df['转股溢价率'].apply(lambda x: float(x[:-2]))
        df['现价'] = df['现价'].astype(float)
        df['正股价'] = df['正股价'].astype(float)
        df['正股涨跌'] = df['正股涨跌'].apply(lambda x: 0 if x[:-2] == '' else float(x[:-2]))
        df['涨跌幅'] = df['涨跌幅'].apply(lambda x: 0 if x[:-2] == '' else float(x[:-2]))

        df['计算溢价率'] = df.apply(difference, axis=1)
        df['差异'] = (df['计算溢价率'] - df['转股溢价率'])

        df['涨跌差异'] = df.apply(rate_diff, axis=1)
        df['标签'] = df['差异'].apply(get_target)
        df1 = df[df['涨跌差异'] != '']
        df1 = df1[df1['转股溢价率'] <= 50]
        logger.info("字段处理完成")
        return df1[['转债名称', '代码', '正股名称', '正股代码', '现价', '正股价', '涨跌幅', '正股涨跌', '涨跌差异', '转股价', '转股溢价率', '差异', '计算溢价率', '标签']]

    except Exception as e:
        logger.error("处理过程中发生错误: {}".format(e))
        return None
