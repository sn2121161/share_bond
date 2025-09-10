# coding:utf-8
from datetime import datetime
from logs import logger
import polars as pl
import pandas as pd

# 计算溢价率
def fixed_fair_value(fair_value, bond_par_value, convert_value, bond_trans_value):
    """
    return: 债券交易价格得到的溢价率
    """
    # B_T = B_P * S_F / S_C
    convert = (fair_value * bond_par_value / convert_value)
    return 100*(bond_trans_value - convert) / convert

# 计算溢价率和表中溢价率的差
def difference(fair_value, convert_value, bond_trans_value):
    return fixed_fair_value(fair_value, 100, convert_value, bond_trans_value)

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
def rate_diff(stock_rate, bond_rate):
    if (stock_rate > 0 and bond_rate > 0):  # 债券的涨跌幅是大于0的，也就是说债券是往上发展的
        if (stock_rate - bond_rate >= 4.2) or (bond_rate - stock_rate >= 4.2):   # 正股比债股涨跌幅大
            return '债股差异大'
        return ''
    else:
        return ''


def calc_columns(df):
    try:
        if isinstance(df, pd.DataFrame):
            df = pl.from_pandas(df)

        df = df.with_columns([
            # 数据类型转换——直接转
            pl.col('现价').cast(pl.Float64),
            pl.col('正股价').cast(pl.Float64),
            pl.col('转股价').str.count_matches(r'\*').alias('下修次数'),

            # 数据类型转换——处理后转
            pl.col('正股涨跌').str.replace('%', '').cast(pl.Float64, strict=False)
            .alias('正股涨跌'),
            
            # 处理涨跌幅：去除%符号，空值处理为0
            pl.col('涨跌幅').str.replace('%', '').cast(pl.Float64, strict=False)
            .alias('涨跌幅'),
            
            pl.col('转股溢价率').str.replace('%', '').cast(pl.Float64, strict=False)
            .alias('转股溢价率'),  
                
            pl.col('转股价').str.replace_all(r'\*+', '').cast(pl.Float64).alias('转股价')
        ])
        
        # 新增列计算
        df = df.with_columns([
            pl.struct(['正股价', '转股价', '现价'])
            .map_elements(lambda x: difference(x['正股价'], x['转股价'], x['现价']), return_dtype=pl.Float64)
            .alias('计算溢价率')
        ]).with_columns([
            (pl.col('计算溢价率') - pl.col('转股溢价率')).alias('差异')
        ]).with_columns([
            pl.struct(['正股涨跌', '涨跌幅'])
            .map_elements(lambda x: rate_diff(x['正股涨跌'], x['涨跌幅']), return_dtype=pl.Utf8)
            .alias('涨跌差异'),

            pl.col('差异').map_elements(get_target, return_dtype = pl.Utf8).alias('标签')
        ])

        # 筛选过滤条件
        df_filter = df.filter(
            (pl.col('涨跌差异') != '') & (pl.col('转股溢价率') <= 50)
        )

        result_col = [
            '转债名称', '代码', '正股名称', '正股代码', '现价', 
            '正股价', '涨跌幅', '正股涨跌', '涨跌差异', '转股价', 
            '转股溢价率', '差异', '计算溢价率', '标签'
        ]
        logger.info("字段处理完成")
        return df_filter[result_col]

    except Exception as e:
        logger.error("处理过程中发生错误: {}".format(e))
        return None
