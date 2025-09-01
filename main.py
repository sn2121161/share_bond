# coding:utf-8
from spider import get_webpage_content, refresh_data
from dataresolve import calc_columns
from database import generate_insert_sql, Connection, del_query
import time
from logs import logger

def main():
    # 参数
    url = "https://www.jisilu.cn/web/data/cb/list"
    username="postgres"
    password=""
    host="localhost"
    port="5432"
    database="nzw"
    time_interval = 20  # 20 seconds for refresh
    hours = 2
    counts = 0
    start_time = time.time()
    end_time = start_time + hours * 3600  # 2小时之后停止

    pg = Connection(
        username=username, password=password,
        host=host, port=port, database=database
    )
    # driver只进一次
    print("程序开始")
    driver = get_webpage_content(url)

    while time.time() < end_time:
        loop_start = time.time()

        df = refresh_data(driver)
        df = calc_columns(df)
        if df.shape[0] == 0:
            logger.warning("没有符合条件的数据")
        else:
            sql = generate_insert_sql(df)
            pg.manipulate(sql)
            logger.info("数据插入成功")
        
        elapsed_time = time.time() - loop_start # 计算执行时间
        counts += 1  # 计数器

        logger.info("第{}次循环完成，耗时{}秒".format(counts, elapsed_time))
        time.sleep(max(0, time_interval - elapsed_time))  # 休眠20秒中去掉执行时间的部分
        
    driver.quit()
    logger.info("循环完成")

    #清理昨日数据的语句
    delq = del_query()
    pg.manipulate(delq)
    logger.info("前日数据清理完成")

    # 关闭数据库连接
    pg.close_conn()
    print("程序结束")

# if __name__ == '__main__':
#     main()
