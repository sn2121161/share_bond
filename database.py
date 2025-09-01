# coding:utf-8

import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import psycopg2.extras
from urllib.parse import quote_plus as urlquote


class Connection():
    def __init__(self, username, password, host, port, database):
        self.username = username
        self.password = urlquote(password.encode('utf-8'))
        self.host = host
        self.port = port
        self.database = database
        self.url = f'postgresql+psycopg2://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}'
        
        try:
            self.conn = psycopg2.connect(
                        user = username,
                        password = password,
                        host = host,
                        port = port,
                        database = database
            )
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)  # cursor is 0
        except psycopg2.Error as e:
            print("Error: " + str(e))

    def query(self, sql):
        """input sql query
            please notice, any query should be committed after query, otherwise the status will remain <IDLE> in transaction
            then, use sqlalchemy to replace pure psycopg2 because it is compatible
            use engine wil not make <IDLE> in transaction
        """
        engine = create_engine(url=self.url)
        df = pd.read_sql_query(sql, engine)
        self.conn.commit()  # commit the transaction
        # self.cursor.close()  # no need to close cursor, but need to commit transaction
        return df
    
    def manipulate(self, sql):
        try:
            self.cursor.execute(sql)
            self.conn.commit()
            return 0  # 提交事务以后的结果
            
        except Exception as e:
            print("Error: "+str(e))
            self.close_conn()     # when insert query has some problems, close the connection

    # def submit_df(self, df, table_name, schema, strategy):
    #     """use sqlalchemy to sumbit dataframe to database, psycopg2 is not compatible"""
    #     try:
    #         engine = create_engine(url = self.url)
    #         df.to_sql(name=table_name, con=engine, schema=schema, if_exists=strategy, index=False)
    #         return 0
    #     except Exception as e:
    #         print("Error: " + str(e))

    def close_conn(self):
        self.cursor.close()
        self.conn.close()


def generate_insert_sql(df):
    """生成批量插入SQL"""
    values = []
    for _, row in df.iterrows():
        # 处理特殊字符并构建值元组
        value_str = (
            f"('{row['转债名称']}', '{row['代码']}', '{row['正股名称']}', "
            f"'{row['正股代码']}', {row['现价']}, {row['正股价']}, {row['涨跌幅']}, {row['正股涨跌']}, '{row['涨跌差异']}', {row['转股价']}, "
            f"{row['转股溢价率']}, {row['差异']}, "
            f"{row['计算溢价率']}, '{row['标签']}', NOW())"
        )
        values.append(value_str)
        
    return f"""
        INSERT INTO public.test_share_bond_relation (
            bond_name, bond_no, share_name, share_no, bond_value, share_value,
            bond_rate, share_rate, rate_diff, convert_value, premium, difference,
            calc_premium, label, execute_time
        ) VALUES {','.join(values)}
    """

def del_query():
    """清理昨日数据的语句"""
    return """
        DELETE FROM public.test_share_bond_relation 
        WHERE execute_time::date <= CURRENT_DATE - 2;
    """


# if __name__ == "__main__":
#     conn = Connection(
#         username="postgres",
#         password="",
#         host="localhost",
#         port="5432",
#         database="nzw"
#     )
#     sql = """select * from public.dim_pub_rq; """
#     df = conn.query(sql)
#     print(df)