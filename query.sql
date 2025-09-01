CREATE TABLE public.test_share_bond_relation (
    bond_name VARCHAR(255),        -- 转债名称
    bond_no VARCHAR(20), -- 代码（设为主键）
    share_name VARCHAR(255),       -- 正股名称
    share_no VARCHAR(20),          -- 正股代码
    bond_value NUMERIC,            -- 现价
    share_value NUMERIC,            -- 正股价
    bond_rate numeric,             -- 债券涨跌幅
    share_rate numeric,            -- 正股涨跌幅
    rate_diff varchar(255),             -- 两者涨跌幅的差价
    convert_value NUMERIC,         -- 转股价
    premium NUMERIC,               -- 转股溢价率
    difference NUMERIC,            -- 差异
    calc_premium NUMERIC,          -- 计算溢价率
    label VARCHAR(255),             -- 标签
    execute_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 运行时间戳
    PRIMARY KEY (bond_no, execute_time)
);

CREATE INDEX idx_execute_time ON public.test_share_bond_relation (execute_time);


delete from public.test_share_bond_relation where execute_time = '2025-08-28 09:06:02.525494'


    SELECT * FROM public.test_share_bond_relation 
    WHERE execute_time = (select max(execute_time) from public.test_share_bond_relation)
    ORDER BY share_rate DESC
    LIMIT 100