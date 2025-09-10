# coding:utf-8
# 配置logging日志
import logging
import sys
import os

log_dir = '/Users/nzw/Documents/python_proj/share_bond'
log_file = os.path.join(log_dir, 'logs.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(filename)s - %(lineno)d - %(message)s',
    datefmt = '%Y-%m-%d %H:%M:%S',
    handlers = [
        logging.FileHandler(log_file, mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    force = True # 强制重新初始化logger, model = a 增加日志模式
)

logger = logging.getLogger(__name__)