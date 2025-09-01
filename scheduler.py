# coding:utf-8
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import atexit


# 初始化调度器
def init_scheduler(app):
    if not app.config.get('SCHEDULER_INITIALIZED'):
        scheduler = BackgroundScheduler()

    # 添加定时任务（每天9:00-15:00每分钟执行）
        scheduler.add_job(
            id = 'morning_monitor',
            func = app.config['JOB_FUNCTION'],
            trigger=CronTrigger(
            day_of_week='mon-fri',
            hour='9', # 9:30-11:30
            minute='30',
            timezone='Asia/Shanghai'
            )
        )

        scheduler.add_job(
            id = 'afternoon_monitor',
            func = app.config['JOB_FUNCTION'],
            trigger=CronTrigger(
            day_of_week='mon-fri',
            hour='13', # 10:00 - 11:29
            minute='0',
            timezone='Asia/Shanghai'
            )
        )

        # 启动调度器
        scheduler.start()
        app.config['SCHEDULER_INITIALIZED']=True
        # 退出时关闭调度器
        atexit.register(lambda: scheduler.shutdown())
        return scheduler
    
    return app.extensions['scheduler']
