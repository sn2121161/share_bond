# coding:utf-8

from flask import Flask, render_template
from database import Connection
from waitress import serve
from scheduler import init_scheduler

app = Flask(__name__)

def job():
    from main import main
    main()

app.config['JOB_FUNCTION'] = job

# if not hasattr(app, 'extensions'):
#     app.extensions = {}

if 'scheduler' not in app.extensions:
    app.extensions['scheduler'] = init_scheduler(app)
    scheduler = app.extensions['scheduler']
    # scheduler.start()

@app.route("/")
def show_data():
    # 从数据库获取最新数据
    sql = """
    SELECT * FROM public.test_share_bond_relation 
    WHERE execute_time = (select max(execute_time) from public.test_share_bond_relation)
    ORDER BY share_rate DESC
    LIMIT 100
    """
    username="postgres"
    password=""
    host="localhost"
    port="5432"
    database="nzw"
    
    pg = Connection(username, password, host, port, database)
    data = pg.query(sql)
    pg.close_conn()
    return render_template('index.html', data=data.to_dict('records'))


if __name__ == '__main__':
    # app.run(host='0.0.0.0', port=5000)
    serve(app, host = '0.0.0.0', port=5000)
