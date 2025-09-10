# 服务细节

## 启动
waitress-serve --port=5000 app:app

## ngrok 端口映射
ngrok路径: /usr/local/bin/ngrok
ngrok authtoken <token>
ngrok http http://localhost:5000

## 启停kafka, zookeeper, flink
脚本位置:
/Users/nzw/realtime/start-all.sh
/Users/nzw/realtime/stop-all.sh

检验端口是否正常运行:
kafka: lsof -i -P | grep LISTE -> 9092
zookeeper: -> 2181
flink: http://localhost:8081
当kafka任务停止时，需要修复下，否则重启kafka会被认为kafka加入到错误的broker内，运行修复路径以后，再运行start-all.sh启动flink
修复路径: ./kafka_2.13-3.5.2/fix_kafka_cluster.sh

## 刷新时间
20秒包含程序执行的时间大概7秒多，20-7=13是时间间隔
实际情况是每13秒刷新一次


## 尝试使用Flink计算数据流——步骤
1. 把从爬取网站上得到的每行数据（包含股票和债券的信息）单独处理
2. 把数据转换成字典格式（添加执行的时间戳），关键数字需要转换数据类型，并提交到flink进行分发处理
3. 在flink上进行计算

### 算法——识别快速拉升的股票
和欺诈一样，需要给前一次和本次标签
假如每5秒刷新网页获取数据一次，动态的赋予和删除标签

分析流的形式：
和kafka一样，一边产生一边消费，一边计算
什么时候股票应当进入我的视线：
1. 当股票涨幅超过5%，且股票涨幅 - 债券涨幅 > 4%，说明股票大涨，在这个时间点上给一个标记
此时的债券价格作为基准点，计算下一次债券价格与上一次的差异百分比，
    差异百分比 = （下一次债券价格 - 上一次债券价格）/ 上一次债券价格
    如果差超过-0.5%，说明债券在拉升，则预警，一直为正一直预警
    如果差小于0.5%，则不需要预警，说明债券在下降

预警结果按照数据库的方式（可能在目前的基础上进行增减），写入数据库