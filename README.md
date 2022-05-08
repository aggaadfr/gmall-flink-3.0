# 项目说明

大数据项目之电商实时数仓3.0版本



# 使用框架

- maven-3.6.3
- jdk1.8
- flink-
- hbase
- phoeinx
- redis
- clickhouse
- mysql-5.7



# 项目结构层次说明

```
gmall-flink-3.0
└── gmall-realtime   																	# 实时模块

```



# 数仓分层后每层的编程逻辑

## DIM层编程

1、消费kafka topic_db主题数据（包含所有的业务表数据）

2、过滤维表数据（根据表名做过滤，***动态加载维表***） 使用（广播流 + Connect）方式

- 每隔一段时间自动加载（Java定时任务，process的open方法中）
- 配置信息写到mysql 	--> flinkCDC实时抓取
- 配置信息写到File     --> Flume+kafka+Flink消费
- 广播流 + Connect（广播状态大小问题）  **or**  Keyby + connect（数据倾斜）
- 配置信息写入zk，利用主程序的open方法监听事件（主动推送）

3、将数据写入phoenix（每张维表对应一张phoenix表）

### 用到的相关类

- com.atguigu.app.dim.DimApp
- com.atguigu.app.func.TableProcessFunction
- com.atguigu.bean.TableProcess
- com.atguigu.common.GmallConfig
- com.atguigu.utils.DimUtil
- com.atguigu.utils.MyKafkaUtil









































