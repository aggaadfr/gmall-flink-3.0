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
└── gmall-realtime   					 实时模块

```



# 数仓分层后每层的编程逻辑

## DIM层编程

1、消费kafka **topic_db**主题数据（包含所有的业务表数据）

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

### 配置信息表字段

- sourceTable                 表名
- sinkTable                      phoenix表名
- sinkColumns                phoenix建表字段
- sinkPk                            phoenix主键（Hbase的rowkey）
- sinkExtend                    建表的扩展字段



## DWD层编程

### 1、流量域未经加工的事务事实表

1、读取kafka **topic_log** 主题的数据创建流

2、数据清洗（脏数据清洗，过滤掉非JSON数据）

3、使用keyby聚合mid数据，做新老用户的检验

4、分流（将各个流的数据分别写出到kafka对应的主题中）

​	dwd_traffic_page_log			页面浏览：主流

​	dwd_traffic_start_log			 启动日志

​	dwd_traffic_display_log		 曝光日志

​	dwd_traffic_action_log		  动作日志

​	dwd_traffic_error_log			错误日志

#### 用到的相关类

- com.atguigu.app.dwd.log.BaseLogApp
- com.atguigu.utils.DateFormatUtil
- com.atguigu.utils.MyKafkaUtil



### 2、页面浏览主题，做过滤并统计日活明细uv

1、读取页面浏览主题数据 **dwd_traffic_page_log**

2、把数据转成json格式，并过滤上一跳id不等于null的数据

3、把数据keyby后，使用状态算子(带有过期时间1 day)，过滤掉重复的mid数据

4、将数据写入到kafka dwd_traffic_unique_visitor_detail 主题

#### 用到的相关类

- com.atguigu.app.dwd.log.DwdTrafficUniqueVisitorDetail
- com.atguigu.utils.DateFormatUtil
- com.atguigu.utils.MyKafkaUtil





















