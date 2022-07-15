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

## DIM层编程(DimApp)

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

### 1、流量域未经加工的事务事实表(BaseLogApp)

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



### 2、流量域独立访客事务事实表(DwdTrafficUniqueVisitorDetail)

1、读取页面浏览主题数据 **dwd_traffic_page_log**

2、把数据转成json格式，并过滤上一跳id不等于null的数据

3、把数据keyby后，使用状态算子(带有过期时间1 day)，过滤掉重复的mid数据

4、将数据写入到kafka dwd_traffic_unique_visitor_detail 主题

#### 用到的相关类

- com.atguigu.app.dwd.log.DwdTrafficUniqueVisitorDetail
- com.atguigu.utils.DateFormatUtil
- com.atguigu.utils.MyKafkaUtil



### 3、流量域用户跳出事务事实表(DwdTrafficUserJumpDetail)

 1、读取kafka dwd_traffic_page_log 主题数据

 2、 将数据转换成JSON对象，提取事件时间生成watermark

 3、 按照mid进行分组

 4、 定义CEP模式序列（处理乱序数据和状态编程）

```
判断为进入页面：last_page_id is null
判断为跳出页面：下一条数据的last_page_id is null 或者 超过10秒没有数据(定时器，TTL)
逻辑：
    来了一条数据，取出上一跳页面信息判断是否为null
        1.last_page_id == null ->
            取出状态数据如果不为null则输出
            将当前数据更新至状态中来等待第三条数据
            删除上一次定时器
            重新注册10秒的定时器
        2、last_page_id != null  ->
            清空状态数据
            删除上一次定时器
        定时器方法：
            输出状态数据
```

 5、 将模式序列作用到流上

 6、 提取匹配上的事件以及超时事件

 7、 合并两个事件流

 8、 将数据写出到kafka

#### 用到的相关类

- com.atguigu.utils.MyKafkaUtil

### 4、交易域加购事务事实表(DwdTradeCartAdd)

1、读取kafka topic_db主题数据

2、筛选加购数据封装为维表

3、建立mysql-Lookup维度表

4、关联加购表和字典表获得维度退化后的加购表

5、将数据写回到kafka dwd_trade_cart_add DWD层

#### 用到的相关类

- com.atguigu.utils.MyKafkaUtil
- com.atguigu.utils.MysqlUtil

### 5、交易域订单预处理表(**DwdTradeOrderPreProcess **)

1、读取kafka ODS topic_db主题数据

2、刷选订单明细表(insert、updata)、订单表(insert、updata)、订单明细活动关联表(insert)、订单明细优惠卷关联表(insert)、Mysql-Lookup字典表

3、关联5张表获得订单预处理表

4、写入kafka订单处理主题

#### 用到的相关的类

- com.atguigu.app.dwd.db.DwdTradeOrderPreProcess
- com.atguigu.utils.MyKafkaUtil
- com.atguigu.utils.MysqlUtil

### 6、交易域下单事务事实表(DwdTradeOrderDetail)

1、消费Kafka DWD dwd_trade_order_pre_process订单预处理主题

2、过滤出订单明细数据；新增数据，即订单表操作类型为 insert 的数据即为订单明细数据；

3、将数据写到kafka dwd_trade_order_detail主题

#### 用到的相关类

- com.atguigu.app.dwd.db.DwdTradeOrderDetail

- com.atguigu.utils.MyKafkaUtil

### 7、交易域取消订单事务事实表(DwdTradeCancelDetail)

1、消费Kafka DWD dwd_trade_order_pre_process订单预处理主题

2、过滤出取消订单明细数据；保留修改了 order_status 字段且修改后该字段值为 "1003" 的数据;

3、将数据写到kafka dwd_trade_cancel_detail主题

#### 用到的相关类

- com.atguigu.utils.MyKafkaUtil;
- com.atguigu.app.dwd.db.DwdTradeCancelDetail

### 8、交易域支付成功事务事实表(DwdTradePayDetailSuc)

1、获取订单明细数据 dwd dwd_trade_order_detail主题

2、筛选支付表数据

3、构建 MySQL-LookUp 字典表

4、关联上述三张表形成支付成功宽表，写入 Kafka  dwd_trade_pay_detail_suc 支付成功主题

#### 用到的相关类

- com.atguigu.utils.MyKafkaUtil
- com.atguigu.utils.MysqlUtil
- com.atguigu.app.dwd.db.DwdTradePayDetailSuc

### 9、交易域退单事务事实表(DwdTradeOrderRefund)

1、筛选退单表数据 topic_db

2、筛选订单表数据并转化为流

3、建立 MySQL-Lookup 字典表 base_dic

4、关联这几张表获得退单明细宽表，写入 Kafka dwd_trade_order_refund 退单明细主题

#### 用到的相关类

- com.atguigu.utils.MyKafkaUtil

- com.atguigu.utils.MysqlUtil

- com.atguigu.app.dwd.db.DwdTradeOrderRefund

### 10、交易域退款成功事务事实表(DwdTradeRefundPaySuc)

1、建立 MySQL-Lookup 字典表

2、读取退款表数据，筛选退款成功数据

3、读取订单表数据，过滤退款成功订单数据

4、筛选退款成功的退单明细数据

5、关联四张表并写出到 Kafka 退款成功主题

#### 用到的相关类

- com.atguigu.utils.MyKafkaUtil
- com.atguigu.utils.MysqlUtil
- com.atguigu.app.dwd.db.DwdTradeRefundPaySuc

### 11、工具域优惠券领取事务事实表(DwdToolCouponGet)

1、消费kafka ODS topic_db 主题数据

2、筛选优惠卷领取数据封装为表

3、写入kafka优惠卷领取事务主题表 dwd_tool_coupon_get

#### 用到的相关类

- com.atguigu.utils.MyKafkaUtil
- com.atguigu.app.dwd.db.DwdToolCouponGet

### 12、工具域优惠券使用（下单）事务事实表(DwdToolCouponOrder)

1、消费kafka ODS topic_db 业务主题数据

2、筛选优惠卷领取数据封装为表

3、封装为流

4、封装为表

5、写入kafka优惠券使用(下单)事实主题 dwd_tool_coupon_order

#### 用到的相关类

- com.alibaba.fastjson.JSON
- com.atguigu.bean.CouponUseOrderBean
- com.atguigu.utils.MyKafkaUtil
- com.atguigu.app.dwd.db.DwdToolCouponOrder

### 13、工具域优惠券使用(支付)事务事实表(DwdToolCouponPay)

1、消费Kafka ODS topic_db 业务主题数据

2、筛选收藏数据封装为表

3、写入Kafka收藏事实主题 dwd_interaction_favor_add

#### 用到的相关类

- com.atguigu.utils.MyKafkaUtil
- com.atguigu.app.dwd.db.DwdToolCouponPay

















