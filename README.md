# 项目说明

大数据项目之电商实时数仓3.0版本

参考giteen地址：https://gitee.com/wh-alex/gmall-211027-flink.git



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



## DWD层编程(数据明细层)

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

### 14、互动域评价事务事实表(DwdInteractionComment)

1、消费kafka ODS 业务主题数据

2、筛选评论数据封装为表

3、维度退化获取最终的评论表，建立Mysql-Lookup字典表 base_dic

4、写入Kafka评论事实主题

#### 用到的相关类

- com.atguigu.utils.MyKafkaUtil
- com.atguigu.utils.MysqlUtil
- com.atguigu.app.dwd.db.DwdInteractionComment

### 15、用户域用户注册事务事实表(DwdUserRegister)

1、消费kafka ODS topic_db 业务主题数据

2、筛选用户注册数据封装为表

3、写入kafka用户注册事实主题 dwd_user_register

#### 用到的相关类

- com.atguigu.utils.MyKafkaUtil
- com.atguigu.app.dwd.db.DwdUserRegister

## DWS层(聚合数据层)

### 16、流量域来源关键词粒度页面浏览各窗口汇总表

1、使用DDL方式读取 DWD层页面浏览日志创建表,同时获取事件时间生成Watermark

2、过滤出搜索数据

3、使用自定义函数分词处理

4、分组开窗聚合

5、将数据转化为流

6、将数据写出到ClinkHouse(先在ClinkHouse中创建出表)

#### 用到的相关类

- com.atguigu.app.func.KeywordUDTF
- com.atguigu.bean.KeywordBean
- com.atguigu.utils.ClickHouseUtil
- com.atguigu.utils.MyKafkaUtil
- com.atguigu.app.dws.DwsTrafficSourceKeywordPageViewWindow

### 17、流量域版本-渠道-地区-访客类别粒度页面浏览各窗口汇总表

1、读取3个主题的数据创建三个流。

2、将3个流统一数据格式 JavaBean

3、提取事件事件生成WaterMark

4、分组、开窗、聚合

5、将数据写出到ClinkHouse

#### 用到的相关类

- com.atguigu.bean.TrafficPageViewBean
- com.atguigu.utils.ClickHouseUtil
- com.atguigu.utils.DateFormatUtil
- com.atguigu.utils.MyKafkaUtil
- com.atguigu.app.dws.DwsTrafficVcChArIsNewPageViewWindow

### 18、流量域页面浏览各窗口汇总表

1、读取 Kafka 页面主题数据

2、转换数据结构，过滤数据，设置水位线

3、按照 mid 分组，统计首页和商品详情页独立访客数，转换数据结构

4、开窗，聚合

5、将数据写出到 ClickHouse

#### 用到的相关类

- com.atguigu.bean.TrafficHomeDetailPageViewBean
- com.atguigu.utils.ClickHouseUtil
- com.atguigu.utils.DateFormatUtil
- com.atguigu.utils.MyKafkaUtil
- com.atguigu.app.dws.DwsTrafficPageViewWindow

### 19、用户域用户登陆各窗口汇总表

1、读取 Kafka 页面主题数据

2、转换数据结构，过滤数据，设置水位线，按照 uid 分组

3、统计回流用户数和独立用户数，开窗，聚合

4、写入 ClickHouse

#### 用到的相关类

- com.atguigu.bean.UserLoginBean
- com.atguigu.utils.ClickHouseUtil
- com.atguigu.utils.DateFormatUtil
- com.atguigu.utils.MyKafkaUtil
- com.atguigu.app.dws.DwsUserUserLoginWindow

### 20、用户域用户注册各窗口汇总表

1、读取 Kafka 用户注册主题数据
2、转换数据结构(String 转换为 JSONObject)
3、设置水位线
4、开窗、聚合
5、写入 ClickHouse

#### 用到的相关类

- com.atguigu.bean.UserRegisterBean
- com.atguigu.utils.ClickHouseUtil
- com.atguigu.utils.DateFormatUtil
- com.atguigu.utils.MyKafkaUtil
- com.atguigu.app.dws.DwsUserUserRegisterWindow

### 21、交易域加购各窗口汇总表

1、从Kafka加购明细主题读取数据

2、转换数据结构、设置水位线、按照用户 id 分组、过滤独立用户加购记录

3、开窗、聚合

4、将数据写入 ClickHouse

#### 用到的相关类

- com.atguigu.bean.CartAddUuBean
- com.atguigu.utils.ClickHouseUtil
- com.atguigu.utils.DateFormatUtil
- com.atguigu.utils.MyKafkaUtil
- com.atguigu.app.dws.DwsTradeCartAddUuWindow

### 22、交易域下单各窗口汇总表

1、从 Kafka订单明细主题读取数据

2、过滤为 null 数据并转换数据结构

3、按照 order_detail_id 分组

4、对 order_detail_id 相同的数据去重

5、设置水位线

6、按照用户 id 分组

7、计算度量字段的值

8、开窗、聚合

9、写出到 ClickHouse

#### 用到的相关类

- com.atguigu.bean.TradeOrderBean
- com.atguigu.utils.ClickHouseUtil
- com.atguigu.utils.DateFormatUtil
- com.atguigu.utils.MyKafkaUtil
- com.atguigu.utils.TimestampLtz3CompareUtil
- com.atguigu.app.dws.DwsTradeOrderWindow

### 23、交易域品牌-品类-用户-SPU粒度下单各窗口汇总表

1、从 Kafka 订单明细主题读取数据。

2、过滤 null 数据并按照唯一键对数据去重。

3、关联维度信息，按照维度分组。

4、统计各维度各窗口的订单数和订单金额。

5、将数据写入 ClickHouse。

#### 用到的相关类

- com.atguigu.app.func.DimAsyncFunction
- com.atguigu.bean.TradeTrademarkCategoryUserSpuOrderBean
- com.atguigu.utils.ClickHouseUtil
- com.atguigu.utils.DateFormatUtil
- com.atguigu.utils.MyKafkaUtil
- com.atguigu.utils.TimestampLtz3CompareUtil

### 24、交易域省份粒度下单各窗口汇总表

1、从 Kafka 订单明细主题读取数据。
2、过滤 null 数据并转换数据结构。
3、按照唯一键去重。
4、转换数据结构。
5、设置水位线。
6、按照省份 ID 和省份名称分组。
7、开窗。
8、聚合计算。
9、关联省份信息。
10、写出到 ClickHouse。

#### 用到的相关类

- com.atguigu.app.func.DimAsyncFunction
- com.atguigu.app.func.OrderDetailFilterFunction
- com.atguigu.bean.TradeProvinceOrderWindow
- com.atguigu.utils.ClickHouseUtil
- com.atguigu.utils.DateFormatUtil

### 25、交易域支付各窗口汇总表

1、从 Kafka 支付成功明细主题读取数据。
2、过滤为 null 的数据，转换数据结构。
3、按照唯一键分组。
4、去重。
5、设置水位线，按照 user_id 分组。
6、统计独立支付人数和新增支付人数。
7、开窗、聚合。
8、写出到 ClickHouse。

#### 用到的相关类

- com.atguigu.bean.TradePaymentWindowBean
- com.atguigu.utils.ClickHouseUtil
- com.atguigu.utils.DateFormatUtil
- com.atguigu.utils.MyKafkaUtil

### 26、交易域品牌-品类-用户粒度退单各窗口汇总表

1、从 Kafka 退单明细主题读取数据。
2、过滤 null 数据并转换数据结构。
3、按照唯一键去重。
4、转换数据结构。
5、补充维度信息。
6、设置水位线。
7、分组、开窗、聚合。
8、写出到 ClickHouse。

#### 用到的相关类

- com.atguigu.app.func.DimAsyncFunction
- com.atguigu.bean.TradeTrademarkCategoryUserRefundBean
- com.atguigu.utils.ClickHouseUtil
- com.atguigu.utils.DateFormatUtil
- com.atguigu.utils.MyKafkaUtil

## ADS层(数据展示层)

可视化大屏使用Sugar

### GVM(翻页记牌器)

JSON格式：

```json
{
	"status": 0,
	"msg": "",
	"data": 123456.0000
}
```

clichhouse sql语句：

```sql
select sum(order_amount) from dws_trade_province_order_window where toYYYYMMDD(stt)=20220427;
```



### UV(用户日活)

JSON格式：

```json
{
	"status": 0,
	"msg": "",
	"data": {
		"categories": [
			"苹果",
			"三星"
		],
		"series": [
			"name": "手机品牌",
			"data"; [
				7777,
				9950
			]
		]
	}
}
```



clickhouse sql语句：

```
select ch,sum(uv_ct) uv,sum(uj_ct) uj from dws_traffic_channel_page_view_window where toYYYYMMDD(stt)=#{date} group by ch;
```



# 内网穿透工具

1、使用花生壳



2、钉钉的内网穿透工具

Github 地址为 https://github.com/open-dingtalk/dingtalk-pierced-client

在CMD中执行以下命令：

```
ding --config ding.cfg --subdomain dinghhh 8070
```

 --config 指定内网穿透的配置文件，固定为钉钉提供的./ding.cfg，无需修改。

 --subdomain 指定需要使用的域名前缀，该前缀将会匹配到“vaiwan.cn”前面，此处 subdomain 是 dinghhh，启动工具后会将 dinghhh.vaiwan.cn 映射到本地。

 8070 为需要代理的本地服务 http-server 端口。

执行完毕后，局域网之外的设备可以通过 http://dinghhh.vaiwan.cn 访问本地 8070 端口的 web 服务



# Flink调优

## **资源配置调优**

内存设置（1CPU配置4G内存）

```
bin/flink run \
-t yarn-per-job \
-d \
-p 5 \ 指定并行度
-Dyarn.application.queue=test \ 指定yarn队列
-Djobmanager.memory.process.size=2048mb \ JM2~4G足够
-Dtaskmanager.memory.process.size=6144mb \ 单个TM2~8G足够
-Dyarn.containers.vcores=3 \ 强行指定yarn容器的核心数
-Dtaskmanager.numberOfTaskSlots=2 \ 与容器核数1core：1slot或1core：2slot
-c com.atguigu.app.dwd.LogBaseApp \
/opt/module/gmall-flink/gmall-realtime-1.0-SNAPSHOT-jar-with-dependencies.jar
```

参数列表：https://ci.apache.org/projects/flink/flink-docs-release-1.12/deployment/config.html

## 并行度设置

开发完成后，先进行压测。任务并行度给10以下，测试单个并行度的处理上限。然后 总QPS/单并行度的处理能力 = 并行度

不能只从QPS去得出并行度，因为有些字段少、逻辑简单的任务，单并行度一秒处理几万条数据。而有些数据字段多，处理逻辑复杂，单并行度一秒只能处理1000条数据。

最好根据高峰期的QPS压测，并行度*1.2倍，富余一些资源。

### Source 端并行度的配置

数据源端是 Kafka，Source的并行度设置为Kafka对应Topic的分区数。

如果已经等于 Kafka 的分区数，消费速度仍跟不上数据生产速度，考虑下Kafka 要扩大分区，同时调大并行度等于分区数。

Flink 的一个并行度可以处理一至多个分区的数据，如果并行度多于 Kafka 的分区数，那么就会造成有的并行度空闲，浪费资源。



### Transform端并行度的配置

Keyby之前的算子

一般不会做太重的操作，都是比如map、filter、flatmap等处理较快的算子，并行度可以和source保持一致。

Keyby之后的算子（KeyGroup 最小值为128）*举例：如并行度为10，则会取128对key进行hash，将数据均匀放置于10个并行度，如取2的整次幂，则更方便被整除，数据更均匀些。*

如果并发较大，建议设置并行度为 2 的整数次幂，例如：128、256、512；

小并发任务的并行度不一定需要设置成 2 的整数次幂；

大并发任务如果没有 KeyBy，并行度也无需设置为 2 的整数次幂；

### **Sink 端并行度的配置**

Sink 端是数据流向下游的地方，可以根据 Sink 端的数据量及下游的服务抗压能力进行评估。

如果Sink端是Kafka，可以设为Kafka对应Topic的分区数。

Sink 端的数据量小，比较常见的就是监控告警的场景，并行度可以设置的小一些。

Source 端的数据量是最小的，拿到 Source 端流过来的数据后做了细粒度的拆分，数据量不断的增加，到 Sink 端的数据量就非常大。那么在 Sink 到下游的存储中间件的时候就需要提高并行度。

另外 Sink 端要与下游的服务进行交互，并行度还得根据下游的服务抗压能力来设置，如果在 Flink Sink 这端的数据量过大的话，且 Sink 处并行度也设置的很大，但下游的服务完全撑不住这么大的并发写入，可能会造成下游服务直接被写挂，所以最终还是要在 Sink 处的并行度做一定的权衡。

## RocksDB大状态调优(增量状态)

RocksDB 是基于 LSM Tree 实现的（类似HBase），写数据都是先缓存到内存中，所以RocksDB 的写请求效率比较高。RocksDB 使用内存结合磁盘的方式来存储数据，每次获取数据时，先从内存中 blockcache 中查找，如果内存中没有再去磁盘中查询。优化后差不多单并行度 TPS 5000 record/s，性能瓶颈主要在于 RocksDB 对磁盘的读请求，所以当处理性能不够时，仅需要横向扩展并行度即可提高整个Job 的吞吐量。以下几个调优参数：

***\*state.backend.incremental\*******\*：\****开启增量检查点，默认false，改为true。

***\*state.backend.rocksdb.predefined-options\*******\*：\****SPINNING_DISK_OPTIMIZED_HIGH_MEM设置为机械硬盘+内存模式，有条件上SSD，指定为FLASH_SSD_OPTIMIZED

***\*state.backend.rocksdb.block.cache-size\****: 整个 RocksDB 共享一个 block cache，读数据时内存的 cache 大小，该参数越大读数据时缓存命中率越高，默认大小为 8 MB，建议设置到 64 ~ 256 MB。

***\*state.backend.rocksdb.thread.num\****: 用于后台 flush 和合并 sst 文件的线程数，默认为 1，建议调大，机械硬盘用户可以改为 4 等更大的值。

***\*state.backend.rocksdb.writebuffer.size\****: RocksDB 中，每个 State 使用一个 Column Family，每个 Column Family 使用独占的 write buffer，建议调大，例如：32M

**\*state.backend.rocksdb.writebuffer.count\****: 每个 Column Family 对应的 writebuffer 数目，默认值是 2，对于机械磁盘来说，如果内存⾜够大，可以调大到 5 左右

***\*state.backend.rocksdb.writebuffer.number-to-merge\****: 将数据从 writebuffer 中 flush 到磁盘时，需要合并的 writebuffer 数量，默认值为 1，可以调成3。

***\*state.backend.local-recovery\****: 设置本地恢复，当 Flink 任务失败时，可以基于本地的状态信息进行恢复任务，可能不需要从 hdfs 拉取数据

## Checkpoint设置

一般我们的 Checkpoint 时间间隔可以设置为分钟级别，例如 1 分钟、3 分钟，对于状态很大的任务每次 Checkpoint 访问 HDFS 比较耗时，可以设置为 5~10 分钟一次Checkpoint，并且调大两次 Checkpoint 之间的暂停间隔，例如设置两次Checkpoint 之间至少暂停 4或8 分钟。

如果 Checkpoint 语义配置为 EXACTLY_ONCE，那么在 Checkpoint 过程中还会存在 barrier 对齐的过程，可以通过 Flink Web UI 的 Checkpoint 选项卡来查看 Checkpoint 过程中各阶段的耗时情况，从而确定到底是哪个阶段导致 Checkpoint 时间过长然后针对性的解决问题。

RocksDB相关参数在1.3中已说明，可以在flink-conf.yaml指定，也可以在Job的代码中调用API单独指定，这里不再列出。

## 使用 Flink ParameterTool 读取配置

### 读取运行参数

在 Flink 程序中可以直接使用 ParameterTool.fromArgs(args) 获取到所有的参数，也可以通过 parameterTool.get("username") 方法获取某个参数对应的值。

举例：通过运行参数指定jobname

```
bin/flink run \
-t yarn-per-job \
-d \
-p 5 \ 指定并行度
-Dyarn.application.queue=test \ 指定yarn队列
-Djobmanager.memory.process.size=1024mb \ 指定JM的总进程大小
-Dtaskmanager.memory.process.size=1024mb \ 指定每个TM的总进程大小
-Dtaskmanager.numberOfTaskSlots=2 \ 指定每个TM的slot数
-c com.atguigu.app.dwd.LogBaseApp \
/opt/module/gmall-flink/gmall-realtime-1.0-SNAPSHOT-jar-with-dependencies.jar \
--jobname dwd-LogBaseApp  //参数名自己随便起，代码里对应上即可
```

在代码里获取参数值：

```
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String myJobname = parameterTool.get("jobname");  //参数名对应
        env.execute(myJobname);
```

### 读取配置文件

可以使用ParameterTool.fromPropertiesFile("/application.properties") 读取 properties 配置文件。可以将所有要配置的地方（比如并行度和一些 Kafka、MySQL 等配置）都写成可配置的，然后其对应的 key 和 value 值都写在配置文件中，最后通过 ParameterTool 去读取配置文件获取对应的值。



## 反压处理

反压（BackPressure）通常产生于这样的场景：短时间的负载高峰导致系统接收数据的速率远高于它处理数据的速率。许多日常问题都会导致反压，例如，垃圾回收停顿可能会导致流入的数据快速堆积，或遇到大促、秒杀活动导致流量陡增。反压如果不能得到正确的处理，可能会导致资源耗尽甚至系统崩溃。

反压机制是指系统能够自己检测到被阻塞的 Operator，然后自适应地降低源头或上游数据的发送速率，从而维持整个系统的稳定。Flink 任务一般运行在多个节点上，数据从上游算子发送到下游算子需要网络传输，若系统在反压时想要降低数据源头或上游算子数据的发送速率，那么肯定也需要网络传输。所以下面先来了解一下 Flink 的网络流控（Flink 对网络数据流量的控制）机制。

### 反压的原因及处理

先检查基本原因，然后再深入研究更复杂的原因，最后找出导致瓶颈的原因。下面列出从最基本到比较复杂的一些反压潜在原因。

注意：反压可能是暂时的，可能是由于负载高峰、CheckPoint 或作业重启引起的数据积压而导致反压。如果反压是暂时的，应该忽略它。另外，请记住，断断续续的反压会影响我们分析和解决问题。

#### 系统资源

检查涉及服务器基本资源的使用情况，如CPU、网络或磁盘I/O，目前 Flink 任务使用最主要的还是内存和 CPU 资源，本地磁盘、依赖的外部存储资源以及网卡资源一般都不会是瓶颈。如果某些资源被充分利用或大量使用，可以借助分析工具，分析性能瓶颈（JVM Profiler+ FlameGraph生成火焰图）。

如何生成火焰图：http://www.54tianzhisheng.cn/2020/10/05/flink-jvm-profiler/

如何读懂火焰图：https://zhuanlan.zhihu.com/p/29952444

Ø 针对特定的资源调优Flink

Ø 通过增加并行度或增加集群中的服务器数量来横向扩展

Ø 减少瓶颈算子上游的并行度，从而减少瓶颈算子接收的数据量（不建议，可能造成整个Job数据延迟增大）

#### 垃圾收集（GC）

长时间GC暂停会导致性能问题。可以通过打印调试GC日志（通过-XX:+PrintGCDetails）或使用某些内存或 GC 分析器（GCViewer工具）来验证是否处于这种情况。

Ø 在Flink提交脚本中,设置JVM参数，打印GC日志：

```
bin/flink run \
-t yarn-per-job \
-d \
-p 5 \ 指定并行度
-Dyarn.application.queue=test \ 指定yarn队列
-Djobmanager.memory.process.size=1024mb \ 指定JM的总进程大小
-Dtaskmanager.memory.process.size=1024mb \ 指定每个TM的总进程大小
-Dtaskmanager.numberOfTaskSlots=2 \ 指定每个TM的slot数
-Denv.java.opts="-XX:+PrintGCDetails -XX:+PrintGCDateStamps"
-c com.atguigu.app.dwd.LogBaseApp \
/opt/module/gmall-flink/gmall-realtime-1.0-SNAPSHOT-jar-with-dependencies.jar
```



#### CPU/线程瓶颈

有时，一个或几个线程导致 CPU 瓶颈，而整个机器的CPU使用率仍然相对较低，则可能无法看到 CPU 瓶颈。例如，48核的服务器上，单个 CPU 瓶颈的线程仅占用 2％的 CPU 使用率，就算单个线程发生了 CPU 瓶颈，我们也看不出来。可以考虑使用2.2.1提到的分析工具，它们可以显示每个线程的 CPU 使用情况来识别热线程。

#### 线程竞争

与上⾯的 CPU/线程瓶颈问题类似，subtask 可能会因为共享资源上高负载线程的竞争而成为瓶颈。同样，可以考虑使用2.2.1提到的分析工具，考虑在用户代码中查找同步开销、锁竞争，尽管避免在用户代码中添加同步。

#### 负载不平衡

如果瓶颈是由数据倾斜引起的，可以尝试通过将数据分区的 key 进行加盐或通过实现本地预聚合来减轻数据倾斜的影响。（关于数据倾斜的详细解决方案，会在下一章节详细讨论）

#### 外部依赖

如果发现我们的 Source 端数据读取性能比较低或者 Sink 端写入性能较差，需要检查第三方组件是否遇到瓶颈。例如，Kafka 集群是否需要扩容，Kafka 连接器是否并行度较低，HBase 的 rowkey 是否遇到热点问题。关于第三方组件的性能问题，需要结合具体的组件来分析。

## 数据倾斜

### keyBy 之前发生数据倾斜

如果 keyBy 之前就存在数据倾斜，上游算子的某些实例可能处理的数据较多，某些实例可能处理的数据较少，产生该情况可能是因为数据源的**数据本身就不均匀**，例如由于某些原因 Kafka 的 topic 中某些 partition 的数据量较大，某些 partition 的数据量较少。对于不存在 keyBy 的 Flink 任务也会出现该情况。

这种情况，需要让 Flink 任务强制进行shuffle。使用shuffle、***rebalance*** 或 rescale算子即可将数据均匀分配，从而解决数据倾斜的问题。

### keyBy 后的聚合操作存在数据倾斜

1、加随机数实现双重聚合		不可行

会出现问题：没有对数据进行聚合，数据没有减少，还会出现计算错误。

2、预聚合：combiner组件	定时器+状态(普通的算子状态)。  可行



### keyBy 后的窗口聚合操作存在数据倾斜

1、加随机数实现双重聚合	可行

*注意：二阶段聚合时要加上window时间在做keyby*

2、预聚合：combiner组件	定时器+状态(普通的算子状态)	不可行

会出现问题：聚合后窗口内的数据会丢失原始数据时间。



# KafkaSource调优

## 动态发现分区

在使用 FlinkKafkaConsumer 时，可以开启 partition 的动态发现。通过 Properties指定参数开启（单位是毫秒）：

```
FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS
```

该参数表示间隔多久检测一次是否有新创建的 partition。默认值是Long的最小值，表示不开启，大于0表示开启。开启时会启动一个线程根据传入的interval定期获取Kafka最新的元数据，新 partition 对应的那一个 subtask 会自动发现并从earliest 位置开始消费，新创建的 partition 对其他 subtask 并不会产生影响。

```
properties.setProperty(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, 30 * 1000 + ""); 
```



## 设置空闲等待

如果数据源中的某一个分区/分片在一段时间内未发送事件数据，则意味着 WatermarkGenerator 也不会获得任何新数据去生成 watermark。我们称这类数据源为空闲输入或空闲源。在这种情况下，当某些其他分区仍然发送事件数据的时候就会出现问题。比如Kafka的Topic中，由于某些原因，造成个别Partition一直没有新的数据。

*由于下游算子 watermark 的计算方式是取所有不同的上游并行数据源 watermark 的最小值，则其 watermark 将不会发生变化，导致窗口、定时器等不会被触发。*

为了解决这个问题，你可以使用 WatermarkStrategy 来检测空闲输入并将其标记为空闲状态。

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "hadoop1:9092,hadoop2:9092,hadoop3:9092");
properties.setProperty("group.id", "fffffffffff");

FlinkKafkaConsumer<String> kafkaSourceFunction = new FlinkKafkaConsumer<>(
                "flinktest",
                new SimpleStringSchema(),
                properties
        );

kafkaSourceFunction.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .forBoundedOutOfOrderness(Duration.ofMinutes(2))
						.withIdleness(Duration.ofMinutes(5))	//设置空闲等待时间
);

env.addSource(kafkaSourceFunction)
```





















