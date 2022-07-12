package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.MysqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * TODO 5 交易域加购事务事实表
 * 1、读取kafka topic_db主题数据
 * 2、筛选加购数据封装为维表
 * 3、建立mysql-Lookup维度表
 * 4、关联加购表和字典表获得维度退化后的加购表
 * 5、将数据写回到kafka dwd_trade_cart_add DWD层
 *
 * <p>
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.dwd.db
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/6/12 21:23
 */
public class DwdTradeCartAdd {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setCheckpointStorage("hdfs:xxx:8020//xxx/xx");
//        env.setStateBackend(new HashMapStateBackend());

        // 设置Lookup表数据过期时间
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // 使用DDL方式读取kafka topic_db主题表
        tableEnv.executeSql(MyKafkaUtil.getTopicDbDDL("dwd_trade_cart_add"));


        // 测试数据打印
//        Table table = tableEnv.sqlQuery("select * from topic_db");
        // 将table转成DateSet并打印
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
//        rowDataStream.print();

        // 过滤出加购数据
        Table cartAddTable = tableEnv.sqlQuery("" +
                "select " +
                "    data['id'] id, " +
                "    data['user_id'] user_id, " +
                "    data['sku_id'] sku_id, " +
                "    data['cart_price'] cart_price, " +
                "    if(`type`='insert',cast(`data`['sku_num'] as int),cast(`data`['sku_num'] as int) - cast(`old`['sku_num'] as int)) sku_num, " +
                "    data['sku_name'] sku_name, " +
                "    data['is_checked'] is_checked, " +
                "    data['create_time'] create_time, " +
                "    data['operate_time'] operate_time, " +
                "    data['is_ordered'] is_ordered, " +
                "    data['order_time'] order_time, " +
                "    data['source_type'] source_type, " +
                "    data['source_id'] source_id, " +
                "    pt " +
                "from topic_db " +
                "where `database`='gmall-211027-flink' " +
                "and `table`='cart_info' " +
                "and (`type`='insert'  " +
                "    or (`type`='update'  " +
                "        and `old`['sku_num'] is not null " +
                "        and cast(`data`['sku_num'] as int) > cast(`old`['sku_num'] as int)))");
        tableEnv.createTemporaryView("cart_add", cartAddTable);

        // 测试数据打印
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(cartAddTable, Row.class);
//        rowDataStream.print();


        // 读取Mysql中的base_dic表构建维表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());


        //打印测试
        //Table table = tableEnv.sqlQuery("select * from base_dic");
        tableEnv.sqlQuery("select * from" + cartAddTable);
        //DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
        //rowDataStream.print(">>>>>>>>>>>");

        // 关联两张表    维度退化
        // 在连接Lookup表时需要定义表中的一个处理时间字段 SYSTEM_TIME 的对应时间字段 ps
        Table resultTable = tableEnv.sqlQuery("" +
                "select " +
                "    c.id, " +
                "    c.user_id, " +
                "    c.sku_id, " +
                "    c.cart_price, " +
                "    c.sku_num, " +
                "    c.sku_name, " +
                "    c.is_checked, " +
                "    c.create_time, " +
                "    c.operate_time, " +
                "    c.is_ordered, " +
                "    c.order_time, " +
                "    c.source_type, " +
                "    c.source_id, " +
                "    b.dic_name " +
                "from cart_add c " +
                "join base_dic FOR SYSTEM_TIME AS OF c.pt as b " +
                "on c.source_type = b.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);


        //表测试
//        Table table = tableEnv.sqlQuery("select * from result_table");
//        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
//        rowDataStream.print();


        // 将数据写回到kafka DWD层
        String sinkTopic = "dwd_trade_cart_add";
        tableEnv.executeSql("" +
                "create table trade_cart_add( " +
                "    id string, " +
                "    user_id string, " +
                "    sku_id string, " +
                "    cart_price string, " +
                "    sku_num int, " +
                "    sku_name string, " +
                "    is_checked string, " +
                "    create_time string, " +
                "    operate_time string, " +
                "    is_ordered string, " +
                "    order_time string, " +
                "    source_type string, " +
                "    source_id string, " +
                "    dic_name string " +
                ")" + MyKafkaUtil.getKafkaDDL(sinkTopic, ""));

        // 往kafka中写入数据
        tableEnv.executeSql("insert into trade_cart_add select * from result_table")
                .print();

        // 启动任务
        env.execute("DwdTradeCartAdd");
    }
}
