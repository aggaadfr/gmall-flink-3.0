package com.atguigu.app.dwd.db;

import com.atguigu.utils.MyKafkaUtil;
import com.atguigu.utils.MysqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * TODO 10 交易域退单事务事实表
 * 1、筛选退单表数据 topic_db
 * 2、筛选订单表数据并转化为流
 * 3、建立 MySQL-Lookup 字典表 base_dic
 * 4、关联这几张表获得退单明细宽表，写入 Kafka dwd_trade_order_refund 退单明细主题
 * Project: gmall-flink-3.0
 * Package: com.atguigu.app.dwd.db
 * Version: 1.0
 * <p>
 * Created by  wangjiaxin  on 2022/7/14 21:37
 */
public class DwdTradeOrderRefund {
    public static void main(String[] args) throws Exception {

        // TODO 1. 环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2. 状态后端设置
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                3, Time.days(1), Time.minutes(1)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(
                "hdfs://hadoop102:8020/ck"
        );
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // TODO 3. 从 Kafka 读取 topic_db 数据，封装为 Flink SQL 表
        tableEnv.executeSql("create table topic_db(" +
                "`database` string, " +
                "`table` string, " +
                "`type` string, " +
                "`data` map<string, string>, " +
                "`old` map<string, string>, " +
                "`proc_time` as PROCTIME(), " +
                "`ts` string " +
                ")" + MyKafkaUtil.getKafkaDDL("topic_db", "dwd_trade_order_refund"));

        // TODO 4. 读取退单表数据
        Table orderRefundInfo = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['user_id'] user_id, " +
                "data['order_id'] order_id, " +
                "data['sku_id'] sku_id, " +
                "data['refund_type'] refund_type, " +
                "data['refund_num'] refund_num, " +
                "data['refund_amount'] refund_amount, " +
                "data['refund_reason_type'] refund_reason_type, " +
                "data['refund_reason_txt'] refund_reason_txt, " +
                "data['create_time'] create_time, " +
                "proc_time, " +
                "ts " +
                "from topic_db " +
                "where `table` = 'order_refund_info' " +
                "and `type` = 'insert' ");
        tableEnv.createTemporaryView("order_refund_info", orderRefundInfo);

        // TODO 5. 读取订单表数据，筛选退单数据
        Table orderInfoRefund = tableEnv.sqlQuery("select " +
                "data['id'] id, " +
                "data['province_id'] province_id, " +
                "`old` " +
                "from topic_db " +
                "where `table` = 'order_info' " +
                "and `type` = 'update' " +
                "and data['order_status']='1005' " +
                "and `old`['order_status'] is not null");

        tableEnv.createTemporaryView("order_info_refund", orderInfoRefund);

        // TODO 6. 建立 MySQL-LookUp 字典表
        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        // TODO 7. 关联三张表获得退单宽表
        Table resultTable = tableEnv.sqlQuery("select  " +
                "ri.id, " +
                "ri.user_id, " +
                "ri.order_id, " +
                "ri.sku_id, " +
                "oi.province_id, " +
                "date_format(ri.create_time,'yyyy-MM-dd') date_id, " +
                "ri.create_time, " +
                "ri.refund_type, " +
                "type_dic.dic_name, " +
                "ri.refund_reason_type, " +
                "reason_dic.dic_name, " +
                "ri.refund_reason_txt, " +
                "ri.refund_num, " +
                "ri.refund_amount, " +
                "ri.ts, " +
                "current_row_timestamp() row_op_ts " +
                "from order_refund_info ri " +
                "left join  " +
                "order_info_refund oi " +
                "on ri.order_id = oi.id " +
                "left join  " +
                "base_dic for system_time as of ri.proc_time as type_dic " +
                "on ri.refund_type = type_dic.dic_code " +
                "left join " +
                "base_dic for system_time as of ri.proc_time as reason_dic " +
                "on ri.refund_reason_type=reason_dic.dic_code");
        tableEnv.createTemporaryView("result_table", resultTable);

        // TODO 8. 建立 Upsert-Kafka dwd_trade_order_refund 表
        tableEnv.executeSql("create table dwd_trade_order_refund( " +
                "id string, " +
                "user_id string, " +
                "order_id string, " +
                "sku_id string, " +
                "province_id string, " +
                "date_id string, " +
                "create_time string, " +
                "refund_type_code string, " +
                "refund_type_name string, " +
                "refund_reason_type_code string, " +
                "refund_reason_type_name string, " +
                "refund_reason_txt string, " +
                "refund_num string, " +
                "refund_amount string, " +
                "ts string, " +
                "row_op_ts timestamp_ltz(3), " +
                "primary key(id) not enforced " +
                ")" + MyKafkaUtil.getUpsertKafkaDDL("dwd_trade_order_refund"));

        // TODO 9. 将关联结果写入 Upsert-Kafka 表
        tableEnv.executeSql("" +
                "insert into dwd_trade_order_refund select * from result_table");
    }
}
